package objects

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"crypto/sha256"
	"encoding/hex"

	"github.com/InsulaLabs/insi/runtime"
	"github.com/google/uuid"
)

/*

       Given DIR:
	         - /objects
			      some-uuid/
				      file.data // the actual binary data of the file upload stripped of name and extension
					  file.data.sha256 // calculated on upload
					  meta.json // stores date uploaded, files size, and file upload name (what it was called on upload)
				  some-other-uuid/
				      file.data // the actual binary data of the file upload stripped of name and extension
					  file.data.sha256 // calculated on upload
					  meta.json // stores date uploaded, files size, and file upload name (what it was called on upload)



		Path "upload/file"








*/

// MetaData stores information about the uploaded object.
type MetaData struct {
	OriginalFilename string    `json:"originalFilename"`
	FileSize         int64     `json:"fileSize"`
	UploadDate       time.Time `json:"uploadDate"`
	UserUUID         string    `json:"userUUID"`
	ObjectUUID       string    `json:"objectUUID"` // The UUID part of the object name
	ContentType      string    `json:"contentType"`
}

type ObjectsPlugin struct {
	logger *slog.Logger
	prif   runtime.PluginRuntimeIF

	startedAt time.Time
	uploadDir string
}

var _ runtime.Plugin = &ObjectsPlugin{}

func New(logger *slog.Logger, uploadDir string) *ObjectsPlugin {
	return &ObjectsPlugin{
		logger:    logger,
		uploadDir: uploadDir,
	}
}

func (p *ObjectsPlugin) GetName() string {
	return "objects"
}

func (p *ObjectsPlugin) Init(prif runtime.PluginRuntimeIF) *runtime.PluginImplError {
	p.prif = prif
	p.startedAt = time.Now()

	if err := os.MkdirAll(p.uploadDir, 0755); err != nil {
		return &runtime.PluginImplError{Err: err}
	}

	uploadDir := filepath.Join(p.uploadDir, "uploads")
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return &runtime.PluginImplError{Err: err}
	}

	return nil
}

func (p *ObjectsPlugin) GetRoutes() []runtime.PluginRoute {
	return []runtime.PluginRoute{
		/*
			IMPORTANT:
				DO NOT PREFIX THE PATH WITH TEH NAME OF THE PLUGIN

				THIS IS OF PARAMOUNT IMPORTANCE AS THE ROUTES ARE AUTOMATICALLY MOUNTED
				TO THE SERVICE PREFIXED BY THE NAME OF THE PLUGIN.
				ADDING "/service-name" HERE WILL CASUSE "/service-name/service-name/uptime"
				TO BE MOUNTED.
		*/
		{Path: "upload", Handler: http.HandlerFunc(p.uploadBinaryHandler), Limit: 10, Burst: 10},
		{Path: "download", Handler: http.HandlerFunc(p.downloadBinaryHandler), Limit: 10, Burst: 10},
	}
}

// / -------- routes --------
func (p *ObjectsPlugin) uploadBinaryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Parse the multipart form, 32 << 20 specifies a maximum upload of 32 MB files.
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		p.logger.Error("could not parse multipart form", "error", err)
		http.Error(w, "Error parsing form data: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Get handler for filename, size and headers
	file, handler, err := r.FormFile("file") // "file" is the key of the form-data
	if err != nil {
		p.logger.Error("error retrieving the file from form-data", "error", err)
		http.Error(w, "Error retrieving file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	originalFilename := handler.Filename
	contentType := handler.Header.Get("Content-Type")

	userUUID := td.UUID
	objectFileUUID := uuid.New().String()
	newObjectName := fmt.Sprintf("%s:%s", userUUID, objectFileUUID) // This is the ID for the object, and also the directory name.
	objectDir := filepath.Join(p.uploadDir, newObjectName)

	if err := os.MkdirAll(objectDir, 0755); err != nil {
		p.logger.Error("error creating object directory", "error", err, "objectDir", objectDir)
		http.Error(w, "Could not create storage for file", http.StatusInternalServerError)
		return
	}

	// Create the file.data file
	dataPath := filepath.Join(objectDir, "file.data")
	dst, err := os.Create(dataPath)
	if err != nil {
		p.logger.Error("error creating file.data", "error", err, "dataPath", dataPath)
		http.Error(w, "Could not create file on server", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	// Copy the uploaded file data to the new file, and get its size.
	fileSize, err := io.Copy(dst, file)
	if err != nil {
		p.logger.Error("error copying uploaded file to destination", "error", err, "dataPath", dataPath)
		http.Error(w, "Error saving file", http.StatusInternalServerError)
		return
	}

	// Calculate SHA256 hash of the file.data
	// We need to close dst first to ensure all data is flushed, then reopen for hashing.
	dst.Close()                        // Close before hashing
	hashFile, err := os.Open(dataPath) // Reopen for reading
	if err != nil {
		p.logger.Error("error opening file.data for hashing", "error", err, "dataPath", dataPath)
		http.Error(w, "Error processing file", http.StatusInternalServerError)
		return
	}
	defer hashFile.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, hashFile); err != nil {
		p.logger.Error("error hashing file.data", "error", err, "dataPath", dataPath)
		http.Error(w, "Error processing file", http.StatusInternalServerError)
		return
	}
	sha256sum := hex.EncodeToString(hasher.Sum(nil))

	// Write the SHA256 hash to file.data.sha256
	sha256Path := filepath.Join(objectDir, "file.data.sha256")
	if err := os.WriteFile(sha256Path, []byte(sha256sum), 0644); err != nil {
		p.logger.Error("error writing sha256 file", "error", err, "sha256Path", sha256Path)
		http.Error(w, "Error saving file metadata", http.StatusInternalServerError)
		return
	}

	// Create and write meta.json
	meta := MetaData{
		OriginalFilename: originalFilename,
		FileSize:         fileSize,
		UploadDate:       time.Now(),
		UserUUID:         userUUID,
		ObjectUUID:       objectFileUUID, // Just the file's UUID part
		ContentType:      contentType,
	}

	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		p.logger.Error("error marshalling meta.json", "error", err)
		http.Error(w, "Error creating file metadata", http.StatusInternalServerError)
		return
	}

	metaPath := filepath.Join(objectDir, "meta.json")
	if err := os.WriteFile(metaPath, metaBytes, 0644); err != nil {
		p.logger.Error("error writing meta.json", "error", err, "metaPath", metaPath)
		http.Error(w, "Error saving file metadata", http.StatusInternalServerError)
		return
	}

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "OK",
		"objectID": newObjectName,
	})
}

func (p *ObjectsPlugin) downloadBinaryHandler(w http.ResponseWriter, r *http.Request) {

	// todo: ensure is get
	// todo: use p.prif to validate the request token
	// todo: get the uuid for the file
	// todo: read the file from the disk
	// todo: return the file

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Token validation: Ensure this logic is appropriate for your auth scheme.
	// If the token is part of the query or headers, extract and validate it here.
	// For this example, it's assumed RT_ValidateAuthToken handles extraction from the request.
	td, isValid := p.prif.RT_ValidateAuthToken(r) // td might be nil if token is not required for download or handled differently
	if !isValid {
		// Depending on policy, public downloads might be allowed, or this is a hard error.
		// For now, assume valid token is required.
		http.Error(w, "Unauthorized or Invalid Token", http.StatusUnauthorized)
		return
	}
	_ = td // if td is not used further, to avoid unused variable error. Re-evaluate if user context from token is needed.

	objectID := r.URL.Query().Get("id")
	if objectID == "" {
		http.Error(w, "Missing object ID", http.StatusBadRequest)
		return
	}

	objectDir := filepath.Join(p.uploadDir, objectID)
	dataFile := filepath.Join(objectDir, "file.data")
	metaFile := filepath.Join(objectDir, "meta.json")

	metaBytes, err := os.ReadFile(metaFile)
	if err != nil {
		p.logger.Error("failed to read meta file", "error", err, "objectID", objectID)
		if os.IsNotExist(err) {
			http.Error(w, "Object metadata not found", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to read object metadata", http.StatusInternalServerError)
		}
		return
	}

	var meta MetaData
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		p.logger.Error("failed to parse meta file", "error", err, "objectID", objectID)
		http.Error(w, "Failed to parse object metadata", http.StatusInternalServerError)
		return
	}

	// Security check: Optional, but good practice if td.UUID is available and should match meta.UserUUID
	// if td != nil && td.UUID != meta.UserUUID {
	// 	p.logger.Warn("User UUID mismatch for object access", "tokenUserUUID", td.UUID, "objectUserUUID", meta.UserUUID, "objectID", objectID)
	// 	http.Error(w, "Forbidden", http.StatusForbidden)
	// 	return
	// }

	file, err := os.Open(dataFile)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Object data not found", http.StatusNotFound)
		} else {
			p.logger.Error("failed to open data file", "error", err, "objectID", objectID)
			http.Error(w, "Failed to read object", http.StatusInternalServerError)
		}
		return
	}
	defer file.Close()

	originalFilename := meta.OriginalFilename
	if originalFilename == "" { // Fallback filename
		originalFilename = "downloaded_object"
	}

	// Set Content-Type. Use meta.ContentType if available, otherwise default to application/octet-stream.
	contentType := meta.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", originalFilename))

	// Serve the file. http.ServeContent handles Range requests, ETag, If-Modified-Since, etc.
	// We pass meta.UploadDate as the modtime.
	// The name argument to ServeContent is used for MIME type detection if Content-Type is not set,
	// but since we set it, it's mostly for logging/debugging by ServeContent.
	http.ServeContent(w, r, originalFilename, meta.UploadDate, file)
}
