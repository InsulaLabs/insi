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
		{Path: "hash", Handler: http.HandlerFunc(p.getObjectHashHandler), Limit: 10, Burst: 10},
	}
}

// / -------- routes --------
func (p *ObjectsPlugin) uploadBinaryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
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
		"objectID": objectFileUUID, // Return only the file's UUID part
	})
}

func (p *ObjectsPlugin) downloadBinaryHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized or Invalid Token", http.StatusUnauthorized)
		return
	}

	// Client provides objectFileUUID as "id"
	objectFileUUID := r.URL.Query().Get("id")
	if objectFileUUID == "" {
		http.Error(w, "Missing object ID in query parameter 'id'", http.StatusBadRequest)
		return
	}

	// Reconstruct the full object name for path resolution using userUUID from token and objectFileUUID from client
	fullObjectNameForPath := fmt.Sprintf("%s:%s", td.UUID, objectFileUUID)
	objectDir := filepath.Join(p.uploadDir, fullObjectNameForPath)
	dataFile := filepath.Join(objectDir, "file.data")
	metaFile := filepath.Join(objectDir, "meta.json")

	metaBytes, err := os.ReadFile(metaFile)
	if err != nil {
		p.logger.Error("failed to read meta file", "error", err, "objectID", objectFileUUID, "resolvedPath", metaFile)
		if os.IsNotExist(err) {
			http.Error(w, "Object metadata not found", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to read object metadata", http.StatusInternalServerError)
		}
		return
	}

	var meta MetaData
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		p.logger.Error("failed to parse meta file", "error", err, "objectID", objectFileUUID)
		http.Error(w, "Failed to parse object metadata", http.StatusInternalServerError)
		return
	}

	if td.UUID != meta.UserUUID {
		p.logger.Warn("User UUID mismatch for object access", "tokenUserUUID", td.UUID, "objectUserUUID", meta.UserUUID, "objectID", objectFileUUID)
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	file, err := os.Open(dataFile)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Object data not found", http.StatusNotFound)
		} else {
			p.logger.Error("failed to open data file", "error", err, "objectID", objectFileUUID, "resolvedPath", dataFile)
			http.Error(w, "Failed to read object", http.StatusInternalServerError)
		}
		return
	}
	defer file.Close()

	originalFilename := meta.OriginalFilename
	if originalFilename == "" { // Fallback filename
		originalFilename = "downloaded_object"
	}

	contentType := meta.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", originalFilename))
	http.ServeContent(w, r, originalFilename, meta.UploadDate, file)
}

func (p *ObjectsPlugin) getObjectHashHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized or Invalid Token", http.StatusUnauthorized)
		return
	}

	objectFileUUID := r.URL.Query().Get("id")
	if objectFileUUID == "" {
		http.Error(w, "Missing object ID in query parameter 'id'", http.StatusBadRequest)
		return
	}

	// Reconstruct the full object name for path resolution using userUUID from token and objectFileUUID from client
	fullObjectNameForPath := fmt.Sprintf("%s:%s", td.UUID, objectFileUUID)
	objectDir := filepath.Join(p.uploadDir, fullObjectNameForPath)
	metaFile := filepath.Join(objectDir, "meta.json")
	hashFilePath := filepath.Join(objectDir, "file.data.sha256")

	// First, verify ownership by checking meta.json
	metaBytes, err := os.ReadFile(metaFile)
	if err != nil {
		p.logger.Error("failed to read meta file for hash", "error", err, "objectID", objectFileUUID, "resolvedPath", metaFile)
		if os.IsNotExist(err) {
			http.Error(w, "Object metadata not found", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to read object metadata", http.StatusInternalServerError)
		}
		return
	}

	var meta MetaData
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		p.logger.Error("failed to parse meta file for hash", "error", err, "objectID", objectFileUUID)
		http.Error(w, "Failed to parse object metadata", http.StatusInternalServerError)
		return
	}

	if td.UUID != meta.UserUUID {
		p.logger.Warn("User UUID mismatch for object hash access", "tokenUserUUID", td.UUID, "objectUserUUID", meta.UserUUID, "objectID", objectFileUUID)
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// If ownership is verified, proceed to read the hash file
	hashBytes, err := os.ReadFile(hashFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			p.logger.Warn("hash file not found", "objectID", objectFileUUID, "resolvedPath", hashFilePath)
			http.Error(w, "Object hash not found", http.StatusNotFound)
		} else {
			p.logger.Error("failed to read hash file", "error", err, "objectID", objectFileUUID, "resolvedPath", hashFilePath)
			http.Error(w, "Failed to read object hash", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"objectID": objectFileUUID, // Return only the file's UUID part
		"sha256":   string(hashBytes),
	})
}
