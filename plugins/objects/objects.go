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

	"github.com/InsulaLabs/insi/models"
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



		Path "upload"








*/

// MetaData stores information about the uploaded object.
type MetaData struct {
	OriginalFilename string    `json:"originalFilename"`
	HostedAtNodeID   string    `json:"hostedAtNodeID"`
	FileSize         int64     `json:"fileSize"`
	UploadDate       time.Time `json:"uploadDate"`
	UserUUID         string    `json:"userUUID"`
	ObjectUUID       string    `json:"objectUUID"` // The UUID part of the object name
	ContentType      string    `json:"contentType"`
	SHA256           string    `json:"sha256"` // The SHA256 hash of the file.data file
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
	// It's important to parse the form before accessing r.FormValue
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		p.logger.Error("could not parse multipart form", "error", err)
		http.Error(w, "Error parsing form data: "+err.Error(), http.StatusBadRequest)
		return
	}

	clientSha256 := r.FormValue("clientSha256")
	if clientSha256 == "" {
		p.logger.Error("clientSha256 form field is missing")
		http.Error(w, "Missing clientSha256 form field", http.StatusBadRequest)
		return
	}
	if len(clientSha256) != 64 { // SHA256 hex string length
		p.logger.Error("clientSha256 has invalid length", "length", len(clientSha256))
		http.Error(w, "Invalid clientSha256 format", http.StatusBadRequest)
		return
	}

	// Check if an object with this SHA256 already exists
	sha256Key := fmt.Sprintf("plugin:objects:sha256:%s", clientSha256)
	existingObjectFileUUID, err := p.prif.RT_Get(sha256Key)
	if err == nil && existingObjectFileUUID != "" {
		// Object with this SHA256 exists, verify its metadata also exists
		metaKey := fmt.Sprintf("plugin:objects:%s", existingObjectFileUUID)
		_, metaErr := p.prif.RT_Get(metaKey)
		if metaErr == nil {
			p.logger.Info("object with same SHA256 already exists", "clientSha256", clientSha256, "existingObjectFileUUID", existingObjectFileUUID)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK) // Or perhaps http.StatusConflict if preferred, but client expects OK for existing.
			json.NewEncoder(w).Encode(map[string]string{
				"status":           "OK",
				"objectID":         existingObjectFileUUID,
				"message":          "Object with this SHA256 hash already exists.",
				"clientSha256":     clientSha256,
				"calculatedSha256": clientSha256, // Since it matched an existing one
			})
			return
		}
		p.logger.Warn("found sha256 key but corresponding meta key was not found or error", "sha256Key", sha256Key, "metaKey", metaKey, "error", metaErr)
		// Proceed to upload if meta check failed, as the previous entry might be corrupted/incomplete.
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
	// Defer closing dst until after hash calculation is confirmed or copy is done.

	// TeeReader to write to file and hasher simultaneously
	hasher := sha256.New()
	teeReader := io.TeeReader(file, hasher)

	// Copy the uploaded file data to the new file, and get its size.
	fileSize, err := io.Copy(dst, teeReader)
	if err != nil {
		dst.Close()             // Close dst before potential removal
		os.RemoveAll(objectDir) // Attempt to clean up
		p.logger.Error("error copying uploaded file to destination", "error", err, "dataPath", dataPath)
		http.Error(w, "Error saving file", http.StatusInternalServerError)
		return
	}
	dst.Close() // Close dst now that copy is complete.

	calculatedSha256 := hex.EncodeToString(hasher.Sum(nil))

	// Verify client-provided hash against calculated hash
	if calculatedSha256 != clientSha256 {
		os.RemoveAll(objectDir) // Clean up the stored file and directory
		p.logger.Error("SHA256 hash mismatch", "clientSha256", clientSha256, "calculatedSha256", calculatedSha256, "originalFilename", originalFilename)
		http.Error(w, fmt.Sprintf("SHA256 hash mismatch. Client: %s, Server: %s", clientSha256, calculatedSha256), http.StatusBadRequest) // Consider 409 Conflict
		return
	}

	// Write the SHA256 hash to file.data.sha256 (optional, as it's in meta.json and db now)
	// For consistency, let's keep it for local file system integrity if needed.
	sha256FilePath := filepath.Join(objectDir, "file.data.sha256")
	if err := os.WriteFile(sha256FilePath, []byte(calculatedSha256), 0644); err != nil {
		os.RemoveAll(objectDir) // Clean up
		p.logger.Error("error writing sha256 file", "error", err, "sha256Path", sha256FilePath)
		http.Error(w, "Error saving file metadata", http.StatusInternalServerError)
		return
	}

	// Create and write meta.json
	meta := MetaData{
		OriginalFilename: originalFilename,
		HostedAtNodeID:   p.prif.RT_GetNodeID(),
		FileSize:         fileSize,
		UploadDate:       time.Now(),
		UserUUID:         userUUID,
		ObjectUUID:       objectFileUUID, // Just the file's UUID part
		ContentType:      contentType,
		SHA256:           calculatedSha256, // Use the verified calculated hash
	}

	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		os.RemoveAll(objectDir) // Clean up
		p.logger.Error("error marshalling meta.json", "error", err)
		http.Error(w, "Error creating file metadata", http.StatusInternalServerError)
		return
	}

	metaPath := filepath.Join(objectDir, "meta.json")
	if err := os.WriteFile(metaPath, metaBytes, 0644); err != nil {
		os.RemoveAll(objectDir) // Clean up
		p.logger.Error("error writing meta.json", "error", err, "metaPath", metaPath)
		http.Error(w, "Error saving file metadata", http.StatusInternalServerError)
		return
	}

	// Store record in database (UUID -> Meta)
	metaKey := fmt.Sprintf("plugin:objects:%s", objectFileUUID)
	if err := p.prif.RT_Set(models.KVPayload{
		Key:   metaKey,
		Value: string(metaBytes),
	}); err != nil {
		os.RemoveAll(objectDir) // Clean up
		p.logger.Error("error storing meta.json in database", "error", err, "metaKey", metaKey)
		http.Error(w, "Error saving file metadata to database", http.StatusInternalServerError)
		return
	}

	// Store the SHA256 -> ObjectFileUUID mapping in the database
	// This uses the already defined sha256Key
	if err := p.prif.RT_Set(models.KVPayload{
		Key:   sha256Key, // Key was defined earlier: plugin:objects:sha256:<calculatedSha256>
		Value: objectFileUUID,
	}); err != nil {
		// If this fails, we have an object but no quick SHA lookup.
		// For now, log the error but don't delete the object as meta is already stored.
		// Consider a rollback or cleanup strategy for production.
		p.logger.Error("error storing sha256 to objectFileUUID mapping in database", "error", err, "sha256Key", sha256Key, "objectFileUUID", objectFileUUID)
		// Not returning an error to client here as the primary object storage succeeded.
	}

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":           "OK",
		"objectID":         objectFileUUID, // Return only the file's UUID part
		"clientSha256":     clientSha256,
		"calculatedSha256": calculatedSha256,
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
	// metaFile := filepath.Join(objectDir, "meta.json") // This is no longer used as we rely on metaDataFromDB

	/*

		Here we attempt to get the record from the database about the object.
		Then we check if the object is hosted at the same node as the current node.
		If not, we redirect to the node that the object is hosted at.
		If it is, we serve the file from the local node.

	*/

	metaKey := fmt.Sprintf("plugin:objects:%s", objectFileUUID)
	metaBytesFromDB, err := p.prif.RT_Get(metaKey)
	if err != nil {
		p.logger.Error("failed to get meta.json from database", "error", err, "metaKey", metaKey)
		http.Error(w, "Failed to read object metadata", http.StatusInternalServerError)
		return
	}

	var metaDataFromDB MetaData
	if err := json.Unmarshal([]byte(metaBytesFromDB), &metaDataFromDB); err != nil {
		p.logger.Error("failed to parse meta.json from database", "error", err, "metaKey", metaKey)
		http.Error(w, "Failed to parse object metadata", http.StatusInternalServerError)
		return
	}

	// Reconstruct the full object name for path resolution using the owner's UUID from metadata.
	fullObjectNameForPath = fmt.Sprintf("%s:%s", metaDataFromDB.UserUUID, objectFileUUID)
	objectDir = filepath.Join(p.uploadDir, fullObjectNameForPath)
	dataFile = filepath.Join(objectDir, "file.data")

	// If the object is hosted at a different node, redirect to the node that the object is hosted at
	if metaDataFromDB.HostedAtNodeID != p.prif.RT_GetNodeID() {

		hostedNodeCfg, ok := p.prif.RT_GetClusterConfig().Nodes[metaDataFromDB.HostedAtNodeID]
		if !ok {
			p.logger.Error("hosted node not found in cluster config", "hostedNodeID", metaDataFromDB.HostedAtNodeID)
			http.Error(w, "Hosted node not found", http.StatusInternalServerError)
			return
		}

		if hostedNodeCfg.ClientDomain == "" {
			p.logger.Error("hosted node has no client domain", "hostedNodeID", metaDataFromDB.HostedAtNodeID)
			http.Error(w, "Hosted node has no client domain", http.StatusInternalServerError)
			return
		}

		//Redirect to the node that the object is hosted at
		clientDomainOfHostedNode := hostedNodeCfg.ClientDomain
		// write to the header the client domain of the hosted node
		w.Header().Set("X-Insid-Client-Domain", clientDomainOfHostedNode)
		w.Header().Set("X-Insid-Node-ID", metaDataFromDB.HostedAtNodeID)
		http.Redirect(
			w, r,
			fmt.Sprintf("https://%s/objects/download?id=%s", clientDomainOfHostedNode, objectFileUUID),
			http.StatusTemporaryRedirect,
		)
		return
	}

	// If the object is hosted at the same node, serve the file from the local node
	// The metadata is already available in metaDataFromDB, no need to read local meta.json again.

	if td.UUID != metaDataFromDB.UserUUID {
		p.logger.Warn("User UUID mismatch for object access", "tokenUserUUID", td.UUID, "objectUserUUID", metaDataFromDB.UserUUID, "objectID", objectFileUUID)
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

	originalFilename := metaDataFromDB.OriginalFilename
	if originalFilename == "" { // Fallback filename
		originalFilename = "downloaded_object"
	}

	contentType := metaDataFromDB.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", originalFilename))
	http.ServeContent(w, r, originalFilename, metaDataFromDB.UploadDate, file)
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

	metaKey := fmt.Sprintf("plugin:objects:%s", objectFileUUID)
	metaBytesFromDB, err := p.prif.RT_Get(metaKey)
	if err != nil {
		p.logger.Error("failed to get meta.json from database", "error", err, "metaKey", metaKey)
		http.Error(w, "Failed to read object metadata", http.StatusInternalServerError)
		return
	}

	var metaDataFromDB MetaData
	if err := json.Unmarshal([]byte(metaBytesFromDB), &metaDataFromDB); err != nil {
		p.logger.Error("failed to parse meta file for hash", "error", err, "objectID", objectFileUUID)
		http.Error(w, "Failed to parse object metadata", http.StatusInternalServerError)
		return
	}

	if td.UUID != metaDataFromDB.UserUUID {
		p.logger.Warn("User UUID mismatch for object hash access", "tokenUserUUID", td.UUID, "objectUserUUID", metaDataFromDB.UserUUID, "objectID", objectFileUUID)
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"objectID": objectFileUUID, // Return only the file's UUID part
		"sha256":   metaDataFromDB.SHA256,
	})
}
