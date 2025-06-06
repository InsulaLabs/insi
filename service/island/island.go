package island

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	db_models "github.com/InsulaLabs/insi/db/models"
	service_models "github.com/InsulaLabs/insi/service/models"

	"github.com/InsulaLabs/insi/runtime"
	"github.com/google/uuid"
)

/*

The server mounts plugins by name on "/" so if we have a plugin called "static"
then we can serve directly from "/static" automatically, and serve out the static
files from the dir handed to the plugin on creation.

ALlowing the server to server static files without modifying the internals
*/

type IslandPlugin struct {
	logger    *slog.Logger
	prif      runtime.PluginRuntimeIF
	staticDir string
	startedAt time.Time
}

var _ runtime.Plugin = &IslandPlugin{}

func New(logger *slog.Logger) *IslandPlugin {
	return &IslandPlugin{
		logger: logger,
	}
}

func (p *IslandPlugin) GetName() string {
	return "island"
}

func (p *IslandPlugin) Init(prif runtime.PluginRuntimeIF) *runtime.PluginImplError {
	p.prif = prif
	if _, err := os.Stat(p.staticDir); os.IsNotExist(err) {
		return &runtime.PluginImplError{Err: fmt.Errorf("static directory does not exist, so plugin cannot be initialized: %s", p.staticDir)}
	}
	p.startedAt = time.Now()

	fs := http.FileServer(http.Dir(p.staticDir))
	err := p.prif.RT_MountStatic(p, fs)
	if err != nil {
		return &runtime.PluginImplError{Err: fmt.Errorf("failed to mount static files: %w", err)}
	}
	return nil
}

func (p *IslandPlugin) GetRoutes() []runtime.PluginRoute {
	// No routes for static plugin
	return []runtime.PluginRoute{
		{
			Path:    "/new",
			Handler: http.HandlerFunc(p.handleNewIsland),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/delete",
			Handler: http.HandlerFunc(p.handleDeleteIsland),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/update/name", // POST NO URL PARAMS _EVER_
			Handler: http.HandlerFunc(p.handleUpdateIslandName),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/update/description", // POST NO URL PARAMS _EVER_
			Handler: http.HandlerFunc(p.handleUpdateIslandDescription),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/update/model-slug", // POST NO URL PARAMS _EVER_
			Handler: http.HandlerFunc(p.handleUpdateIslandModelSlug),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/update/add/resources", // POST NO URL PARAMS _EVER_ []Resource
			Handler: http.HandlerFunc(p.handleUpdateIslandAddResources),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/update/remove/resources",                            // POST NO URL PARAMS _EVER_ []string (UUIDS OF RESOURCES)
			Handler: http.HandlerFunc(p.handleUpdateIslandRemoveResources), // Removal does NOT mean deleting the object it means REMOVING the "Resource" tracker object "island.Resource"
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/iterate/resources",                             // GET NO URL PARAMS _EVER_
			Handler: http.HandlerFunc(p.handleIterateIslandResources), // limit, offset (same as kvstore)
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/iterate/islands",                       // GET NO URL PARAMS _EVER_
			Handler: http.HandlerFunc(p.handleIterateIslands), // limit, offset (same as kvstore)
			Limit:   10,
			Burst:   10,
		},
	}
}

func (p *IslandPlugin) handleNewIsland(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req NewIslandRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	// See if slug is already taken
	slugKey := service_models.GetIslandSlugKey(td.UUID, req.ModelSlug)
	_, err = p.prif.RT_Get(slugKey)
	if err == nil {
		http.Error(w, "Slug already taken - must be unique", http.StatusBadRequest)
		return
	} else if !strings.Contains(err.Error(), "key not found") {
		// "key not found" is the expected error. Any other error is a server issue.
		p.logger.Error("failed to check for existing slug", "error", err)
		http.Error(w, "Failed to check for existing slug", http.StatusInternalServerError)
		return
	}

	// Create island
	island := service_models.Island{
		Entity:      td.Entity,
		EntityUUID:  td.UUID,
		UUID:        uuid.New().String(),
		Name:        req.Name,
		ModelSlug:   req.ModelSlug,
		Description: req.Description,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	islandBytes, err := json.Marshal(island)
	if err != nil {
		http.Error(w, "Failed to marshal island", http.StatusInternalServerError)
		return
	}

	islandKey := service_models.GetIslandKey(td.UUID, island.UUID)

	if err = p.prif.RT_Set(db_models.KVPayload{
		Key:   islandKey,
		Value: string(islandBytes),
	}); err != nil {
		http.Error(w, "Failed to set island", http.StatusInternalServerError)
		return
	}

	if err = p.prif.RT_Set(db_models.KVPayload{
		Key:   slugKey,
		Value: island.UUID,
	}); err != nil {
		http.Error(w, "Failed to set island slug", http.StatusInternalServerError)
		if err := p.prif.RT_Delete(islandKey); err != nil {
			p.logger.Error("Failed to delete island after failed to set island slug", "error", err)
		}
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write(islandBytes)
}

func (p *IslandPlugin) handleDeleteIsland(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req DeleteIslandRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	if req.UUID == "" {
		http.Error(w, "UUID is required", http.StatusBadRequest)
		return
	}

	islandKey := service_models.GetIslandKey(td.UUID, req.UUID)

	islandData, err := p.prif.RT_Get(islandKey)
	if err != nil {
		http.Error(w, "Failed to get island for deletion", http.StatusInternalServerError)
		return
	}

	var island service_models.Island
	if err := json.Unmarshal([]byte(islandData), &island); err != nil {
		http.Error(w, "Failed to unmarshal island", http.StatusInternalServerError)
		return
	}

	slugKey := service_models.GetIslandSlugKey(td.UUID, island.ModelSlug)

	if island.EntityUUID != td.UUID && !p.prif.RT_IsRoot(td) {
		http.Error(w, "Permission denied. Island does not belong to you.", http.StatusForbidden)
		return
	}

	httpStatus := http.StatusOK
	if err = p.prif.RT_Delete(islandKey); err != nil {
		p.logger.Error("Failed to delete island", "error", err)
		httpStatus = http.StatusInternalServerError
	}

	if err = p.prif.RT_Delete(slugKey); err != nil {
		p.logger.Error("Failed to delete island slug", "error", err)
		httpStatus = http.StatusInternalServerError
	}

	w.WriteHeader(httpStatus)
	w.Write([]byte("Island deleted"))
}

func (p *IslandPlugin) handleUpdateIslandName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req UpdateIslandNameRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "Name is required", http.StatusBadRequest)
		return
	}
	if req.UUID == "" {
		http.Error(w, "Island UUID is required", http.StatusBadRequest)
		return
	}

	islandKey := service_models.GetIslandKey(td.UUID, req.UUID)
	islandData, err := p.prif.RT_Get(islandKey)
	if err != nil {
		http.Error(w, "Failed to get island", http.StatusInternalServerError)
		return
	}

	var island service_models.Island
	if err := json.Unmarshal([]byte(islandData), &island); err != nil {
		http.Error(w, "Failed to unmarshal island", http.StatusInternalServerError)
		return
	}

	if island.EntityUUID != td.UUID && !p.prif.RT_IsRoot(td) {
		http.Error(w, "Permission denied", http.StatusForbidden)
		return
	}

	island.Name = req.Name
	island.UpdatedAt = time.Now()

	updatedIslandBytes, err := json.Marshal(island)
	if err != nil {
		http.Error(w, "Failed to marshal island", http.StatusInternalServerError)
		return
	}

	if err := p.prif.RT_Set(db_models.KVPayload{Key: islandKey, Value: string(updatedIslandBytes)}); err != nil {
		http.Error(w, "Failed to update island", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(updatedIslandBytes)
}

func (p *IslandPlugin) handleUpdateIslandDescription(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req UpdateIslandDescriptionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	if len(req.Description) > 1024 {
		http.Error(w, "Description exceeds max length of 1024", http.StatusBadRequest)
		return
	}
	if req.UUID == "" {
		http.Error(w, "Island UUID is required", http.StatusBadRequest)
		return
	}

	islandKey := service_models.GetIslandKey(td.UUID, req.UUID)
	islandData, err := p.prif.RT_Get(islandKey)
	if err != nil {
		http.Error(w, "Failed to get island", http.StatusInternalServerError)
		return
	}

	var island service_models.Island
	if err := json.Unmarshal([]byte(islandData), &island); err != nil {
		http.Error(w, "Failed to unmarshal island", http.StatusInternalServerError)
		return
	}

	if island.EntityUUID != td.UUID && !p.prif.RT_IsRoot(td) {
		http.Error(w, "Permission denied", http.StatusForbidden)
		return
	}

	island.Description = req.Description
	island.UpdatedAt = time.Now()

	updatedIslandBytes, err := json.Marshal(island)
	if err != nil {
		http.Error(w, "Failed to marshal island", http.StatusInternalServerError)
		return
	}

	if err := p.prif.RT_Set(db_models.KVPayload{Key: islandKey, Value: string(updatedIslandBytes)}); err != nil {
		http.Error(w, "Failed to update island", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(updatedIslandBytes)
}

func (p *IslandPlugin) handleUpdateIslandModelSlug(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req UpdateIslandModelSlugRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	if req.ModelSlug == "" {
		http.Error(w, "Model slug is required", http.StatusBadRequest)
		return
	}
	if req.UUID == "" {
		http.Error(w, "Island UUID is required", http.StatusBadRequest)
		return
	}

	newSlugKey := service_models.GetIslandSlugKey(td.UUID, req.ModelSlug)
	if _, err := p.prif.RT_Get(newSlugKey); err == nil {
		http.Error(w, "Model slug already taken", http.StatusBadRequest)
		return
	}

	islandKey := service_models.GetIslandKey(td.UUID, req.UUID)
	islandData, err := p.prif.RT_Get(islandKey)
	if err != nil {
		http.Error(w, "Failed to get island", http.StatusInternalServerError)
		return
	}

	var island service_models.Island
	if err := json.Unmarshal([]byte(islandData), &island); err != nil {
		http.Error(w, "Failed to unmarshal island", http.StatusInternalServerError)
		return
	}

	if island.EntityUUID != td.UUID && !p.prif.RT_IsRoot(td) {
		http.Error(w, "Permission denied", http.StatusForbidden)
		return
	}

	oldSlug := island.ModelSlug
	oldSlugKey := service_models.GetIslandSlugKey(td.UUID, island.ModelSlug)
	island.ModelSlug = req.ModelSlug
	island.UpdatedAt = time.Now()

	updatedIslandBytes, err := json.Marshal(island)
	if err != nil {
		http.Error(w, "Failed to marshal island", http.StatusInternalServerError)
		return
	}

	if err := p.prif.RT_Set(db_models.KVPayload{Key: islandKey, Value: string(updatedIslandBytes)}); err != nil {
		http.Error(w, "Failed to update island", http.StatusInternalServerError)
		return
	}

	if err := p.prif.RT_Set(db_models.KVPayload{Key: newSlugKey, Value: island.UUID}); err != nil {
		http.Error(w, "Failed to set new island slug", http.StatusInternalServerError)
		// Attempt to rollback island update
		island.ModelSlug = oldSlug
		originalIslandBytes, _ := json.Marshal(island)
		p.prif.RT_Set(db_models.KVPayload{Key: islandKey, Value: string(originalIslandBytes)})
		return
	}

	if err := p.prif.RT_Delete(oldSlugKey); err != nil {
		p.logger.Error("Failed to delete old island slug", "error", err)
	}

	w.WriteHeader(http.StatusOK)
	w.Write(updatedIslandBytes)
}

func (p *IslandPlugin) handleUpdateIslandAddResources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req UpdateIslandAddResourcesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	islandKey := service_models.GetIslandKey(td.UUID, req.IslandUUID)
	islandData, err := p.prif.RT_Get(islandKey)
	if err != nil {
		http.Error(w, "Failed to get island", http.StatusInternalServerError)
		return
	}
	var island service_models.Island
	if err := json.Unmarshal([]byte(islandData), &island); err != nil {
		http.Error(w, "Failed to unmarshal island", http.StatusInternalServerError)
		return
	}
	if island.EntityUUID != td.UUID && !p.prif.RT_IsRoot(td) {
		http.Error(w, "Permission denied", http.StatusForbidden)
		return
	}

	for _, resource := range req.Resources {
		if resource.UUID == "" {
			resource.UUID = uuid.New().String()
		}
		resource.Entity = td.Entity
		resource.EntityUUID = td.UUID
		resource.CreatedAt = time.Now()
		resource.UpdatedAt = time.Now()

		resourceBytes, err := json.Marshal(resource)
		if err != nil {
			http.Error(w, "Failed to marshal resource", http.StatusInternalServerError)
			return // continuing would be ambiguous
		}

		resourceKey := service_models.GetIslandResourceKey(td.UUID, req.IslandUUID, resource.UUID)
		if err := p.prif.RT_Set(db_models.KVPayload{Key: resourceKey, Value: string(resourceBytes)}); err != nil {
			http.Error(w, "Failed to set resource", http.StatusInternalServerError)
			return // continuing would be ambiguous
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (p *IslandPlugin) handleUpdateIslandRemoveResources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req UpdateIslandRemoveResourcesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	// Optional: Check if the island exists and the user has permission.
	islandKey := service_models.GetIslandKey(td.UUID, req.IslandUUID)
	if _, err := p.prif.RT_Get(islandKey); err != nil {
		http.Error(w, "Island not found or permission denied", http.StatusNotFound)
		return
	}

	for _, resourceUUID := range req.ResourceUUIDs {
		resourceKey := service_models.GetIslandResourceKey(td.UUID, req.IslandUUID, resourceUUID)
		if err := p.prif.RT_Delete(resourceKey); err != nil {
			p.logger.Error("Failed to delete resource, may not exist", "error", err, "uuid", resourceUUID)
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (p *IslandPlugin) handleIterateIslandResources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req IterateIslandResourcesRequest
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.Limit > 1024 {
		req.Limit = 1024
	}
	if req.Offset < 0 {
		req.Offset = 0
	}

	// At this point, td.UUID is the entity UUID from the token.
	// We should allow iteration only for the token holder's islands.
	// Root users are an exception, but the current Get/Iterate interface is scoped to a single user's keyspace anyway.
	if req.IslandUUID == "" {
		http.Error(w, "IslandUUID is required", http.StatusBadRequest)
		return
	}

	// Verify the user owns the island they are trying to iterate resources from.
	islandKey := service_models.GetIslandKey(td.UUID, req.IslandUUID)
	if _, err := p.prif.RT_Get(islandKey); err != nil {
		http.Error(w, "Island not found or permission denied", http.StatusNotFound)
		return
	}

	prefix := service_models.GetIslandResourceIterationPrefix(td.UUID, req.IslandUUID)
	keys, err := p.prif.RT_Iterate(prefix, req.Offset, req.Limit)
	if err != nil {
		// If no keys are found for the prefix, the runtime returns an error.
		// We should handle this gracefully by returning an empty list.
		if strings.Contains(err.Error(), "key not found") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}
		http.Error(w, "Failed to list resources", http.StatusInternalServerError)
		return
	}

	resources := []service_models.Resource{}
	for _, key := range keys {
		value, err := p.prif.RT_Get(key)
		if err != nil {
			p.logger.Error("Failed to get resource value for key", "key", key, "error", err)
			continue
		}

		var resource service_models.Resource
		if err := json.Unmarshal([]byte(value), &resource); err != nil {
			p.logger.Debug("Failed to unmarshal as resource, skipping", "key", key, "error", err)
			continue
		}
		resources = append(resources, resource)
	}

	respBytes, err := json.Marshal(resources)
	if err != nil {
		http.Error(w, "Failed to marshal resources", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respBytes)
}

func (p *IslandPlugin) handleIterateIslands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Any authenticated user can list their own islands. No root check needed.
	// if !p.prif.RT_IsRoot(td) {
	// 	http.Error(w, "Permission denied", http.StatusForbidden)
	// 	return
	// }

	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 {
		limit = 100
	}
	if limit > 1024 {
		limit = 1024
	}

	offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
	if err != nil || offset < 0 {
		offset = 0
	}

	prefix := service_models.GetIslandIterationPrefix(td.UUID)
	keys, err := p.prif.RT_Iterate(prefix, offset, limit)
	if err != nil {
		// If no keys are found for the prefix, the runtime returns an error.
		// We should handle this gracefully by returning an empty list.
		if strings.Contains(err.Error(), "key not found") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}
		http.Error(w, "Failed to list islands", http.StatusInternalServerError)
		return
	}

	islands := []service_models.Island{}
	for _, key := range keys {
		// Ensure we're not picking up slug or resource keys by checking structure
		if strings.Contains(key, ":resource:") || strings.Count(key, ":") != 3 {
			continue
		}

		value, err := p.prif.RT_Get(key)
		if err != nil {
			p.logger.Error("Failed to get island value for key", "key", key, "error", err)
			continue
		}

		var island service_models.Island
		if err := json.Unmarshal([]byte(value), &island); err != nil {
			p.logger.Error("Failed to unmarshal island, skipping", "key", key, "error", err)
			continue
		}
		islands = append(islands, island)
	}

	respBytes, err := json.Marshal(islands)
	if err != nil {
		http.Error(w, "Failed to marshal islands", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respBytes)
}
