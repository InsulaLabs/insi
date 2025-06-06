package provider

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	db_models "github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/runtime"
	service_models "github.com/InsulaLabs/insi/service/models"
	"github.com/google/uuid"
)

type ProviderPlugin struct {
	logger *slog.Logger
	prif   runtime.ServiceRuntimeIF
}

type SetPreferredProviderRequest struct {
	ProviderUUID string `json:"provider_uuid"`
}

var _ runtime.Service = &ProviderPlugin{}

func New(logger *slog.Logger) *ProviderPlugin {
	return &ProviderPlugin{
		logger: logger,
	}
}

func (p *ProviderPlugin) GetName() string {
	return "provider"
}

func (p *ProviderPlugin) Init(prif runtime.ServiceRuntimeIF) *runtime.ServiceImplError {
	p.prif = prif
	return nil
}

func (p *ProviderPlugin) GetRoutes() []runtime.ServiceRoute {
	return []runtime.ServiceRoute{
		{
			Path:    "/new",
			Handler: http.HandlerFunc(p.handleNewProvider),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/delete",
			Handler: http.HandlerFunc(p.handleDeleteProvider),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/update/display-name",
			Handler: http.HandlerFunc(p.handleUpdateProviderDisplayName),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/update/api-key",
			Handler: http.HandlerFunc(p.handleUpdateProviderAPIKey),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/update/base-url",
			Handler: http.HandlerFunc(p.handleUpdateProviderBaseURL),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/iterate/providers",
			Handler: http.HandlerFunc(p.handleIterateProviders),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/set-preferred",
			Handler: http.HandlerFunc(p.handleSetPreferredProvider),
			Limit:   10,
			Burst:   10,
		},
		{
			Path:    "/get-preferred",
			Handler: http.HandlerFunc(p.handleGetPreferredProvider),
			Limit:   10,
			Burst:   10,
		},
	}
}

func (p *ProviderPlugin) handleNewProvider(w http.ResponseWriter, r *http.Request) {
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

	var req NewProviderRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	provider := service_models.Provider{
		EntityUUID:  td.UUID,
		UUID:        uuid.New().String(),
		DisplayName: req.DisplayName,
		Provider:    service_models.SupportedProvider(req.Provider),
		APIKey:      req.APIKey,
		BaseURL:     req.BaseURL,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	providerBytes, err := json.Marshal(provider)
	if err != nil {
		http.Error(w, "Failed to marshal provider", http.StatusInternalServerError)
		return
	}

	providerKey := service_models.GetProviderKey(td.UUID, provider.UUID)

	if err = p.prif.RT_Set(db_models.KVPayload{
		Key:   providerKey,
		Value: string(providerBytes),
	}); err != nil {
		http.Error(w, "Failed to set provider", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write(providerBytes)
}

func (p *ProviderPlugin) handleDeleteProvider(w http.ResponseWriter, r *http.Request) {
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

	var req DeleteProviderRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	if req.UUID == "" {
		http.Error(w, "UUID is required", http.StatusBadRequest)
		return
	}

	providerKey := service_models.GetProviderKey(td.UUID, req.UUID)

	providerData, err := p.prif.RT_Get(providerKey)
	if err != nil {
		http.Error(w, "Failed to get provider for deletion", http.StatusInternalServerError)
		return
	}

	var provider service_models.Provider
	if err := json.Unmarshal([]byte(providerData), &provider); err != nil {
		http.Error(w, "Failed to unmarshal provider", http.StatusInternalServerError)
		return
	}

	if provider.EntityUUID != td.UUID && !p.prif.RT_IsRoot(td) {
		http.Error(w, "Permission denied. Provider does not belong to you.", http.StatusForbidden)
		return
	}

	if err = p.prif.RT_Delete(providerKey); err != nil {
		p.logger.Error("Failed to delete provider", "error", err)
		http.Error(w, "Failed to delete provider", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Provider deleted"))
}

func (p *ProviderPlugin) handleUpdateProviderDisplayName(w http.ResponseWriter, r *http.Request) {
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

	var req UpdateProviderDisplayNameRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	if req.DisplayName == "" {
		http.Error(w, "Display name is required", http.StatusBadRequest)
		return
	}
	if req.UUID == "" {
		http.Error(w, "Provider UUID is required", http.StatusBadRequest)
		return
	}

	providerKey := service_models.GetProviderKey(td.UUID, req.UUID)
	providerData, err := p.prif.RT_Get(providerKey)
	if err != nil {
		http.Error(w, "Failed to get provider", http.StatusInternalServerError)
		return
	}

	var provider service_models.Provider
	if err := json.Unmarshal([]byte(providerData), &provider); err != nil {
		http.Error(w, "Failed to unmarshal provider", http.StatusInternalServerError)
		return
	}

	if provider.EntityUUID != td.UUID && !p.prif.RT_IsRoot(td) {
		http.Error(w, "Permission denied", http.StatusForbidden)
		return
	}

	provider.DisplayName = req.DisplayName
	provider.UpdatedAt = time.Now()

	updatedProviderBytes, err := json.Marshal(provider)
	if err != nil {
		http.Error(w, "Failed to marshal provider", http.StatusInternalServerError)
		return
	}

	if err := p.prif.RT_Set(db_models.KVPayload{Key: providerKey, Value: string(updatedProviderBytes)}); err != nil {
		http.Error(w, "Failed to update provider", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(updatedProviderBytes)
}

func (p *ProviderPlugin) handleUpdateProviderAPIKey(w http.ResponseWriter, r *http.Request) {
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

	var req UpdateProviderAPIKeyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	if req.APIKey == "" {
		http.Error(w, "API key is required", http.StatusBadRequest)
		return
	}
	if req.UUID == "" {
		http.Error(w, "Provider UUID is required", http.StatusBadRequest)
		return
	}

	providerKey := service_models.GetProviderKey(td.UUID, req.UUID)
	providerData, err := p.prif.RT_Get(providerKey)
	if err != nil {
		http.Error(w, "Failed to get provider", http.StatusInternalServerError)
		return
	}

	var provider service_models.Provider
	if err := json.Unmarshal([]byte(providerData), &provider); err != nil {
		http.Error(w, "Failed to unmarshal provider", http.StatusInternalServerError)
		return
	}

	if provider.EntityUUID != td.UUID && !p.prif.RT_IsRoot(td) {
		http.Error(w, "Permission denied", http.StatusForbidden)
		return
	}

	provider.APIKey = req.APIKey
	provider.UpdatedAt = time.Now()

	updatedProviderBytes, err := json.Marshal(provider)
	if err != nil {
		http.Error(w, "Failed to marshal provider", http.StatusInternalServerError)
		return
	}

	if err := p.prif.RT_Set(db_models.KVPayload{Key: providerKey, Value: string(updatedProviderBytes)}); err != nil {
		http.Error(w, "Failed to update provider", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(updatedProviderBytes)
}

func (p *ProviderPlugin) handleUpdateProviderBaseURL(w http.ResponseWriter, r *http.Request) {
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

	var req UpdateProviderBaseURLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	if req.UUID == "" {
		http.Error(w, "Provider UUID is required", http.StatusBadRequest)
		return
	}

	providerKey := service_models.GetProviderKey(td.UUID, req.UUID)
	providerData, err := p.prif.RT_Get(providerKey)
	if err != nil {
		http.Error(w, "Failed to get provider", http.StatusInternalServerError)
		return
	}

	var provider service_models.Provider
	if err := json.Unmarshal([]byte(providerData), &provider); err != nil {
		http.Error(w, "Failed to unmarshal provider", http.StatusInternalServerError)
		return
	}

	if provider.EntityUUID != td.UUID && !p.prif.RT_IsRoot(td) {
		http.Error(w, "Permission denied", http.StatusForbidden)
		return
	}

	provider.BaseURL = req.BaseURL
	provider.UpdatedAt = time.Now()

	updatedProviderBytes, err := json.Marshal(provider)
	if err != nil {
		http.Error(w, "Failed to marshal provider", http.StatusInternalServerError)
		return
	}

	if err := p.prif.RT_Set(db_models.KVPayload{Key: providerKey, Value: string(updatedProviderBytes)}); err != nil {
		http.Error(w, "Failed to update provider", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(updatedProviderBytes)
}

func (p *ProviderPlugin) handleIterateProviders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

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

	prefix := service_models.GetProviderIterationPrefix(td.UUID)
	keys, err := p.prif.RT_Iterate(prefix, offset, limit)
	if err != nil {
		if strings.Contains(err.Error(), "key not found") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}
		http.Error(w, "Failed to list providers", http.StatusInternalServerError)
		return
	}

	providers := []service_models.Provider{}
	for _, key := range keys {
		value, err := p.prif.RT_Get(key)
		if err != nil {
			p.logger.Error("Failed to get provider value for key", "key", key, "error", err)
			continue
		}

		var provider service_models.Provider
		if err := json.Unmarshal([]byte(value), &provider); err != nil {
			p.logger.Error("Failed to unmarshal provider, skipping", "key", key, "error", err)
			continue
		}
		providers = append(providers, provider)
	}

	respBytes, err := json.Marshal(providers)
	if err != nil {
		http.Error(w, "Failed to marshal providers", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respBytes)
}

func (p *ProviderPlugin) handleSetPreferredProvider(w http.ResponseWriter, r *http.Request) {
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

	var req SetPreferredProviderRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	if req.ProviderUUID == "" {
		http.Error(w, "Provider UUID is required", http.StatusBadRequest)
		return
	}

	// verification that the provider belongs to the user
	providerKey := service_models.GetProviderKey(td.UUID, req.ProviderUUID)
	providerData, err := p.prif.RT_Get(providerKey)
	if err != nil {
		http.Error(w, "Failed to get provider to verify ownership", http.StatusInternalServerError)
		return
	}
	if providerData == "" {
		http.Error(w, "Provider not found", http.StatusNotFound)
		return
	}
	var provider service_models.Provider
	if err := json.Unmarshal([]byte(providerData), &provider); err != nil {
		http.Error(w, "Failed to unmarshal provider for verification", http.StatusInternalServerError)
		return
	}

	if provider.EntityUUID != td.UUID && !p.prif.RT_IsRoot(td) {
		http.Error(w, "Permission denied. Provider does not belong to you.", http.StatusForbidden)
		return
	}

	// Now set the preferred provider
	preferredProviderKey := service_models.GetPreferredProviderKey(td.UUID)
	if err = p.prif.RT_Set(db_models.KVPayload{
		Key:   preferredProviderKey,
		Value: req.ProviderUUID,
	}); err != nil {
		http.Error(w, "Failed to set preferred provider", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Preferred provider set"))
}

func (p *ProviderPlugin) handleGetPreferredProvider(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, isValid := p.prif.RT_ValidateAuthToken(r, false)
	if !isValid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	preferredProviderKey := service_models.GetPreferredProviderKey(td.UUID)
	providerUUID, err := p.prif.RT_Get(preferredProviderKey)
	if err != nil {
		if strings.Contains(err.Error(), "key not found") {
			http.Error(w, "Preferred provider not set", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get preferred provider", http.StatusInternalServerError)
		return
	}

	providerKey := service_models.GetProviderKey(td.UUID, providerUUID)
	providerData, err := p.prif.RT_Get(providerKey)
	if err != nil {
		http.Error(w, "Failed to get preferred provider details", http.StatusInternalServerError)
		return
	}

	if providerData == "" {
		// This case means the preferred provider UUID is stale and points to a deleted provider.
		// I should probably delete the preferred provider key.
		p.prif.RT_Delete(preferredProviderKey)
		http.Error(w, "Preferred provider was set but not found, clearing setting.", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(providerData))
}
