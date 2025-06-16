package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/InsulaLabs/insi/db/models"
)

var (
	ApiDefaultMaxMemoryUsage   = 1024 * 1024 * 250  // 250MB
	ApiDefaultMaxDiskUsage     = 1024 * 1024 * 1024 // 1GB
	ApiDefaultMaxEvents        = 1000
	ApiDefaultMaxSubscriptions = 100
)

const (
	ApiTrackMemoryPrefix        = "internal:api_key_memory_usage"
	ApiTrackDiskPrefix          = "internal:api_key_disk_usage"
	ApiTrackEventsPrefix        = "internal:api_key_events"
	ApiTrackSubscriptionsPrefix = "internal:api_key_subscriptions"

	// The set upper limit for the api key memory usage, disk usage, events, and subscriptions
	ApiTrackMaxMemoryUsagePrefix   = "internal:api_key_max_memory_usage"
	ApiTrackMaxDiskUsagePrefix     = "internal:api_key_max_disk_usage"
	ApiTrackMaxEventsPrefix        = "internal:api_key_max_events"
	ApiTrackMaxSubscriptionsPrefix = "internal:api_key_max_subscriptions"
)

func WithApiKeyMemoryUsage(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackMemoryPrefix, key)
}

func WithApiKeyDiskUsage(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackDiskPrefix, key)
}

func WithApiKeyEvents(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackEventsPrefix, key)
}

func WithApiKeySubscriptions(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackSubscriptionsPrefix, key)
}

func WithApiKeyMaxMemoryUsage(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackMaxMemoryUsagePrefix, key)
}

func WithApiKeyMaxDiskUsage(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackMaxDiskUsagePrefix, key)
}

func WithApiKeyMaxEvents(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackMaxEventsPrefix, key)
}

func WithApiKeyMaxSubscriptions(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackMaxSubscriptionsPrefix, key)
}

// CalculateDelta returns the delta between the old and new payloads.
// If the new payload is smaller, the delta will be negative.
// If the new payload is larger, the delta will be positive.
// If the new payload is the same, the delta will be 0.
func CalculateDelta(old models.KVPayload, new models.KVPayload) int {
	return new.TotalLength() - old.TotalLength()
}

// These functions are meant to be used with ValidateToken. they seem silly but it helps
// clarify the point of call when reasoning about the code

func RootOnly() AccessEntity {
	return AccessEntityRoot
}

func AnyUser() AccessEntity {
	return AccessEntityAnyUser
}

func (c *Core) apiKeyCreateHandler(w http.ResponseWriter, r *http.Request) {
	_, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for api key create request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.ApiKeyCreateRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		c.logger.Error("Could not unmarshal api key create request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	key, err := c.spawnNewApiKey(req.KeyName)
	if err != nil {
		c.logger.Error("Could not create api key", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	resp := models.ApiKeyCreateResponse{
		KeyName: req.KeyName,
		Key:     key,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		c.logger.Error("Failed to encode API key create response", "error", err)
	}
}

func (c *Core) apiKeyDeleteHandler(w http.ResponseWriter, r *http.Request) {
	_, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for api key delete request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.ApiKeyDeleteRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		c.logger.Error("Could not unmarshal api key delete request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	if err := c.deleteExistingApiKey(req.Key); err != nil {
		c.logger.Error("Could not delete api key", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"success": "true",
	})
}
