package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

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

	ApiTrackEventLastResetPrefix = "internal:api_key_event_last_reset"

	// Tombstone is used to mark an api key as deleted with the data scope uuid given so an automated
	// runner can clean up the key from the system
	// When the runner (on leader node only) runs it will iterate over all tombstone prefixe
	//               <ApiTombstonePrefix[:DELETED_KEY_UUID]>   -> DELETED_KEY_DATA_SCOPE_UUID
	//   Then it can iteratively delete all keys with that data scope uuid. Once complete, it can remove
	// the tombstone and have all the knowledge required to do so the moment it is needed
	ApiTombstonePrefix = "internal:api_key_tombstone"

	// Alias prefixes
	ApiAliasToRootPrefix = "internal:alias_to_root" // a string holding the alias to the root key uuid
	ApiRootToAliasPrefix = "internal:root_to_alias" // a string holding the root key uuid to the alias
)

func WithAliasToRoot(alias string) string {
	return fmt.Sprintf("%s:%s", ApiAliasToRootPrefix, alias)
}

func WithRootToAlias(rootKeyUUID, alias string) string {
	return fmt.Sprintf("%s:%s:%s", ApiRootToAliasPrefix, rootKeyUUID, alias)
}

func WithRootToAliasPrefix(rootKeyUUID string) string {
	return fmt.Sprintf("%s:%s", ApiRootToAliasPrefix, rootKeyUUID)
}

func WithApiKeyMemoryUsage(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackMemoryPrefix, key)
}

func WithApiKeyDiskUsage(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackDiskPrefix, key)
}

func WithApiKeyEventLastReset(key string) string {
	return fmt.Sprintf("%s:%s", ApiTrackEventLastResetPrefix, key)
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

func WithApiKeyTombstone(key string) string {
	return fmt.Sprintf("%s:%s", ApiTombstonePrefix, key)
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
		c.redirectToLeader(w, r, r.URL.Path, rcPrivate)
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
		c.redirectToLeader(w, r, r.URL.Path, rcPrivate)
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

const MaxAliasesPerKey = 16

func (c *Core) setAliasHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if c.tdIsRoot(td) {
		http.Error(w, "Cannot set alias for root key", http.StatusBadRequest)
		return
	}

	if td.IsAlias {
		http.Error(w, "Cannot create an alias from an alias key", http.StatusBadRequest)
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path, rcPublic)
		return
	}

	// Check alias count
	prefix := WithRootToAliasPrefix(td.KeyUUID)
	aliases, err := c.fsm.Iterate(prefix, 0, MaxAliasesPerKey+1)
	if err != nil {
		c.logger.Error("Could not iterate aliases", "error", err)
		http.Error(w, "Could not check alias count", http.StatusInternalServerError)
		return
	}

	if len(aliases) >= MaxAliasesPerKey {
		http.Error(w, fmt.Sprintf("Alias limit of %d reached", MaxAliasesPerKey), http.StatusBadRequest)
		return
	}

	// Generate a new key that will serve as the alias
	// The entity name for an alias key doesn't matter as much, but this is descriptive.
	aliasKey, err := c.spawnNewAliasKey(td, fmt.Sprintf("alias_for_%s", td.Entity))
	if err != nil {
		c.logger.Error("Could not create api key for alias", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Store the mappings
	if err := c.fsm.Set(models.KVPayload{Key: WithAliasToRoot(aliasKey), Value: td.KeyUUID}); err != nil {
		c.logger.Error("Could not set alias to root mapping", "error", err)
		http.Error(w, "Could not set alias", http.StatusInternalServerError)
		return
	}
	if err := c.fsm.Set(models.KVPayload{Key: WithRootToAlias(td.KeyUUID, aliasKey), Value: "1"}); err != nil {
		// Attempt to clean up the other key if this fails
		c.fsm.Delete(WithAliasToRoot(aliasKey))
		c.logger.Error("Could not set root to alias mapping", "error", err)
		http.Error(w, "Could not set alias", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(models.SetAliasResponse{Alias: aliasKey})
}

func (c *Core) deleteAliasHandler(w http.ResponseWriter, r *http.Request) {
	// Authenticate with the root key of the alias, not the alias itself.
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path, rcPublic)
		return
	}

	var req models.DeleteAliasRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Verify the alias belongs to this token
	rootKeyUUID, err := c.fsm.Get(WithAliasToRoot(req.Alias))
	if err != nil {
		http.Error(w, "Alias not found", http.StatusNotFound)
		return
	}

	if rootKeyUUID != td.KeyUUID {
		http.Error(w, "Forbidden: alias does not belong to the authenticated key", http.StatusForbidden)
		return
	}

	// Delete the actual API key created for the alias
	if err := c.deleteExistingApiKey(req.Alias); err != nil {
		c.logger.Error("Could not delete underlying api key for alias", "alias", req.Alias, "error", err)
		// Continue, try to delete the mappings anyway
	}

	// Delete mappings
	if err := c.fsm.Delete(WithAliasToRoot(req.Alias)); err != nil {
		c.logger.Error("Could not delete alias to root mapping", "alias", req.Alias, "error", err)
		http.Error(w, "Could not delete alias", http.StatusInternalServerError)
		return
	}
	if err := c.fsm.Delete(WithRootToAlias(td.KeyUUID, req.Alias)); err != nil {
		c.logger.Error("Could not delete root to alias mapping", "alias", req.Alias, "error", err)
		http.Error(w, "Could not delete alias", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (c *Core) listAliasesHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	prefix := WithRootToAliasPrefix(td.KeyUUID)
	aliases, err := c.fsm.Iterate(prefix, 0, MaxAliasesPerKey)
	if err != nil {
		c.logger.Error("Could not iterate aliases for listing", "error", err)
		http.Error(w, "Could not list aliases", http.StatusInternalServerError)
		return
	}

	aliasKeys := make([]string, len(aliases))
	for i, aliasBytes := range aliases {
		fullKey := string(aliasBytes)
		aliasKeys[i] = strings.TrimPrefix(fullKey, prefix+":")
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(models.ListAliasesResponse{Aliases: aliasKeys})
}
