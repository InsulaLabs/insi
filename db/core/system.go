package core

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/db/models"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

type limiterType string

const (
	limiterTypeData   limiterType = "data"
	limiterTypeEvents limiterType = "events"
)

type routeClassification int

const (
	rcPublic routeClassification = iota
	rcPrivate
)

// used by all endpoints to redirect WRITE related operations to the leader
func (c *Core) redirectToLeader(w http.ResponseWriter, r *http.Request, originalPath string, routeClassification routeClassification) {

	leaderInfo, err := c.fsm.LeaderHTTPAddress()
	if err != nil {
		c.logger.Error(
			"Failed to get leader's connect address for redirection",
			"original_path", originalPath,
			"error", err,
		)
		http.Error(
			w,
			"Failed to determine cluster leader for redirection: "+err.Error(),
			http.StatusServiceUnavailable,
		)
		return
	}

	var leaderConnectAddress string

	switch routeClassification {
	case rcPublic:
		leaderConnectAddress = leaderInfo.PublicBinding

	case rcPrivate:
		leaderConnectAddress = leaderInfo.PrivateBinding
	}

	// parse the public and private bindings
	_, port, err := net.SplitHostPort(leaderConnectAddress)

	// determine the address to return based on the client domain
	// essentially, if a "domain" is provided, we use it to construct the
	// address as "domain:port" rather than "ip:port"
	if err == nil && leaderInfo.ClientDomain != "" {
		leaderConnectAddress = net.JoinHostPort(leaderInfo.ClientDomain, port)
	}

	redirectURL := "https://" + leaderConnectAddress + originalPath
	if r.URL.RawQuery != "" {
		redirectURL += "?" + r.URL.RawQuery
	}

	// DBG because this is a lot of noise
	c.logger.Debug("Issuing redirect to leader",
		"current_node_is_follower", true,
		"leader_connect_address_from_fsm", leaderConnectAddress,
		"final_redirect_url", redirectURL)

	// The client caller should follow this Location header
	// and make the request to the leader
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

func (c *Core) authedPing(w http.ResponseWriter, r *http.Request) {
	c.IndSystemOp()
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		c.logger.Warn("Token validation failed during ping", "remote_addr", r.RemoteAddr)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{
			"error_type": "AUTHENTICATION_FAILED",
			"message":    "Authentication failed. Invalid or missing API key.",
		})
		return
	}

	uptime := time.Since(c.startedAt).String()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":        "ok",
		"entity":        td.Entity,
		"node-badge-id": c.identity.GetID(),
		"leader":        c.fsm.Leader(),
		"uptime":        uptime,
	})
}

// -- SYSTEM OPERATIONS --

func (c *Core) joinHandler(w http.ResponseWriter, r *http.Request) {
	c.IndSystemOp()

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path, rcPrivate)
		return
	}

	// Only admin "root" key can tell nodes to join the cluster
	td, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// We already enforced that its root, but this will be a sanity check to ensure that
	// something isn't corrupted or otherwise malicious
	if td.Entity != EntityRoot {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	followerId := r.URL.Query().Get("followerId")
	followerAddr := r.URL.Query().Get("followerAddr")

	if followerId == "" || followerAddr == "" {
		http.Error(w, "Missing followerId or followerAddr parameters", http.StatusBadRequest)
		return
	}

	if err := c.fsm.Join(followerId, followerAddr); err != nil {
		c.logger.Error(
			"Failed to join follower",
			"followerId", followerId,
			"followerAddr", followerAddr,
			"error", err,
		)
		http.Error(
			w,
			fmt.Sprintf("Failed to join follower: %s", err),
			http.StatusInternalServerError,
		)
		return
	}
}

func (c *Core) normalizeKeyName(keyName string) string {

	keyName = strings.TrimSpace(keyName)
	keyName = strings.ToLower(keyName)
	for _, c := range []string{
		" ", "-", ".", ":", "/", "\\", "|", "`", "~", "!", "@",
		"#", "$", "%", "^", "&", "*", "(", ")", "[", "]", "{",
		"}", "=", "+", "?", "!", "@", "#", "$", "%", "^", "&",
	} {
		keyName = strings.ReplaceAll(keyName, c, "_")
	}

	return keyName
}

func (c *Core) validateTokenInner(token string) (models.TokenData, bool) {
	apiCacheItem := c.apiCache.Get(token)
	if apiCacheItem != nil {
		return apiCacheItem.Value(), true
	}

	td, err := c.decomposeKey(token)
	if err != nil {
		c.logger.Error("Could not decompose key", "error", err)
		return models.TokenData{}, false
	}

	// Ensure there's no tombstone (deleted key)
	tombstoneKey := WithApiKeyTombstone(td.KeyUUID)
	_, err = c.fsm.Get(tombstoneKey)
	if err == nil {
		c.logger.Error("Tombstone found for key (marked for deletion)", "key", td.KeyUUID)
		return models.TokenData{}, false
	}

	// Get the key from the fsm
	fsmStorageKey := fmt.Sprintf("%s:api:key:%s", c.cfg.RootPrefix, td.KeyUUID)
	keyDataFromFsm, err := c.fsm.Get(fsmStorageKey)
	if err != nil {
		c.logger.Error("Could not get key data from FSM", "key", fsmStorageKey, "error", err)
		return models.TokenData{}, false
	}

	// Data from FSM is plain JSON of models.TokenData, it should NOT be decrypted.
	// It should be directly unmarshalled.
	var tdFromFsm models.TokenData
	if err := json.Unmarshal([]byte(keyDataFromFsm), &tdFromFsm); err != nil {
		c.logger.Error(
			"Could not unmarshal token data from FSM",
			"key", fsmStorageKey,
			"data", keyDataFromFsm,
			"error", err,
		)
		return models.TokenData{}, false
	}

	// Compare the UUID from the decomposed token with the UUID stored in FSM for that entity.
	if tdFromFsm.DataScopeUUID != td.DataScopeUUID ||
		tdFromFsm.KeyUUID != td.KeyUUID {
		c.logger.Error("UUID mismatch between token and FSM record",
			"entity", td.Entity,
			"data_scope_uuid_from_token", td.DataScopeUUID,
			"key_uuid_from_token", td.KeyUUID,
			"entity_from_fsm", tdFromFsm.Entity,
			"data_scope_uuid_from_fsm", tdFromFsm.DataScopeUUID,
			"key_uuid_from_fsm", tdFromFsm.KeyUUID,
		)
		return models.TokenData{}, false
	}

	c.apiCache.Set(token, tdFromFsm, c.cfg.Cache.Keys)

	return tdFromFsm, true
}

func (c *Core) decomposeKey(token string) (models.TokenData, error) {

	parts := strings.TrimPrefix(token, "insi_")

	encryptedKeyData, err := base64.StdEncoding.DecodeString(parts)
	if err != nil {
		return models.TokenData{}, fmt.Errorf("could not decode base64: %w", err)
	}

	decryptedKeyData, err := c.decrypt([]byte(encryptedKeyData))
	if err != nil {
		return models.TokenData{}, fmt.Errorf("could not decrypt key data: %w", err)
	}

	var td models.TokenData
	if err := json.Unmarshal(decryptedKeyData, &td); err != nil {
		return models.TokenData{}, fmt.Errorf("could not unmarshal token data: %w", err)
	}

	return td, nil
}

func (c *Core) spawnNewApiKey(keyName string) (string, error) {

	keyName = c.normalizeKeyName(keyName)
	keyUUID := uuid.New().String()
	dsUUID := uuid.New().String()

	apiKeyFsmStorageKey := fmt.Sprintf("%s:api:key:%s", c.cfg.RootPrefix, keyUUID)

	td := models.TokenData{
		DataScopeUUID: dsUUID,
		KeyUUID:       keyUUID,
		IsAlias:       false,
	}

	// Data to be encrypted and base64 encoded for the actual API key string
	tokenDataForApiKeyString, err := json.Marshal(td)
	if err != nil {
		return "", fmt.Errorf("could not marshal token data for api key string: %w", err)
	}

	encryptedKeyDataForApiKey, err := c.encrypt(tokenDataForApiKeyString)
	if err != nil {
		return "", fmt.Errorf("could not encrypt token data for api key string: %w", err)
	}

	b64KeyData := base64.StdEncoding.EncodeToString(encryptedKeyDataForApiKey)
	actualKey := fmt.Sprintf("insi_%s", b64KeyData)

	// NOW we set the entity to the keyName so we dont encode it into the actual key
	td.Entity = keyName

	// Data to be stored in FSM (this is what ValidateToken retrieves and checks)
	// This should be the same TokenData structure.
	keyDataForFsm, err := json.Marshal(td)
	if err != nil {
		return "", fmt.Errorf("could not marshal token data for FSM storage: %w", err)
	}

	// Apply to FSM
	// The value stored in FSM is the JSON representation of TokenData (Entity and UUID)
	if err := c.fsm.Set(models.KVPayload{
		Key:   apiKeyFsmStorageKey,
		Value: string(keyDataForFsm),
	}); err != nil {
		// Add FSM error handling if s.fsm.Set can return an error that should be propagated
		c.logger.Error("Failed to set API key in FSM", "key", apiKeyFsmStorageKey, "error", err)
		return "", fmt.Errorf("failed to set API key in FSM for %s: %w", keyName, err)
	}

	c.fsm.Set(models.KVPayload{
		Key:   withApiKeyRef(keyUUID),
		Value: actualKey,
	})

	c.fsm.Set(models.KVPayload{
		Key:   withApiKeyDataScope(keyUUID),
		Value: dsUUID,
	})

	c.fsm.Set(models.KVPayload{
		Key:   WithApiKeyMemoryUsage(keyUUID),
		Value: "0",
	})
	c.fsm.Set(models.KVPayload{
		Key:   WithApiKeyDiskUsage(keyUUID),
		Value: "0",
	})
	c.fsm.Set(models.KVPayload{
		Key:   WithApiKeyEvents(keyUUID),
		Value: "0",
	})
	c.fsm.Set(models.KVPayload{
		Key:   WithApiKeySubscriptions(keyUUID),
		Value: "0",
	})
	c.fsm.Set(models.KVPayload{
		Key:   WithApiKeyMaxMemoryUsage(keyUUID),
		Value: fmt.Sprintf("%d", ApiDefaultMaxMemoryUsage),
	})
	c.fsm.Set(models.KVPayload{
		Key:   WithApiKeyMaxDiskUsage(keyUUID),
		Value: fmt.Sprintf("%d", ApiDefaultMaxDiskUsage),
	})
	c.fsm.Set(models.KVPayload{
		Key:   WithApiKeyMaxEvents(keyUUID),
		Value: fmt.Sprintf("%d", ApiDefaultMaxEvents),
	})
	c.fsm.Set(models.KVPayload{
		Key:   WithApiKeyMaxSubscriptions(keyUUID),
		Value: fmt.Sprintf("%d", ApiDefaultMaxSubscriptions),
	})
	c.fsm.Set(models.KVPayload{
		Key:   WithApiKeyRPSDataLimit(keyUUID),
		Value: fmt.Sprintf("%d", ApiDefaultRPSDataLimit),
	})
	c.fsm.Set(models.KVPayload{
		Key:   WithApiKeyRPSEventLimit(keyUUID),
		Value: fmt.Sprintf("%d", ApiDefaultRPSEventLimit),
	})

	return actualKey, nil
}

func (c *Core) spawnNewAliasKey(parentTd models.TokenData, aliasKeyName string) (string, error) {
	aliasKeyName = c.normalizeKeyName(aliasKeyName)
	aliasKeyUUID := uuid.New().String()

	// Inherit DataScopeUUID from parent, and mark as an alias.
	td := models.TokenData{
		DataScopeUUID: parentTd.DataScopeUUID,
		KeyUUID:       aliasKeyUUID,
		IsAlias:       true,
	}

	// Data to be encrypted and base64 encoded for the actual API key string
	tokenDataForApiKeyString, err := json.Marshal(td)
	if err != nil {
		return "", fmt.Errorf("could not marshal token data for alias api key string: %w", err)
	}

	encryptedKeyDataForApiKey, err := c.encrypt(tokenDataForApiKeyString)
	if err != nil {
		return "", fmt.Errorf("could not encrypt token data for alias api key string: %w", err)
	}

	b64KeyData := base64.StdEncoding.EncodeToString(encryptedKeyDataForApiKey)
	actualKey := fmt.Sprintf("insi_%s", b64KeyData)

	// NOW we set the entity to the keyName so we dont encode it into the actual key
	td.Entity = aliasKeyName

	keyDataForFsm, err := json.Marshal(td)
	if err != nil {
		return "", fmt.Errorf("could not marshal token data for FSM storage: %w", err)
	}

	apiKeyFsmStorageKey := fmt.Sprintf("%s:api:key:%s", c.cfg.RootPrefix, aliasKeyUUID)

	// Apply to FSM
	if err := c.fsm.Set(models.KVPayload{
		Key:   apiKeyFsmStorageKey,
		Value: string(keyDataForFsm),
	}); err != nil {
		c.logger.Error("Failed to set alias API key in FSM", "key", apiKeyFsmStorageKey, "error", err)
		return "", fmt.Errorf("failed to set alias API key in FSM for %s: %w", aliasKeyName, err)
	}

	return actualKey, nil
}

func (c *Core) deleteExistingApiKey(key string) error {

	c.IndSystemOp()

	if !c.fsm.IsLeader() {
		return fmt.Errorf("this operation must be performed by the leader node")
	}

	td, err := c.decomposeKey(key)
	if err != nil {
		return fmt.Errorf("could not decompose key: %w", err)
	}

	apiKeyFsmStorageKey := fmt.Sprintf("%s:api:key:%s", c.cfg.RootPrefix, td.KeyUUID)

	// Confirm its real and had a value
	apiKeyFsmStorageValue, err := c.fsm.Get(apiKeyFsmStorageKey)
	if err != nil {
		return fmt.Errorf("could not get API key from FSM: %w", err)
	}
	if apiKeyFsmStorageValue == "" {

		// If the key is not found, it means it was already deleted
		return nil
	}

	if err := c.fsm.Set(models.KVPayload{
		Key:   WithApiKeyTombstone(td.KeyUUID),
		Value: td.DataScopeUUID,
	}); err != nil {
		return fmt.Errorf("could not set tombstone for api key: %w", err)
	}

	c.apiCache.Delete(key)

	return nil
}

func (c *Core) deleteApiKeyDirectly(key string) error {
	td, err := c.decomposeKey(key)
	if err != nil {
		return fmt.Errorf("could not decompose key for direct deletion: %w", err)
	}

	apiKeyFsmStorageKey := fmt.Sprintf("%s:api:key:%s", c.cfg.RootPrefix, td.KeyUUID)

	if err := c.fsm.Delete(apiKeyFsmStorageKey); err != nil {
		c.logger.Error("Could not delete api key from fsm directly", "key", apiKeyFsmStorageKey, "error", err)
		return fmt.Errorf("could not delete api key from fsm directly: %w", err)
	}

	c.apiCache.Delete(key)
	return nil
}

func (c *Core) encrypt(data []byte) ([]byte, error) {
	hash := sha256.Sum256([]byte(c.cfg.InstanceSecret))
	aesKey := hash[:]

	blockCipher, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(blockCipher)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

func (c *Core) decrypt(data []byte) ([]byte, error) {
	// Derive a 32-byte key using SHA-256
	hash := sha256.Sum256([]byte(c.cfg.InstanceSecret))
	aesKey := hash[:]

	blockCipher, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(blockCipher)
	if err != nil {
		return nil, err
	}
	nonce, ciphertext := data[:gcm.NonceSize()], data[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}

func (c *Core) CheckRateLimit(w http.ResponseWriter, r *http.Request, keyUUID string, lType limiterType) bool {

	// Root key has no rate limits
	if keyUUID == c.cfg.RootPrefix {
		return true
	}

	item := c.entityRateLimiters.Get(keyUUID)
	if item == nil {
		rpse, err := c.fsm.Get(WithApiKeyRPSEventLimit(keyUUID))
		if err != nil {
			c.logger.Error("Could not get RPS event limit", "error", err)
			return false
		}

		rpsd, err := c.fsm.Get(WithApiKeyRPSDataLimit(keyUUID))
		if err != nil {
			c.logger.Error("Could not get RPS data limit", "error", err)
			return false
		}

		rpseInt, err := strconv.ParseInt(rpse, 10, 64)
		if err != nil {
			c.logger.Error("Could not parse RPS event limit", "error", err)
			return false
		}

		rpsdInt, err := strconv.ParseInt(rpsd, 10, 64)
		if err != nil {
			c.logger.Error("Could not parse RPS data limit", "error", err)
			return false
		}

		limiter := &endpointKeyRateLimiters{
			events: rate.NewLimiter(rate.Limit(rpseInt), int(rpseInt)),
			data:   rate.NewLimiter(rate.Limit(rpsdInt), int(rpsdInt)),
		}
		c.entityRateLimiters.Set(keyUUID, limiter, time.Minute*1)
		// The first request is always allowed to create the limiter. Subsequent requests will be checked.
		return true
	}

	limiter := item.Value()
	var rateLimiter *rate.Limiter
	var isAllowed bool

	switch lType {
	case limiterTypeData:
		rateLimiter = limiter.data
	case limiterTypeEvents:
		rateLimiter = limiter.events
	default:
		c.logger.Error("Invalid limiter type specified", "type", lType)
		return false
	}

	isAllowed = rateLimiter.Allow()

	if !isAllowed {
		retryAfter := rateLimiter.Reserve().Delay()
		w.Header().Set("Retry-After", fmt.Sprintf("%.0f", retryAfter.Seconds()))
		http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
		return false
	}

	return true
}

// This is still a system level operation, but it is used by all endpoints
// and is made public so it can be exposed to the plugin system for validation
// of tokens.
func (c *Core) ValidateToken(r *http.Request, rootOnly AccessEntity) (models.TokenData, bool) {

	authHeader := r.Header.Get("Authorization")
	const bearerPrefix = "Bearer "

	// Check if the header starts with "Bearer " and remove it
	token := authHeader
	if strings.HasPrefix(authHeader, bearerPrefix) {
		token = strings.TrimPrefix(authHeader, bearerPrefix)
	}

	if rootOnly {
		if token != c.authToken {
			return models.TokenData{
				Entity:        EntityRoot,
				DataScopeUUID: c.cfg.RootPrefix,
				KeyUUID:       c.cfg.RootPrefix,
			}, false
		}
	}

	if token == c.authToken {
		return models.TokenData{
			Entity:        EntityRoot,
			DataScopeUUID: c.cfg.RootPrefix,
			KeyUUID:       c.cfg.RootPrefix,
		}, true
	}

	// Attempt to validate the token directly
	if td, ok := c.validateTokenInner(token); ok {
		// This is the primary path for both root and regular keys.
		// If it's an alias, we need to fetch the root key's data scope.
		if td.IsAlias {
			rootKeyUUID, err := c.fsm.Get(WithAliasToRoot(token))
			if err != nil {
				// This case should be rare, implies an orphaned alias key.
				c.logger.Error("Could not find root key for an alias token", "alias_key_uuid", td.KeyUUID, "error", err)
				return models.TokenData{}, false
			}

			// Fetch the root key's full data from FSM.
			rootFsmStorageKey := fmt.Sprintf("%s:api:key:%s", c.cfg.RootPrefix, rootKeyUUID)
			rootKeyDataFromFsm, err := c.fsm.Get(rootFsmStorageKey)
			if err != nil {
				c.logger.Error("Could not get root key data for alias from FSM", "root_key_uuid", rootKeyUUID, "error", err)
				return models.TokenData{}, false
			}

			var rootTdFromFsm models.TokenData
			if err := json.Unmarshal([]byte(rootKeyDataFromFsm), &rootTdFromFsm); err != nil {
				c.logger.Error("Could not unmarshal root key token data from FSM for alias", "root_key_uuid", rootKeyUUID, "error", err)
				return models.TokenData{}, false
			}

			// Construct the final token data for the request context:
			// Use the root's DataScopeUUID for access control.
			// Keep the alias's other details (Entity, KeyUUID, IsAlias flag).
			finalTd := rootTdFromFsm
			finalTd.Entity = td.Entity // Keep alias entity name
			finalTd.IsAlias = true     // Mark that auth was via an alias

			c.apiCache.Set(token, finalTd, c.cfg.Cache.Keys)
			return finalTd, true
		}
		return td, true
	}

	// If direct validation fails, something is wrong, as validateTokenInner should handle all valid keys (aliased or not).
	// The logic below this point is effectively a fallback and should ideally not be hit.
	c.logger.Warn("Token validation failed initial check, which should not happen for valid keys.", "token_prefix", strings.Split(token, "_")[0])
	return models.TokenData{}, false
}

// Allow any user to get their limits and current usage
func (c *Core) callerLimitsHandler(w http.ResponseWriter, r *http.Request) {

	c.IndSystemOp()

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Get the limit for disk usage
	diskLimitForKey, err := c.fsm.Get(WithApiKeyMaxDiskUsage(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get limit for disk usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	diskLimitForKeyInt, err := strconv.ParseInt(diskLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for disk usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	memLimitForKey, err := c.fsm.Get(WithApiKeyMaxMemoryUsage(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get limit for memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	memLimitForKeyInt, err := strconv.ParseInt(memLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	eventsLimitForKey, err := c.fsm.Get(WithApiKeyMaxEvents(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get limit for events usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	eventsLimitForKeyInt, err := strconv.ParseInt(eventsLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for events usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	subscribersLimitForKey, err := c.fsm.Get(WithApiKeyMaxSubscriptions(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get limit for subscribers usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	subscribersLimitForKeyInt, err := strconv.ParseInt(subscribersLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for subscribers usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get the current usage for disk
	diskUsage, err := c.fsm.Get(WithApiKeyDiskUsage(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current disk usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	diskUsageInt, err := strconv.ParseInt(diskUsage, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current disk usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get the current usage for memory
	memUsage, err := c.fsm.Get(WithApiKeyMemoryUsage(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	memUsageInt, err := strconv.ParseInt(memUsage, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get the current usage for events
	eventsUsage, err := c.fsm.Get(WithApiKeyEvents(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current events usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	eventsUsageInt, err := strconv.ParseInt(eventsUsage, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current events usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get the current usage for subscribers
	subscribersUsage, err := c.fsm.Get(WithApiKeySubscriptions(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current subscribers usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	subscribersUsageInt, err := strconv.ParseInt(subscribersUsage, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current subscribers usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	rpsDataLimitForKey, err := c.fsm.Get(WithApiKeyRPSDataLimit(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current RPS data limit", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	rpsDataLimitForKeyInt, err := strconv.ParseInt(rpsDataLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current RPS data limit", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	rpsEventLimitForKey, err := c.fsm.Get(WithApiKeyRPSEventLimit(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current RPS event limit", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	rpsEventLimitForKeyInt, err := strconv.ParseInt(rpsEventLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current RPS event limit", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	limitsResponse := models.LimitsResponse{
		MaxLimits: &models.Limits{
			BytesOnDisk:   &diskLimitForKeyInt,
			BytesInMemory: &memLimitForKeyInt,
			EventsEmitted: &eventsLimitForKeyInt,
			Subscribers:   &subscribersLimitForKeyInt,
			RPSDataLimit:  &rpsDataLimitForKeyInt,
			RPSEventLimit: &rpsEventLimitForKeyInt,
		},
		CurrentUsage: &models.Limits{
			BytesOnDisk:   &diskUsageInt,
			BytesInMemory: &memUsageInt,
			EventsEmitted: &eventsUsageInt,
			Subscribers:   &subscribersUsageInt,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(limitsResponse)
}

func (c *Core) setLimitsHandler(w http.ResponseWriter, r *http.Request) {

	c.IndSystemOp()

	tdr, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path, rcPrivate)
		return
	}

	// Get the limits from the request body
	var req models.SetLimitsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.ApiKey == "" {
		http.Error(w, "api_key is missing from request", http.StatusBadRequest)
		return
	}

	if req.Limits == nil {
		http.Error(w, "limits object is missing from request", http.StatusBadRequest)
		return
	}

	// If the target key is root key it must be the root who asks for it
	// and not a user. we distinguish as root key construct is different from
	// user key construct.
	var target models.TokenData
	if req.ApiKey == c.authToken {
		if !c.tdIsRoot(tdr) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		target = tdr
	} else {
		var err error
		target, err = c.decomposeKey(req.ApiKey)
		if err != nil {
			http.Error(w, "Invalid target key", http.StatusBadRequest)
			return
		}
	}

	// Set the limits in the FSM
	if req.Limits.BytesOnDisk != nil {
		if err := c.fsm.Set(models.KVPayload{
			Key:   WithApiKeyMaxDiskUsage(target.DataScopeUUID),
			Value: fmt.Sprintf("%d", *req.Limits.BytesOnDisk),
		}); err != nil {
			c.logger.Error("failed to set disk limit", "error", err)
			http.Error(w, "failed to set disk limit", http.StatusInternalServerError)
			return
		}
	}
	if req.Limits.BytesInMemory != nil {
		if err := c.fsm.Set(models.KVPayload{
			Key:   WithApiKeyMaxMemoryUsage(target.DataScopeUUID),
			Value: fmt.Sprintf("%d", *req.Limits.BytesInMemory),
		}); err != nil {
			c.logger.Error("failed to set memory limit", "error", err)
			http.Error(w, "failed to set memory limit", http.StatusInternalServerError)
			return
		}
	}
	if req.Limits.EventsEmitted != nil {
		if err := c.fsm.Set(models.KVPayload{
			Key:   WithApiKeyMaxEvents(target.DataScopeUUID),
			Value: fmt.Sprintf("%d", *req.Limits.EventsEmitted),
		}); err != nil {
			c.logger.Error("failed to set events limit", "error", err)
			http.Error(w, "failed to set events limit", http.StatusInternalServerError)
			return
		}
	}
	if req.Limits.Subscribers != nil {
		if err := c.fsm.Set(models.KVPayload{
			Key:   WithApiKeyMaxSubscriptions(target.DataScopeUUID),
			Value: fmt.Sprintf("%d", *req.Limits.Subscribers),
		}); err != nil {
			c.logger.Error("failed to set subscribers limit", "error", err)
			http.Error(w, "failed to set subscribers limit", http.StatusInternalServerError)
			return
		}
	}

	if req.Limits.RPSDataLimit != nil {
		if err := c.fsm.Set(models.KVPayload{
			Key:   WithApiKeyRPSDataLimit(target.DataScopeUUID),
			Value: fmt.Sprintf("%d", *req.Limits.RPSDataLimit),
		}); err != nil {
			c.logger.Error("failed to set RPS data limit", "error", err)
			http.Error(w, "failed to set RPS data limit", http.StatusInternalServerError)
			return
		}
	}
	if req.Limits.RPSEventLimit != nil {
		if err := c.fsm.Set(models.KVPayload{
			Key:   WithApiKeyRPSEventLimit(target.DataScopeUUID),
			Value: fmt.Sprintf("%d", *req.Limits.RPSEventLimit),
		}); err != nil {
			c.logger.Error("failed to set RPS event limit", "error", err)
			http.Error(w, "failed to set RPS event limit", http.StatusInternalServerError)
			return
		}
	}

	if req.Limits.RPSDataLimit != nil || req.Limits.RPSEventLimit != nil {
		c.entityRateLimiters.Delete(target.DataScopeUUID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// Allow any user to get their limits and current usage
func (c *Core) specificLimitsHandler(w http.ResponseWriter, r *http.Request) {

	c.IndSystemOp()

	// ONLY ROOT CAN GET LIMITS FOR SPECIFIC KEYS
	_, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req models.GetLimitsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// get the targeted key
	td, err := c.decomposeKey(req.ApiKey)
	if err != nil {
		http.Error(w, "Invalid target key format", http.StatusBadRequest)
		return
	}

	// Ensure theres no tombstone (deleted key)
	tombstoneKey := WithApiKeyTombstone(td.KeyUUID)
	_, err = c.fsm.Get(tombstoneKey)
	if err == nil {
		c.logger.Error("Tombstone found for key (marked for deletion)", "key", td.KeyUUID)

		// We return the same as if the key did not exist to prevent
		// leaking information about the key to the caller
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error_type": "API_KEY_NOT_FOUND",
			"message":    "The specified API key does not exist or has been deleted.",
		})
		return
	}

	// Check if the key exists before proceeding.
	fsmStorageKey := fmt.Sprintf("%s:api:key:%s", c.cfg.RootPrefix, td.KeyUUID)
	if _, err := c.fsm.Get(fsmStorageKey); err != nil {
		c.logger.Warn("Attempt to get limits for non-existent API key", "key_uuid", td.KeyUUID, "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error_type": "API_KEY_NOT_FOUND",
			"message":    "The specified API key does not exist or has been deleted.",
		})
		return
	}

	// Get the limit for disk usage
	diskLimitForKey, err := c.fsm.Get(WithApiKeyMaxDiskUsage(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get limit for disk usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	diskLimitForKeyInt, err := strconv.ParseInt(diskLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for disk usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	memLimitForKey, err := c.fsm.Get(WithApiKeyMaxMemoryUsage(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get limit for memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	memLimitForKeyInt, err := strconv.ParseInt(memLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	eventsLimitForKey, err := c.fsm.Get(WithApiKeyMaxEvents(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get limit for events usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	eventsLimitForKeyInt, err := strconv.ParseInt(eventsLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for events usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	subscribersLimitForKey, err := c.fsm.Get(WithApiKeyMaxSubscriptions(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get limit for subscribers usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	subscribersLimitForKeyInt, err := strconv.ParseInt(subscribersLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for subscribers usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get the current usage for disk
	diskUsage, err := c.fsm.Get(WithApiKeyDiskUsage(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current disk usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	diskUsageInt, err := strconv.ParseInt(diskUsage, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current disk usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get the current usage for memory
	memUsage, err := c.fsm.Get(WithApiKeyMemoryUsage(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	memUsageInt, err := strconv.ParseInt(memUsage, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get the current usage for events
	eventsUsage, err := c.fsm.Get(WithApiKeyEvents(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current events usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	eventsUsageInt, err := strconv.ParseInt(eventsUsage, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current events usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get the current usage for subscribers
	subscribersUsage, err := c.fsm.Get(WithApiKeySubscriptions(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current subscribers usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	subscribersUsageInt, err := strconv.ParseInt(subscribersUsage, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current subscribers usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Get the KEYUUID SET VALUE FROM ABOVE
	rpsDataLimitForKey, err := c.fsm.Get(WithApiKeyRPSDataLimit(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current RPS data limit", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	rpsDataLimitForKeyInt, err := strconv.ParseInt(rpsDataLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current RPS data limit", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	rpsEventLimitForKey, err := c.fsm.Get(WithApiKeyRPSEventLimit(td.DataScopeUUID))
	if err != nil {
		c.logger.Error("Could not get current RPS event limit", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	rpsEventLimitForKeyInt, err := strconv.ParseInt(rpsEventLimitForKey, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current RPS event limit", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	limitsResponse := models.LimitsResponse{
		MaxLimits: &models.Limits{
			BytesOnDisk:   &diskLimitForKeyInt,
			BytesInMemory: &memLimitForKeyInt,
			EventsEmitted: &eventsLimitForKeyInt,
			Subscribers:   &subscribersLimitForKeyInt,
			RPSDataLimit:  &rpsDataLimitForKeyInt,
			RPSEventLimit: &rpsEventLimitForKeyInt,
		},
		CurrentUsage: &models.Limits{
			BytesOnDisk:   &diskUsageInt,
			BytesInMemory: &memUsageInt,
			EventsEmitted: &eventsUsageInt,
			Subscribers:   &subscribersUsageInt,
			// NOTE: Don't load current rps here, it's not correct
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(limitsResponse)
}
