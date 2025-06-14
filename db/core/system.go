package core

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/db/models"
	"github.com/google/uuid"
)

// Raft limits on single writes "should" be enforced by us here to max 1MB for congestion
// If we re-think how we take data and sync in-addition to raft rather than ONLY with raft
// then we can remove this or otherwise adjust it
func sizeTooLargeForStorage(value string) bool {
	return len(value) >= 1024*1024
}

// used by all endpoints to redirect WRITE related operations to the leader
func (c *Core) redirectToLeader(w http.ResponseWriter, r *http.Request, originalPath string) {

	leaderConnectAddress, err := c.fsm.LeaderHTTPAddress()
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
	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
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

/*

	TODO:

	 Right now we base off of entity for storage but they might not be unique

	 We need to do UUID-focused storage for the api key -> token data mapping



*/

func (c *Core) spawnNewApiKey(keyName string) (string, error) {

	keyName = c.normalizeKeyName(keyName)
	keyUUID := uuid.New().String()

	// Use only the keyName (entity) for the FSM key, consistent with ValidateToken lookup
	apiKeyFsmStorageKey := fmt.Sprintf("%s:api:key:%s", c.cfg.RootPrefix, keyUUID)

	td := models.TokenData{
		DataScopeUUID: keyUUID,
		KeyUUID:       keyUUID,
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
		Key:   WithApiKeySubscribers(keyUUID),
		Value: "0",
	})
	return actualKey, nil
}

func (c *Core) deleteExistingApiKey(key string) error {

	// WARNING: This must only be called by the LEADER NODE
	if !c.fsm.IsLeader() {
		return fmt.Errorf("this operation must be performed by the leader node")
	}

	td, err := c.decomposeKey(key) // We only need the entity to form the FSM key
	if err != nil {
		return fmt.Errorf("could not decompose key: %w", err)
	}

	// The FSM key is based on the entity (key name)
	apiKeyFsmStorageKey := fmt.Sprintf("%s:api:key:%s", c.cfg.RootPrefix, td.KeyUUID)

	// TODO: Add the UUID to some preserved structure that we can have run delete iterations on
	// to clean out old keys in a non-demending way

	// Apply to FSM
	if err := c.fsm.Delete(apiKeyFsmStorageKey); err != nil {
		// Add FSM error handling if s.fsm.Delete can return an error
		c.logger.Error("Failed to delete API key from FSM", "key", apiKeyFsmStorageKey, "error", err)
		return fmt.Errorf("failed to delete API key from FSM for %s: %w", td.Entity, err)
	}

	c.fsm.Delete(WithApiKeyMemoryUsage(td.KeyUUID))
	c.fsm.Delete(WithApiKeyDiskUsage(td.KeyUUID))
	c.fsm.Delete(WithApiKeyEvents(td.KeyUUID))
	c.fsm.Delete(WithApiKeySubscribers(td.KeyUUID))

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

/*
// DO NOT DELETE THIS CODE
func (c *Core) compressFlate(data []byte) ([]byte, error) {
	var b bytes.Buffer
	fw, err := flate.NewWriter(&b, flate.BestCompression)
	if err != nil {
		return nil, err
	}
	_, err = fw.Write(data)
	if err != nil {
		return nil, err
	}
	err = fw.Close()
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// DO NOT DELETE THIS CODE
func (c *Core) decompressFlate(data []byte) (string, error) {
	b := bytes.NewReader(data)
	fr := flate.NewReader(b)
	defer fr.Close()
	var buf bytes.Buffer
	_, err := io.Copy(&buf, fr)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
*/

// This is still a system level operation, but it is used by all endpoints
// and is made public so it can be exposed to the plugin system for validation
// of tokens.
func (c *Core) ValidateToken(r *http.Request, mustBeRoot bool) (models.TokenData, bool) {

	authHeader := r.Header.Get("Authorization")
	const bearerPrefix = "Bearer "

	// Check if the header starts with "Bearer " and remove it
	token := authHeader
	if strings.HasPrefix(authHeader, bearerPrefix) {
		token = strings.TrimPrefix(authHeader, bearerPrefix)
	}

	if mustBeRoot {
		if token != c.authToken {
			return models.TokenData{
				Entity:        EntityRoot,
				DataScopeUUID: c.cfg.RootPrefix,
			}, false
		}
	}

	if token == c.authToken {
		return models.TokenData{
			Entity:        EntityRoot,
			DataScopeUUID: c.cfg.RootPrefix,
		}, true
	}

	apiCacheItem := c.apiCache.Get(token)
	if apiCacheItem != nil {
		return apiCacheItem.Value(), true
	}

	td, err := c.decomposeKey(token)
	if err != nil {
		c.logger.Error("Could not decompose key", "error", err)
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
