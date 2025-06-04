package service

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

	"github.com/InsulaLabs/insi/models"
	"github.com/google/uuid"
)

// Raft limits on single writes "should" be enforced by us here to max 1MB for congestion
// If we re-think how we take data and sync in-addition to raft rather than ONLY with raft
// then we can remove this or otherwise adjust it
func sizeTooLargeForStorage(value string) bool {
	return len(value) >= 1024*1024
}

// used by all endpoints to redirect WRITE related operations to the leader
func (s *Service) redirectToLeader(w http.ResponseWriter, r *http.Request, originalPath string) {

	leaderConnectAddress, err := s.fsm.LeaderHTTPAddress()
	if err != nil {
		s.logger.Error(
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
	s.logger.Debug("Issuing redirect to leader",
		"current_node_is_follower", true,
		"leader_connect_address_from_fsm", leaderConnectAddress,
		"final_redirect_url", redirectURL)

	// The client caller should follow this Location header
	// and make the request to the leader
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

func (s *Service) authedPing(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		s.logger.Warn("Token validation failed during ping", "remote_addr", r.RemoteAddr)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{
			"error_type": "AUTHENTICATION_FAILED",
			"message":    "Authentication failed. Invalid or missing API key.",
		})
		return
	}

	uptime := time.Since(s.startedAt).String()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":        "ok",
		"entity":        td.Entity,
		"node-badge-id": s.identity.GetID(),
		"leader":        s.fsm.Leader(),
		"uptime":        uptime,
	})
}

// -- SYSTEM OPERATIONS --

func (s *Service) joinHandler(w http.ResponseWriter, r *http.Request) {
	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	// Only admin "root" key can tell nodes to join the cluster
	td, ok := s.ValidateToken(r, true)
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

	if err := s.fsm.Join(followerId, followerAddr); err != nil {
		s.logger.Error(
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

func (s *Service) normalizeKeyName(keyName string) string {

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

func (s *Service) decomposeKey(token string) (string, string, error) {

	parts := strings.TrimPrefix(token, "insi_")

	encryptedKeyData, err := base64.StdEncoding.DecodeString(parts)
	if err != nil {
		return "", "", fmt.Errorf("could not decode base64: %w", err)
	}

	decryptedKeyData, err := s.decrypt(encryptedKeyData)
	if err != nil {
		return "", "", fmt.Errorf("could not decrypt key data: %w", err)
	}

	var td models.TokenData
	if err := json.Unmarshal(decryptedKeyData, &td); err != nil {
		return "", "", fmt.Errorf("could not unmarshal token data: %w", err)
	}

	return td.Entity, td.UUID, nil
}

func (s *Service) spawnNewApiKey(keyName string) (string, error) {

	keyName = s.normalizeKeyName(keyName)
	keyUUID := uuid.New().String()

	// Use only the keyName (entity) for the FSM key, consistent with ValidateToken lookup
	apiKeyFsmStorageKey := fmt.Sprintf("%s:api:key:%s", s.cfg.RootPrefix, keyName)

	td := models.TokenData{
		Entity: keyName,
		UUID:   keyUUID,
	}

	// Data to be encrypted and base64 encoded for the actual API key string
	tokenDataForApiKeyString, err := json.Marshal(td)
	if err != nil {
		return "", fmt.Errorf("could not marshal token data for api key string: %w", err)
	}

	encryptedKeyDataForApiKey, err := s.encrypt(tokenDataForApiKeyString)
	if err != nil {
		return "", fmt.Errorf("could not encrypt token data for api key string: %w", err)
	}
	b64KeyData := base64.StdEncoding.EncodeToString(encryptedKeyDataForApiKey)
	actualKey := fmt.Sprintf("insi_%s", b64KeyData)

	// Data to be stored in FSM (this is what ValidateToken retrieves and checks)
	// This should be the same TokenData structure.
	keyDataForFsm, err := json.Marshal(td)
	if err != nil {
		return "", fmt.Errorf("could not marshal token data for FSM storage: %w", err)
	}

	// Apply to FSM
	// The value stored in FSM is the JSON representation of TokenData (Entity and UUID)
	if err := s.fsm.Set(models.KVPayload{
		Key:   apiKeyFsmStorageKey,
		Value: string(keyDataForFsm),
	}); err != nil {
		// Add FSM error handling if s.fsm.Set can return an error that should be propagated
		s.logger.Error("Failed to set API key in FSM", "key", apiKeyFsmStorageKey, "error", err)
		return "", fmt.Errorf("failed to set API key in FSM for %s: %w", keyName, err)
	}

	return actualKey, nil
}

func (s *Service) deleteExistingApiKey(key string) error {

	entity, _, err := s.decomposeKey(key) // We only need the entity to form the FSM key
	if err != nil {
		return fmt.Errorf("could not decompose key: %w", err)
	}

	// The FSM key is based on the entity (key name)
	apiKeyFsmStorageKey := fmt.Sprintf("%s:api:key:%s", s.cfg.RootPrefix, entity)

	// Apply to FSM
	if err := s.fsm.Delete(apiKeyFsmStorageKey); err != nil {
		// Add FSM error handling if s.fsm.Delete can return an error
		s.logger.Error("Failed to delete API key from FSM", "key", apiKeyFsmStorageKey, "error", err)
		return fmt.Errorf("failed to delete API key from FSM for %s: %w", entity, err)
	}

	s.apiCache.Delete(key)

	return nil
}

func (s *Service) encrypt(data []byte) ([]byte, error) {
	hash := sha256.Sum256([]byte(s.cfg.InstanceSecret))
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

func (s *Service) decrypt(data []byte) ([]byte, error) {
	// Derive a 32-byte key using SHA-256
	hash := sha256.Sum256([]byte(s.cfg.InstanceSecret))
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

// This is still a system level operation, but it is used by all endpoints
// and is made public so it can be exposed to the plugin system for validation
// of tokens.
func (s *Service) ValidateToken(r *http.Request, mustBeRoot bool) (models.TokenData, bool) {

	authHeader := r.Header.Get("Authorization")
	const bearerPrefix = "Bearer "

	// Check if the header starts with "Bearer " and remove it
	token := authHeader
	if strings.HasPrefix(authHeader, bearerPrefix) {
		token = strings.TrimPrefix(authHeader, bearerPrefix)
	}

	if mustBeRoot {
		if token != s.authToken {
			return models.TokenData{
				Entity: EntityRoot,
				UUID:   s.cfg.RootPrefix,
			}, false
		}
	}

	if token == s.authToken {
		return models.TokenData{
			Entity: EntityRoot,
			UUID:   s.cfg.RootPrefix,
		}, true
	}

	apiCacheItem := s.apiCache.Get(token)
	if apiCacheItem != nil {
		return apiCacheItem.Value(), true
	}

	entity, uuid, err := s.decomposeKey(token)
	if err != nil {
		s.logger.Error("Could not decompose key", "error", err)
		return models.TokenData{}, false
	}

	// Get the key from the fsm
	fsmStorageKey := fmt.Sprintf("%s:api:key:%s", s.cfg.RootPrefix, entity)
	keyDataFromFsm, err := s.fsm.Get(fsmStorageKey)
	if err != nil {
		s.logger.Error("Could not get key data from FSM", "key", fsmStorageKey, "error", err)
		return models.TokenData{}, false
	}

	// Data from FSM is plain JSON of models.TokenData, it should NOT be decrypted.
	// It should be directly unmarshalled.
	var tdFromFsm models.TokenData
	if err := json.Unmarshal([]byte(keyDataFromFsm), &tdFromFsm); err != nil {
		s.logger.Error(
			"Could not unmarshal token data from FSM",
			"key", fsmStorageKey,
			"data", keyDataFromFsm,
			"error", err,
		)
		return models.TokenData{}, false
	}

	// Compare the UUID from the decomposed token with the UUID stored in FSM for that entity.
	if tdFromFsm.UUID != uuid || tdFromFsm.Entity != entity {
		s.logger.Error("UUID mismatch between token and FSM record",
			"entity", entity,
			"uuid_from_token", uuid,
			"uuid_from_fsm", tdFromFsm.UUID)
		return models.TokenData{}, false
	}

	s.apiCache.Set(token, tdFromFsm, time.Second*10)

	return tdFromFsm, true
}
