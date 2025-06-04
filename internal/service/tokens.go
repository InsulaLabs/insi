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

// Returns the entity name (as encoded by user) and then the uuid generated unique to the key
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
		s.logger.Error("Could not unmarshal token data from FSM", "key", fsmStorageKey, "data", keyDataFromFsm, "error", err)
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

func (s *Service) CreateApiKey(keyName string) (string, error) {

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

func (s *Service) DeleteApiKey(key string) error {

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
