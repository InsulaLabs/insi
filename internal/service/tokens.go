package service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

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

	entity, uuid, err := s.decomposeKey(token)
	if err != nil {
		s.logger.Error("Could not decompose key", "error", err)
		return models.TokenData{}, false
	}

	// Get the key from the fsm
	keyData, err := s.fsm.Get(fmt.Sprintf("%s:api:key:%s", s.cfg.RootPrefix, entity))
	if err != nil {
		s.logger.Error("Could not get key data", "error", err)
		return models.TokenData{}, false
	}

	decryptedKeyData, err := s.identity.DecryptData([]byte(keyData))
	if err != nil {
		s.logger.Error("Could not decrypt key data", "error", err)
		return models.TokenData{}, false
	}

	var td models.TokenData
	if err := json.Unmarshal(decryptedKeyData, &td); err != nil {
		s.logger.Error("Could not unmarshal token data", "error", err)
		return models.TokenData{}, false
	}

	if td.UUID != uuid {
		s.logger.Error("UUID mismatch", "expected", uuid, "actual", td.UUID)
		return models.TokenData{}, false
	}

	return models.TokenData{
		Entity: entity,
		UUID:   uuid,
	}, true
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

	decryptedKeyData, err := s.identity.DecryptData(encryptedKeyData)
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

	keyTag := fmt.Sprintf("%s:%s", keyName, keyUUID)

	apiKeyRootStorageKey := fmt.Sprintf("%s:api:key:%s", s.cfg.RootPrefix, keyTag)

	td := models.TokenData{
		Entity: keyName,
		UUID:   keyUUID,
	}

	encodedTd, err := json.Marshal(td)
	if err != nil {
		return "", fmt.Errorf("could not marshal token data: %w", err)
	}

	encryptedKeyData, err := s.identity.EncryptData(encodedTd)
	if err != nil {
		return "", fmt.Errorf("could not encrypt uuid: %w", err)
	}

	b64KeyData := base64.StdEncoding.EncodeToString(encryptedKeyData)

	actualKey := fmt.Sprintf("insi_%s", b64KeyData)

	keyDataEncoded, err := json.Marshal(td)
	if err != nil {
		return "", fmt.Errorf("could not marshal token data: %w", err)
	}

	s.fsm.Set(models.KVPayload{
		Key:   apiKeyRootStorageKey,
		Value: string(keyDataEncoded),
	})

	return actualKey, nil
}

func (s *Service) DeleteApiKey(key string) error {

	entity, uuid, err := s.decomposeKey(key)
	if err != nil {
		return fmt.Errorf("could not decompose key: %w", err)
	}

	apiKeyRootStorageKey := fmt.Sprintf("%s:api:key:%s:%s", s.cfg.RootPrefix, entity, uuid)

	s.fsm.Delete(apiKeyRootStorageKey)

	return nil
}
