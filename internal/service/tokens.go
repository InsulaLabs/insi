package service

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insula/security/sentinel"
)

type TokenCache struct {
	Entity string `json:"entity"`
	UUID   string `json:"uuid"`
}

// Returns the entity name (as encoded by user) and then the uuid generated unique to the key
func (s *Service) validateToken(r *http.Request, mustBeRoot bool) (string, string, bool) {

	authHeader := r.Header.Get("Authorization")
	if mustBeRoot {
		return EntityRoot, s.cfg.RootPrefix, authHeader == s.authToken
	}

	if authHeader == s.authToken {
		return EntityRoot, s.cfg.RootPrefix, true
	}

	// Check the cache first
	item := s.lcs.apiKeys.Get(authHeader)
	if item != nil {
		tokenCache := TokenCache{}
		err := json.Unmarshal([]byte(item.Value()), &tokenCache)
		if err != nil {
			s.logger.Error("Failed to unmarshal token cache", "error", err)
			return "", "", false
		}
		return tokenCache.Entity, tokenCache.UUID, true
	}

	/*
		We create the thing with the root api key which means it gets the root prefix
		and we need to scope to the root prefix to get the actual key
	*/
	scopedKey := fmt.Sprintf("%s:%s", s.cfg.RootPrefix, authHeader)
	value, err := s.fsm.Get(scopedKey)
	if err != nil {
		s.logger.Error(
			"Failed to get value from valuesDb",
			"error", err,
		)
		return "", "", false
	}

	if value == "" {
		s.logger.Error(
			"Value is empty",
			"key", authHeader,
		)
		return "", "", false
	}

	keyMan := sentinel.NewSentinel(
		s.logger,
		apiKeyIdentifier,
		[]byte(s.cfg.InstanceSecret),
	)

	entity, uuid, err := keyMan.DeconstructApiKey(authHeader)
	if err != nil {
		s.logger.Error(
			"Failed to deconstruct API key during validation",
			"key", authHeader,
			"error", err,
		)
		return "", "", false
	}

	tokenCache := TokenCache{
		Entity: entity,
		UUID:   uuid,
	}
	cacheValue, err := json.Marshal(tokenCache)
	if err == nil {
		// Store the key in the cache
		s.lcs.apiKeys.Set(authHeader, string(cacheValue), s.cfg.Cache.Keys)
	}

	return entity, uuid, true
}

func (s *Service) newApiKey(entity string) (string, error) {

	if entity == "" {
		return "", fmt.Errorf("entity is required")
	}

	keyGen := sentinel.NewSentinel(
		s.logger,
		apiKeyIdentifier,
		[]byte(s.cfg.InstanceSecret),
	)

	apiKey, err := keyGen.ConstructApiKey(entity)
	if err != nil {
		return "", fmt.Errorf("failed to generate key: %w", err)
	}

	// store the key in the db
	internalClientLogger := s.logger.WithGroup("internal-client")
	c, err := client.NewClient(&client.Config{
		HostPort:     s.nodeCfg.HttpBinding,
		ApiKey:       s.authToken,
		SkipVerify:   s.cfg.ClientSkipVerify,
		Logger:       internalClientLogger,
		ClientDomain: s.nodeCfg.ClientDomain,
	})
	if err != nil {
		return "", fmt.Errorf("failed to store key: %w", err)
	}

	/*
		STORE:
			ROOT_PREFIX:api_key:ENTITY_NAME => API_KEY
			API_KEY => ENTITY_NAME
	*/
	if err := c.Set(fmt.Sprintf("%s:api_key:%s", s.cfg.RootPrefix, entity), apiKey); err != nil {
		return "", fmt.Errorf("failed to store key: [set] %w", err)
	}
	if err := c.Set(apiKey, entity); err != nil {
		return "", fmt.Errorf("failed to store key: [set] %w", err)
	}

	return apiKey, nil
}

func (s *Service) deleteApiKey(targetKey string) error {

	keyMan := sentinel.NewSentinel(
		s.logger,
		apiKeyIdentifier,
		[]byte(s.cfg.InstanceSecret),
	)

	// Ensure that its valid and created by our secret
	entity, _, err := keyMan.DeconstructApiKey(targetKey)
	if err != nil {
		return err
	}

	// Remove from cache
	s.lcs.apiKeys.Delete(targetKey)

	if s.fsm.IsLeader() {
		// Key that validateToken specifically checks in FSM
		validationSpecificKey := fmt.Sprintf("%s:%s", s.cfg.RootPrefix, targetKey)
		err = s.fsm.Delete(validationSpecificKey)
		if err != nil {
			s.logger.Error("Failed to delete validation-specific API key from leader node", "key", validationSpecificKey, "error", err)
			// continue to delete the other values if failure occurs
		}

		// Mapping from ROOT_PREFIX:api_key:ENTITY -> API_KEY
		entityToApiKeyKey := fmt.Sprintf("%s:api_key:%s", s.cfg.RootPrefix, entity)
		err = s.fsm.Delete(entityToApiKeyKey)
		if err != nil {
			s.logger.Error("Failed to delete entity-to-api-key mapping from leader node", "key", entityToApiKeyKey, "error", err)
			// continue to delete the other value if failure occurs
		}

		// Mapping from API_KEY -> ENTITY
		err = s.fsm.Delete(targetKey)
		if err != nil {
			s.logger.Error("Failed to delete api-key-to-entity mapping from leader node", "key", targetKey, "error", err)
			// continue to delete the other value if failure occurs
		}
		return nil
	} else {
		s.logger.Info("Deleting api key from follower node => forwarding to leader", "key", targetKey)
		internalClientLogger := s.logger.WithGroup("internal-client-delete-api-key")
		c, err := client.NewClient(&client.Config{
			HostPort:     s.nodeCfg.HttpBinding,
			ApiKey:       s.authToken,
			SkipVerify:   s.cfg.ClientSkipVerify,
			Logger:       internalClientLogger,
			ClientDomain: s.nodeCfg.ClientDomain,
		})
		if err != nil {
			return fmt.Errorf("failed to create client for forwarding delete: %w", err)
		}

		// Key that validateToken specifically checks in FSM
		validationSpecificKey := fmt.Sprintf("%s:%s", s.cfg.RootPrefix, targetKey)
		err = c.Delete(validationSpecificKey)
		if err != nil {
			s.logger.Error("Failed to delete validation-specific API key via follower->leader", "key", validationSpecificKey, "error", err)
		}

		// Mapping from ROOT_PREFIX:api_key:ENTITY -> API_KEY
		entityToApiKeyKey := fmt.Sprintf("%s:api_key:%s", s.cfg.RootPrefix, entity)
		err = c.Delete(entityToApiKeyKey)
		if err != nil {
			s.logger.Error("Failed to delete entity-to-api-key mapping via follower->leader", "key", entityToApiKeyKey, "error", err)
		}

		err = c.Delete(targetKey)
		if err != nil {
			s.logger.Error("Failed to delete api-key-to-entity mapping via follower->leader", "key", targetKey, "error", err)
			return fmt.Errorf("failed to delete api-key-to-entity mapping on leader: %w", err)
		}
		return nil
	}
}
