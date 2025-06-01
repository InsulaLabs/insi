package service

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/models"
	"github.com/InsulaLabs/insula/security/sentinel"
	"golang.org/x/time/rate"
)

type TokenCache struct {
	Entity    string            `json:"entity"`
	UUID      string            `json:"uuid"`
	KeyLimits *models.KeyLimits `json:"key_limits"`
}

var rootKeyLimits = models.KeyLimits{
	MaxKeySizeBytes:    1024 * 1024, // 1MB
	MaxValueSizeBytes:  1024 * 1024, // 1MB
	MaxBatchSize:       1000,
	WritesPerSecond:    1000,
	ReadsPerSecond:     1000,
	EventsPerSecond:    1000,
	SubscribersPerNode: 100,
	MaxTotalBytes:      100 * 1024 * 1024 * 1024, // 100	GB
}

type TokenData struct {
	Entity    string
	UUID      string
	ApiKey    string
	KeyLimits *models.KeyLimits
}

// ApiKeyDetails holds an API key and its associated usage limits.
// This is used to retrieve all keys and their configurations, for example, on startup.
type ApiKeyDetails struct {
	ApiKey    string            `json:"api_key"`
	KeyLimits *models.KeyLimits `json:"key_limits"`
}

// Returns the entity name (as encoded by user) and then the uuid generated unique to the key
func (s *Service) validateToken(r *http.Request, mustBeRoot bool) (TokenData, bool) {

	authHeader := r.Header.Get("Authorization")
	if mustBeRoot {
		return TokenData{
			Entity:    EntityRoot,
			UUID:      s.cfg.RootPrefix,
			ApiKey:    authHeader,
			KeyLimits: &rootKeyLimits,
		}, authHeader == s.authToken
	}

	if authHeader == s.authToken {
		return TokenData{
			Entity:    EntityRoot,
			UUID:      s.cfg.RootPrefix,
			ApiKey:    authHeader,
			KeyLimits: &rootKeyLimits,
		}, true
	}

	cacheItem := s.lcs.apiKeys.Get(authHeader)
	if cacheItem != nil {
		tokenCache := TokenCache{}
		err := json.Unmarshal([]byte(cacheItem.Value()), &tokenCache)
		if err != nil {
			s.logger.Error("Failed to unmarshal token cache", "error", err)
		}
		fmt.Println("validateToken: CACHE HIT", cacheItem.Value())
		return TokenData{
			Entity:    tokenCache.Entity,
			UUID:      tokenCache.UUID,
			ApiKey:    authHeader,
			KeyLimits: tokenCache.KeyLimits,
		}, true
	}

	/*
		We create the thing with the root api key which means it gets the root prefix
		and we need to scope to the root prefix to get the actual key
	*/
	scopedKey := fmt.Sprintf("%s:%s", s.cfg.RootPrefix, authHeader)
	limitsValue, err := s.fsm.Get(scopedKey)
	if err != nil {
		s.logger.Error(
			"Failed to get value from valuesDb",
			"error", err,
		)
		return TokenData{}, false
	}

	if limitsValue == "" {
		s.logger.Error(
			"Value is empty",
			"key", authHeader,
		)
		return TokenData{}, false
	}

	limits := models.KeyLimits{}
	err = json.Unmarshal([]byte(limitsValue), &limits)
	if err != nil {
		s.logger.Error("Failed to unmarshal key limits", "error", err)
		return TokenData{}, false
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
		return TokenData{}, false
	}

	// store the key in the cache
	tokenCache := TokenCache{
		Entity:    entity,
		UUID:      uuid,
		KeyLimits: &limits,
	}
	cacheValue, err := json.Marshal(tokenCache)
	if err == nil {
		// Store the key in the cache
		s.lcs.apiKeys.Set(authHeader, string(cacheValue), s.cfg.Cache.Keys)
	}

	return TokenData{
		Entity:    entity,
		UUID:      uuid,
		ApiKey:    authHeader,
		KeyLimits: &limits,
	}, true
}

func (s *Service) newApiKey(entity string, keyLimits *models.KeyLimits) (string, error) {

	fmt.Println("newApiKey", entity, keyLimits)
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
		ConnectionType: client.ConnectionTypeDirect,
		Endpoints: []client.Endpoint{
			{
				HostPort:     s.nodeCfg.HttpBinding,
				ClientDomain: s.nodeCfg.ClientDomain,
			},
		},
		ApiKey:     s.authToken,
		SkipVerify: s.cfg.ClientSkipVerify,
		Logger:     internalClientLogger,
	})
	if err != nil {
		return "", fmt.Errorf("failed to store key: %w", err)
	}

	limitsEncoded, err := json.Marshal(keyLimits)
	if err != nil {
		return "", fmt.Errorf("failed to marshal key limits: %w", err)
	}

	/*
		STORE:
			ROOT_PREFIX:api_key:ENTITY_NAME => API_KEY
			API_KEY => ENTITY_NAME
	*/
	if err := c.Set(fmt.Sprintf("%s:api_key:%s", s.cfg.RootPrefix, entity), apiKey); err != nil {
		return "", fmt.Errorf("failed to store key: [set] %w", err)
	}
	if err := c.Set(apiKey, string(limitsEncoded)); err != nil {
		return "", fmt.Errorf("failed to store key: [set] %w", err)
	}

	// Add to usage map for limiting
	s.lcs.apiKeyUsageLock.Lock()
	s.lcs.apiKeyUsage[apiKey] = apiKeyUsageData{
		ReadLimiter:      rate.NewLimiter(rate.Limit(keyLimits.ReadsPerSecond), keyLimits.ReadsPerSecond),
		WriteLimiter:     rate.NewLimiter(rate.Limit(keyLimits.WritesPerSecond), keyLimits.WritesPerSecond),
		EventLimiter:     rate.NewLimiter(rate.Limit(keyLimits.EventsPerSecond), keyLimits.EventsPerSecond),
		ActiveSubsOnNode: 0,
	}
	s.lcs.apiKeyUsageLock.Unlock()

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

	// Remove from usage map
	s.lcs.apiKeyUsageLock.Lock()
	delete(s.lcs.apiKeyUsage, targetKey)
	s.lcs.apiKeyUsageLock.Unlock()

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
			ConnectionType: client.ConnectionTypeDirect,
			Endpoints: []client.Endpoint{
				{
					HostPort:     s.nodeCfg.HttpBinding,
					ClientDomain: s.nodeCfg.ClientDomain,
				},
			},
			ApiKey:     s.authToken,
			SkipVerify: s.cfg.ClientSkipVerify,
			Logger:     internalClientLogger,
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

// getAllApiKeyDetails retrieves all configured API keys along with their associated KeyLimits.
func (s *Service) getAllApiKeyDetails() ([]ApiKeyDetails, error) {
	// Step 1: Get all unique API key strings.
	// API keys are stored as values for FSM keys of the pattern: RootPrefix + ":api_key:" + EntityName.
	iterationPrefix := fmt.Sprintf("%s:api_key:", s.cfg.RootPrefix)

	rawApiKeys, err := s.fsm.Iterate(iterationPrefix, 0, 0) // Offset 0, Limit 0 (no limit)
	if err != nil {
		s.logger.Error("Failed to list API keys from FSM using Iterate for getAllApiKeyDetails", "prefix", iterationPrefix, "error", err)
		return nil, fmt.Errorf("failed to retrieve API keys from FSM: %w", err)
	}

	uniqueApiKeys := make(map[string]struct{})
	for _, apiKey := range rawApiKeys {
		if apiKey != "" {
			uniqueApiKeys[apiKey] = struct{}{}
		}
	}

	apiKeyDetailsList := make([]ApiKeyDetails, 0, len(uniqueApiKeys))

	for apiKey := range uniqueApiKeys {
		fsmLimitsKey := fmt.Sprintf("%s:%s", s.cfg.RootPrefix, apiKey)
		limitsValueString, errGet := s.fsm.Get(fsmLimitsKey)
		if errGet != nil {
			s.logger.Error(
				"Failed to get KeyLimits from FSM for API key",
				"apiKey", apiKey,
				"fsmLimitsKey", fsmLimitsKey,
				"error", errGet,
			)
			continue
		}

		if limitsValueString == "" {
			s.logger.Warn(
				"KeyLimits value is empty in FSM for API key",
				"apiKey", apiKey,
				"fsmLimitsKey", fsmLimitsKey,
			)
			continue
		}

		var limits models.KeyLimits
		if errUnmarshal := json.Unmarshal([]byte(limitsValueString), &limits); errUnmarshal != nil {
			s.logger.Error(
				"Failed to unmarshal KeyLimits for API key",
				"apiKey", apiKey,
				"fsmLimitsKey", fsmLimitsKey,
				"error", errUnmarshal,
			)
			continue
		}

		apiKeyDetailsList = append(apiKeyDetailsList, ApiKeyDetails{
			ApiKey:    apiKey,
			KeyLimits: &limits,
		})
	}

	// Ensure root API key and its limits are included
	if s.authToken != "" {
		foundRootKeyInList := false
		for i, detail := range apiKeyDetailsList {
			if detail.ApiKey == s.authToken {
				s.logger.Info("Root API key found in list from FSM scan; ensuring its limits are set to rootKeyLimits.", "apiKey", s.authToken)
				apiKeyDetailsList[i].KeyLimits = &rootKeyLimits // Ensure canonical root limits
				foundRootKeyInList = true
				break
			}
		}
		if !foundRootKeyInList {
			s.logger.Info("Adding root API key and its limits to the API details list.", "apiKey", s.authToken)
			apiKeyDetailsList = append(apiKeyDetailsList, ApiKeyDetails{
				ApiKey:    s.authToken,
				KeyLimits: &rootKeyLimits, // Use the global rootKeyLimits
			})
		}
	} else {
		s.logger.Warn("s.authToken is empty; cannot add root API key details.")
	}

	return apiKeyDetailsList, nil
}
