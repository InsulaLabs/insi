package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

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

	fmt.Println("[BUG] constructing key for entity", entity)
	key, err := keyGen.ConstructApiKey(entity)
	if err != nil {
		return "", fmt.Errorf("failed to generate key: %w", err)
	}

	fmt.Println("[BUG] key generated", key)

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

	createdAt := time.Now()

	// Store the key in the db with a prefix that only the service can search for (or otherwise auth'd)
	if err := c.Set(key, createdAt.String()); err != nil {
		return "", fmt.Errorf("failed to store key: [set] %w", err)
	}

	// Add a tag that cant be searched by user unless they know the secret hash prefix
	if err := c.Tag(key, entity); err != nil {
		return "", fmt.Errorf("failed to store key: [tag] %w", err)
	}

	return key, nil
}

func (s *Service) deleteApiKey(targetKey string) error {

	keyMan := sentinel.NewSentinel(
		s.logger,
		apiKeyIdentifier,
		[]byte(s.cfg.InstanceSecret),
	)

	// Ensure that its valid and created by our secret
	_, _, err := keyMan.DeconstructApiKey(targetKey)
	if err != nil {
		return err
	}

	if s.fsm.IsLeader() {
		err = s.fsm.Delete(targetKey)
		if err != nil {
			return err
		}
	} else {
		s.logger.Info("Deleting api key from follower node => forwarding to leader", "key", targetKey)
		c, err := client.NewClient(&client.Config{
			HostPort:   s.nodeCfg.HttpBinding,
			ApiKey:     s.cfg.InstanceSecret,
			SkipVerify: s.cfg.ClientSkipVerify,
			Logger:     s.logger,
		})
		if err != nil {
			return err
		}
		err = c.Delete(targetKey)
		if err != nil {
			return err
		}
	}
	return nil
}
