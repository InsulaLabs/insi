package service

import (
	"fmt"
	"net/http"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insula/security/sentinel"
)

func (s *Service) validateToken(r *http.Request, mustBeRoot bool) (string, bool) {

	authHeader := r.Header.Get("Authorization")
	if mustBeRoot {
		return EntityRoot, authHeader == s.authToken
	}

	if authHeader == s.authToken {
		return EntityRoot, true
	}

	// Check the cache first
	item := s.lcs.apiKeys.Get(authHeader)
	if item != nil {
		return item.Value(), true
	}

	pstk := s.assemblePotentiallyStoredKey(authHeader)

	value, err := s.fsm.Get(pstk)
	if err != nil {
		s.logger.Error(
			"Failed to get value from valuesDb",
			"error", err,
		)
		return "", false
	}

	if value == "" {
		s.logger.Error(
			"Value is empty",
			"key", pstk,
		)
		return "", false
	}

	keyMan := sentinel.NewSentinel(
		s.logger,
		apiKeyIdentifier,
		[]byte(s.cfg.InstanceSecret),
	)

	entity, err := keyMan.DeconstructApiKey(authHeader)
	if err != nil {
		s.logger.Error(
			"Failed to deconstruct API key during validation",
			"key", authHeader,
			"error", err,
		)
		return "", false
	}

	// Store the key in the cache
	s.lcs.apiKeys.Set(authHeader, entity, s.cfg.Cache.Keys)

	return entity, true
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

	key, err := keyGen.ConstructApiKey(entity)
	if err != nil {
		return "", fmt.Errorf("failed to generate key: %w", err)
	}

	// store the key in the db
	internalClientLogger := s.logger.WithGroup("internal-client")
	c, err := client.NewClient(s.nodeCfg.HttpBinding, s.authToken, s.cfg.ClientSkipVerify, internalClientLogger)
	if err != nil {
		return "", fmt.Errorf("failed to store key: %w", err)
	}

	keyForStorage := s.assembleSystemKeyForRootStorage(key)
	entityTag := s.assembleSystemTagForNewEntityKey(entity)
	createdAt := time.Now()

	// Store the key in the db with a prefix that only the service can search for (or otherwise auth'd)
	if err := c.Set(keyForStorage, createdAt.String()); err != nil {
		return "", fmt.Errorf("failed to store key: [set] %w", err)
	}

	// Add a tag that cant be searched by user unless they know the secret hash prefix
	if err := c.Tag(keyForStorage, entityTag); err != nil {
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
	_, err := keyMan.DeconstructApiKey(targetKey)
	if err != nil {
		return err
	}

	storedAs := s.assembleSystemKeyForRootStorage(targetKey)

	if s.fsm.IsLeader() {
		err = s.fsm.Delete(storedAs)
		if err != nil {
			return err
		}
	} else {
		s.logger.Info("Deleting api key from follower node => forwarding to leader", "key", targetKey)
		c, err := client.NewClient(s.nodeCfg.HttpBinding, s.cfg.InstanceSecret, s.cfg.ClientSkipVerify, s.logger)
		if err != nil {
			return err
		}
		err = c.Delete(storedAs)
		if err != nil {
			return err
		}
	}
	return nil
}
