package service

import (
	"fmt"
	"net/http"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insula/security/sentinel"
)

func (s *Service) validateToken(r *http.Request, mustBeRoot bool) (string, bool) {

	authHeader := r.Header.Get("Authorization")
	if mustBeRoot {
		return "root", authHeader == s.authToken
	}

	if authHeader == s.authToken {
		return "root", true
	}

	// if the auth header is not the root auth token, we need to check if it's a user api key
	// first prefix with the secret (already hashed)
	// (potentially stored token key)
	pstk := fmt.Sprintf("%s:%s", s.authToken, authHeader)

	value, err := s.fsm.Get(pstk)
	if err != nil {
		s.logger.Error("Failed to get value from valuesDb", "error", err)
		return "", false
	}

	if value == "" {
		s.logger.Error("Value is empty", "key", pstk)
		return "", false
	}

	keyMan := sentinel.NewSentinel(
		s.logger,
		apiKeyIdentifier,
		[]byte(s.cfg.InstanceSecret),
	)

	entity, err := keyMan.DeconstructApiKey(authHeader)
	if err != nil {
		s.logger.Error("Failed to deconstruct API key during validation", "key", authHeader, "error", err)
		return "", false
	}

	return entity, true
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

	storedAs := fmt.Sprintf("%s:%s", s.authToken, targetKey)

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
