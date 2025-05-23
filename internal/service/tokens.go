package service

import (
	"fmt"
	"net/http"
	"strings"

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
		return "", false
	}

	if value == "" {
		return "", false
	}

	// Deconstruct the key using sentinel and validate the entity and secret passworc encoded in it (s.cfg.InstanceSecret)
	keyMan := sentinel.NewSentinel(
		s.logger,
		apiKeyIdentifier,
		[]byte(s.cfg.InstanceSecret),
	)

	deconstructed, err := keyMan.DeconstructApiKey(value)
	if err != nil {
		return "", false
	}

	// The parts are: entity:uuid. This is for randomness.
	// If we are able to deconstruct they key it was made with the same secret
	// as it is encrypted with it
	parts := strings.Split(deconstructed, ":")
	if len(parts) != 2 {
		return "", false
	}

	entity := parts[0]
	return entity, true
}

func (s *Service) deleteApiKey(targetKey string) error {

	// Deconstruct the key using sentinel and validate the entity and secret passworc encoded in it (s.cfg.InstanceSecret)
	keyMan := sentinel.NewSentinel(
		s.logger,
		apiKeyIdentifier,
		[]byte(s.cfg.InstanceSecret),
	)

	deconstructed, err := keyMan.DeconstructApiKey(targetKey)
	if err != nil {
		return err
	}

	parts := strings.Split(deconstructed, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid key")
	}

	pstk := fmt.Sprintf("%s:%s", s.authToken, parts[1])

	if s.fsm.IsLeader() {
		err = s.fsm.Delete(pstk)
		if err != nil {
			return err
		}
	} else {
		s.logger.Info("Deleting api key from follower node => forwarding to leader", "key", pstk)
		c, err := client.NewClient(s.nodeCfg.HttpBinding, s.cfg.InstanceSecret, s.cfg.ClientSkipVerify, s.logger)
		if err != nil {
			return err
		}
		err = c.Delete(pstk)
		if err != nil {
			return err
		}
	}

	return nil
}
