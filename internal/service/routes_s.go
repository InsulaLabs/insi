package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insula/security/sentinel"
)

const apiKeyIdentifier = "insi_"

func (s *Service) authedPing(w http.ResponseWriter, r *http.Request) {
	storedRootEntity, ok := s.validateToken(r, false) // <----- NOTE: Not root only for system
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	uptime := time.Since(s.startedAt).String()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":        "ok",
		"entity":        storedRootEntity,
		"node-badge-id": s.identity.GetID(),
		"leader":        s.fsm.Leader(),
		"uptime":        uptime,
	})
}

/*
	Handlers that are meant for system-level operations (not data-level operations)
*/

func (s *Service) joinHandler(w http.ResponseWriter, r *http.Request) {
	// Join operations must absolutely go to the leader.
	// The redirectToLeader helper is defined in routes_w.go (or could be moved to a common place if preferred)
	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path) // Assumes redirectToLeader is accessible, e.g. part of Service or in same package
		return
	}

	// Only admin "root" key can tell nodes to join the cluster
	entity, ok := s.validateToken(r, true)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// We already enforced that its root, but this will be a sanity check to ensure that
	// something isn't corrupted or otherwise malicious
	if entity != "root" {
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
		s.logger.Error("Failed to join follower", "followerId", followerId, "followerAddr", followerAddr, "error", err)
		http.Error(w, fmt.Sprintf("Failed to join follower: %s", err), http.StatusInternalServerError)
		return
	}
}

func (s *Service) newApiKeyHandler(w http.ResponseWriter, r *http.Request) {
	storedRootEntity, ok := s.validateToken(r, true)
	if !ok || storedRootEntity != "root" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	entity := r.URL.Query().Get("entity")
	if entity == "" {
		http.Error(w, "Missing entity parameter", http.StatusBadRequest)
		return
	}

	if entity == "" {
		http.Error(w, "Invalid entity parameter", http.StatusBadRequest)
		return
	}

	// We generate the key and then store it. Once success, we return the key in a json response

	keyGen := sentinel.NewSentinel(
		s.logger,
		apiKeyIdentifier,
		[]byte(s.cfg.InstanceSecret),
	)

	key, err := keyGen.ConstructApiKey(entity)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate key: %s", err), http.StatusInternalServerError)
	}

	// store the key in the db
	internalClientLogger := s.logger.WithGroup("internal-client")
	c, err := client.NewClient(s.nodeCfg.HttpBinding, s.authToken, s.cfg.ClientSkipVerify, internalClientLogger)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to store key: %s", err), http.StatusInternalServerError)
		return
	}

	keyForStorage := fmt.Sprintf("%s:%s", s.authToken, key)
	entityTag := fmt.Sprintf("%s:%s", s.authToken, entity)

	createdAt := time.Now()

	// Store the key in the db with a prefix that only the service can search for (or otherwise auth'd)
	if err := c.Set(keyForStorage, createdAt.String()); err != nil {
		http.Error(w, fmt.Sprintf("Failed to store key: [set] %s", err), http.StatusInternalServerError)
		return
	}

	// Add a tag that cant be searched by user unless they know the secret hash prefix
	if err := c.Tag(keyForStorage, entityTag); err != nil {
		http.Error(w, fmt.Sprintf("Failed to store key: [tag] %s", err), http.StatusInternalServerError)
		return
	}

	// respond with the json key
	json.NewEncoder(w).Encode(map[string]string{"apiKey": key})

	w.Header().Set("Content-Type", "application/json")
}

func (s *Service) deleteApiKeyHandler(w http.ResponseWriter, r *http.Request) {
	storedRootEntity, ok := s.validateToken(r, true)
	if !ok || storedRootEntity != "root" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	targetKey := r.URL.Query().Get("key")
	if targetKey == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	err := s.deleteApiKey(targetKey)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to delete key: %s", err), http.StatusInternalServerError)
		return
	}
}
