package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/InsulaLabs/insi/models"
)

const apiKeyIdentifier = "insi_"

func (s *Service) authedPing(w http.ResponseWriter, r *http.Request) {
	td, ok := s.validateToken(r, false)
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
	td, ok := s.validateToken(r, true)
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
		s.logger.Error("Failed to join follower", "followerId", followerId, "followerAddr", followerAddr, "error", err)
		http.Error(w, fmt.Sprintf("Failed to join follower: %s", err), http.StatusInternalServerError)
		return
	}
}

func (s *Service) newApiKeyHandler(w http.ResponseWriter, r *http.Request) {
	s.logger.Debug("Enter newApiKeyHandler", "method", r.Method, "url", r.URL.String())
	td, ok := s.validateToken(r, true)
	if !ok || td.Entity != "root" {
		s.logger.Warn("Unauthorized attempt to create API key", "remote_addr", r.RemoteAddr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	entity := r.URL.Query().Get("entity")
	if entity == "" {
		s.logger.Warn("Missing entity parameter in newApiKeyHandler", "remote_addr", r.RemoteAddr)
		http.Error(w, "Missing entity parameter", http.StatusBadRequest)
		return
	}

	requestedLimits := r.URL.Query().Get("limits")
	var limits *models.KeyLimits
	if requestedLimits != "" {
		limits = &models.KeyLimits{}
		err := json.Unmarshal([]byte(requestedLimits), limits)
		if err != nil {
			s.logger.Error("Failed to unmarshal limits", "error", err)
		}
	}

	s.logger.Info("Attempting to create new API key for entity", "entity", entity, "requested_by_root", true)
	key, err := s.newApiKey(entity, limits)
	if err != nil {
		s.logger.Error("Failed to generate or store new API key", "entity", entity, "error", err)
		http.Error(w, fmt.Sprintf("Failed to generate key: %s", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	// respond with the json key
	if err := json.NewEncoder(w).Encode(map[string]string{"apiKey": key}); err != nil {
		s.logger.Error("Failed to encode/write new API key response", "entity", entity, "error", err)
		// Don't try to write another http.Error here as headers might already be sent
		return
	}
	s.logger.Info("Successfully created and returned new API key", "entity", entity, "key_prefix", key[:4]+"...")
}

func (s *Service) deleteApiKeyHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.validateToken(r, true)
	if !ok || td.Entity != "root" {
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
