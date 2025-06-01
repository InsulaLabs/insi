package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const apiKeyIdentifier = "insi_"

func (s *Service) authedPing(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r)
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
	td, ok := s.ValidateToken(r)
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
