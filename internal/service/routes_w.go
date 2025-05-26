package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/InsulaLabs/insi/models"
)

// Helper function for redirection
func (s *Service) redirectToLeader(w http.ResponseWriter, r *http.Request, originalPath string) {
	leaderAddr, err := s.fsm.LeaderHTTPAddress()
	if err != nil {
		s.logger.Error("Failed to get leader HTTP address for redirection", "original_path", originalPath, "error", err)
		http.Error(w, "Failed to determine cluster leader for redirection: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	redirectURL := leaderAddr + originalPath
	if r.URL.RawQuery != "" {
		redirectURL += "?" + r.URL.RawQuery
	}

	s.logger.Info("Redirecting to leader", "current_node_is_follower", true, "leader_addr", leaderAddr, "original_path", originalPath, "redirect_url", redirectURL)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

/*
	Handlers that update the "VALUES" database
*/

func (s *Service) setHandler(w http.ResponseWriter, r *http.Request) {
	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("SetHandler", "entity", entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for set request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KVPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for set request", "error", err)
		http.Error(w, "Invalid JSON payload for set: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in set request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", uuid, p.Key)

	err = s.fsm.Set(p)
	if err != nil {
		s.logger.Error("Could not write key-value via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) deleteHandler(w http.ResponseWriter, r *http.Request) {
	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("DeleteHandler", "entity", entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for delete request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KVPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for unset request", "error", err)
		http.Error(w, "Invalid JSON payload for unset: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in unset request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", uuid, p.Key)

	err = s.fsm.Delete(p.Key)
	if err != nil {
		s.logger.Error("Could not unset value via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

/*
	Handlers that update the "TAGS" database
*/

func (s *Service) untagHandler(w http.ResponseWriter, r *http.Request) {
	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("UntagHandler", "entity", entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for untag request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KVPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for untag request", "error", err)
		http.Error(w, "Invalid JSON payload for untag: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in untag request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", uuid, p.Key)

	err = s.fsm.Untag(p)
	if err != nil {
		s.logger.Error("Could not untag via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) tagHandler(w http.ResponseWriter, r *http.Request) {
	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("TagHandler", "entity", entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for tag request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KVPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for tag request", "error", err)
		http.Error(w, "Invalid JSON payload for tag: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in tag request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", uuid, p.Key)

	err = s.fsm.Tag(p)
	if err != nil {
		s.logger.Error("Could not tag via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) setCacheHandler(w http.ResponseWriter, r *http.Request) {
	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("SetCacheHandler", "entity", entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for set cache request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.CachePayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for set cache request", "error", err)
		http.Error(w, "Invalid JSON payload for set cache: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in set cache request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", uuid, p.Key)

	err = s.fsm.SetCache(p)
	if err != nil {
		s.logger.Error("Could not set cache via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) deleteCacheHandler(w http.ResponseWriter, r *http.Request) {
	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("DeleteCacheHandler", "entity", entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for delete cache request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KeyPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for delete cache request", "error", err)
		http.Error(w, "Invalid JSON payload for delete cache: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in delete cache request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", uuid, p.Key)

	err = s.fsm.DeleteCache(p.Key)
	if err != nil {
		s.logger.Error("Could not delete cache via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
