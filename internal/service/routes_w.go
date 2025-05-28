package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net" // Added for net.SplitHostPort and net.JoinHostPort
	"net/http"

	"github.com/InsulaLabs/insi/models"
)

// Helper function for redirection
func (s *Service) redirectToLeader(w http.ResponseWriter, r *http.Request, originalPath string) {
	leaderRawHttpBinding, err := s.fsm.LeaderHTTPAddress() // This is the HttpBinding string from leader's config
	if err != nil {
		s.logger.Error("Failed to get leader HTTP address for redirection", "original_path", originalPath, "error", err)
		http.Error(w, "Failed to determine cluster leader for redirection: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	var leaderClientDomain string
	var foundLeaderNodeID string // For logging

	// Find the leader's ClientDomain from the cluster configuration (s.cfg)
	for nodeID, nodeCfg := range s.cfg.Nodes {
		if nodeCfg.HttpBinding == leaderRawHttpBinding {
			leaderClientDomain = nodeCfg.ClientDomain
			foundLeaderNodeID = nodeID
			break
		}
	}

	finalRedirectHostPort := ""
	parsedHostFromBinding, parsedPort, err := net.SplitHostPort(leaderRawHttpBinding)
	if err != nil {
		s.logger.Error("Failed to parse host/port from leader's raw HttpBinding for redirection",
			"raw_http_binding", leaderRawHttpBinding, "error", err)
		// As a fallback, use the raw HttpBinding, hoping it's a resolvable domain or client handles IP correctly (less ideal)
		finalRedirectHostPort = leaderRawHttpBinding
	} else {
		if leaderClientDomain != "" {
			finalRedirectHostPort = net.JoinHostPort(leaderClientDomain, parsedPort)
			s.logger.Info("Redirecting to leader: using ClientDomain for redirect host",
				"leader_node_id", foundLeaderNodeID,
				"client_domain", leaderClientDomain,
				"port", parsedPort,
				"derived_redirect_hostport", finalRedirectHostPort)
		} else {
			// ClientDomain not found/set for the leader node, use the host part from its HttpBinding
			finalRedirectHostPort = net.JoinHostPort(parsedHostFromBinding, parsedPort) // Reconstruct with parsed host
			s.logger.Info("Redirecting to leader: using host from HttpBinding for redirect (ClientDomain not found/set)",
				"leader_node_id", foundLeaderNodeID, // Will be empty if HttpBinding didn't match any node
				"raw_http_binding", leaderRawHttpBinding,
				"derived_redirect_hostport", finalRedirectHostPort)
		}
	}

	// Construct the absolute redirect URL, always using HTTPS for production security.
	// originalPath should already include the leading "/" (e.g., "/db/api/v1/set")
	redirectURL := "https://" + finalRedirectHostPort + originalPath
	if r.URL.RawQuery != "" {
		redirectURL += "?" + r.URL.RawQuery
	}

	s.logger.Info("Issuing redirect to leader",
		"current_node_is_follower", true,
		"leader_raw_http_binding_from_fsm", leaderRawHttpBinding,
		"final_redirect_url", redirectURL)

	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect) // Client follows this Location header
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

func (s *Service) eventsHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	fmt.Println("DEV> eventsHandler", entity, uuid)

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for events request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.Event
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for events request", "error", err)
		http.Error(w, "Invalid JSON payload for events: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Prefix the topic with the entity's UUID to scope it
	prefixedTopic := fmt.Sprintf("%s:%s", uuid, p.Topic)
	s.logger.Debug("Publishing event with prefixed topic", "original_topic", p.Topic, "prefixed_topic", prefixedTopic, "entity_uuid", uuid)

	err = s.fsm.Publish(prefixedTopic, p.Data)
	if err != nil {
		s.logger.Error("Could not publish event via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
}
