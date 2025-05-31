/*
	This set of routes are for off-network meaning "local node only"
	routes. So they dont need to interact with the FSM.

	They are used for:
	- Creating scope tokens
	- Verifying scope tokens

	We create scope tokens for ephemeral access and validation, and for security purposes
	we ensure that the any follow-up validation happens specifically with the node that
	issued the token.

*/

package service

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"time"

	"github.com/InsulaLabs/insi/models"
)

func (s *Service) etokNewHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	entity, uuid, _, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.EtokenRequest
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if p.TTL == 0 {
		s.logger.Error("TTL must be greater than 0")
		http.Error(w, "TTL must be greater than 0", http.StatusBadRequest)
		return
	}

	if p.TTL > time.Hour*24 {
		s.logger.Error("TTL must be less than 24 hours")
		http.Error(w, "TTL must be less than 24 hours", http.StatusBadRequest)
		return
	}

	token, err := s.securityManager.NewScopeToken(entity, uuid, p.Scopes, p.TTL)
	if err != nil {
		s.logger.Error("Could not create scope token", "error", err)
		http.Error(w, "Could not create scope token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := models.EtokenResponse{Token: token}

	if err := json.NewEncoder(w).Encode(rsp); err != nil {
		s.logger.Error("Could not encode response for token", "error", err)
	}
}

func (s *Service) etokVerifyHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.EtokenVerifyRequest
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	entity, uuid, scopes, err := s.securityManager.VerifyScopeToken(p.Token)
	if err != nil {
		s.logger.Error("Could not verify scope token", "error", err)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if entity == "" || uuid == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !reflect.DeepEqual(scopes, p.Scopes) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := models.EtokenVerifyResponse{Verified: true}

	if err := json.NewEncoder(w).Encode(rsp); err != nil {
		s.logger.Error("Could not encode response for scopes", "error", err)
	}
}
