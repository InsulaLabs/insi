package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/InsulaLabs/insi/internal/tkv"
	"github.com/InsulaLabs/insi/models"
)

/*
	Handlers for ATOMIC operations
*/

// -- READ ONLY OPERATIONS --

func (s *Service) atomicGetHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	s.logger.Debug("AtomicGetHandler", "entity", td.Entity)

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing 'key' query parameter", http.StatusBadRequest)
		return
	}

	prefixedKey := fmt.Sprintf("%s:%s", td.UUID, key)
	if sizeTooLargeForStorage(prefixedKey) {
		http.Error(w, "Prefixed key is too large", http.StatusBadRequest)
		return
	}

	internalAtomicKey := fmt.Sprintf("__atomic__:%s", prefixedKey)

	value, err := s.fsm.AtomicGet(internalAtomicKey)
	if err != nil {
		var invalidStateErr *tkv.ErrInvalidState
		if errors.As(err, &invalidStateErr) {
			s.logger.Warn("AtomicGet failed due to invalid state", "key", prefixedKey, "internal_key", internalAtomicKey, "error", err)
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		s.logger.Error("Could not perform AtomicGet via FSM/TKV", "key", prefixedKey, "internal_key", internalAtomicKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	resp := models.AtomicGetResponse{
		Key:   key, // Return original non-prefixed key to the client
		Value: value,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode AtomicGetResponse", "error", err)
		// Response already started, cannot send different HTTP error
	}
}

// -- WRITE OPERATIONS --

func (s *Service) atomicNewHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	s.logger.Debug("AtomicNewHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for atomic new request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.AtomicNewRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Invalid JSON payload for atomic new request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Missing key in atomic new request payload", http.StatusBadRequest)
		return
	}

	prefixedKey := fmt.Sprintf("%s:%s", td.UUID, req.Key)
	if sizeTooLargeForStorage(prefixedKey) {
		http.Error(w, "Prefixed key is too large", http.StatusBadRequest)
		return
	}

	internalAtomicKey := fmt.Sprintf("__atomic__:%s", prefixedKey)

	err = s.fsm.AtomicNew(internalAtomicKey, req.Overwrite)
	if err != nil {
		// Check for specific TKV errors that might be returned from FSM/TKV
		var keyExistsErr *tkv.ErrKeyExists
		if errors.As(err, &keyExistsErr) {
			s.logger.Warn("AtomicNew failed because key already exists", "key", prefixedKey, "internal_key", internalAtomicKey, "error", err)
			http.Error(w, err.Error(), http.StatusConflict) // 409 Conflict
			return
		}
		s.logger.Error("Could not perform AtomicNewRaft via FSM", "key", prefixedKey, "internal_key", internalAtomicKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) atomicAddHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	s.logger.Debug("AtomicAddHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for atomic add request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.AtomicAddRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Invalid JSON payload for atomic add request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Missing key in atomic add request payload", http.StatusBadRequest)
		return
	}

	prefixedKey := fmt.Sprintf("%s:%s", td.UUID, req.Key)
	if sizeTooLargeForStorage(prefixedKey) {
		http.Error(w, "Prefixed key is too large", http.StatusBadRequest)
		return
	}

	internalAtomicKey := fmt.Sprintf("__atomic__:%s", prefixedKey)

	newValue, err := s.fsm.AtomicAdd(internalAtomicKey, req.Delta)
	if err != nil {
		var invalidStateErr *tkv.ErrInvalidState
		if errors.As(err, &invalidStateErr) {
			s.logger.Warn("AtomicAdd failed due to invalid state", "key", prefixedKey, "internal_key", internalAtomicKey, "error", err)
			http.Error(w, err.Error(), http.StatusConflict) // 409 Conflict or 422 Unprocessable Entity
			return
		}
		s.logger.Error("Could not perform AtomicAddRaft via FSM", "key", prefixedKey, "internal_key", internalAtomicKey, "delta", req.Delta, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	resp := models.AtomicAddResponse{
		Key:      req.Key, // Return original non-prefixed key
		NewValue: newValue,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("Failed to encode AtomicAddResponse", "error", err)
	}
}

func (s *Service) atomicDeleteHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	s.logger.Debug("AtomicDeleteHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for atomic delete request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.AtomicKeyPayload // Using AtomicKeyPayload as we only need the key
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Invalid JSON payload for atomic delete request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Missing key in atomic delete request payload", http.StatusBadRequest)
		return
	}

	prefixedKey := fmt.Sprintf("%s:%s", td.UUID, req.Key)
	if sizeTooLargeForStorage(prefixedKey) {
		http.Error(w, "Prefixed key is too large", http.StatusBadRequest)
		return
	}

	internalAtomicKey := fmt.Sprintf("__atomic__:%s", prefixedKey)

	err = s.fsm.AtomicDelete(internalAtomicKey)
	if err != nil {
		// AtomicDelete in TKV is idempotent and doesn't return ErrKeyNotFound.
		// Any error here would likely be an internal FSM or TKV error.
		s.logger.Error("Could not perform AtomicDeleteRaft via FSM", "key", prefixedKey, "internal_key", internalAtomicKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
