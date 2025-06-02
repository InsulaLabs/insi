package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/InsulaLabs/insi/internal/tkv"
	"github.com/InsulaLabs/insi/models"
	"github.com/dgraph-io/badger/v3"
)

func (s *Service) getHandler(w http.ResponseWriter, r *http.Request) {

	td, ok := s.ValidateToken(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("GetHandler", "entity", td.Entity)

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	value, err := s.fsm.Get(fmt.Sprintf("%s:%s", td.UUID, key))
	if err != nil {
		s.logger.Info("FSM Get for key returned error, treating as Not Found for now", "key", key, "error", err)
		http.NotFound(w, r)
		return
	}
	if value == "" {
		s.logger.Info("FSM Get for key returned empty value, treating as Not Found", "key", key)
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := struct {
		Data string `json:"data"`
	}{Data: value}

	if err := json.NewEncoder(w).Encode(rsp); err != nil {
		s.logger.Error("Could not encode response for key", "key", key, "error", err)
	}
}

func (s *Service) iterateKeysByPrefixHandler(w http.ResponseWriter, r *http.Request) {

	td, ok := s.ValidateToken(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("IterateKeysByPrefixHandler", "entity", td.Entity)

	prefix := r.URL.Query().Get("prefix")
	if td.Entity != EntityRoot && prefix == "" {
		http.Error(w, "Missing prefix parameter", http.StatusBadRequest)
		return
	}

	offset := r.URL.Query().Get("offset")
	limit := r.URL.Query().Get("limit")

	offsetInt, err := strconv.Atoi(offset)
	if err != nil {
		http.Error(w, "Invalid offset parameter", http.StatusBadRequest)
		return
	}

	limitInt, err := strconv.Atoi(limit)
	if err != nil {
		http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
		return
	}

	if limitInt <= 0 {
		limitInt = 100
	}

	if offsetInt < 0 {
		offsetInt = 0
	}

	value, err := s.fsm.Iterate(fmt.Sprintf("%s:%s", td.UUID, prefix), offsetInt, limitInt)
	if err == badger.ErrKeyNotFound {
		http.NotFound(w, r)
		return
	} else if err != nil {
		s.logger.Error("Could not iterate keys by prefix via FSM", "prefix", prefix, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := struct {
		Data []string `json:"data"`
	}{Data: value}

	if err := json.NewEncoder(w).Encode(rsp); err != nil {
		s.logger.Error("Could not encode response for iterate keys by prefix", "prefix", prefix, "error", err)
	}
}

func (s *Service) getCacheHandler(w http.ResponseWriter, r *http.Request) {

	td, ok := s.ValidateToken(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("GetCacheHandler", "entity", td.Entity)

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	value, err := s.fsm.GetCache(fmt.Sprintf("%s:%s", td.UUID, key))
	if err != nil {
		s.logger.Info("FSM GetCache for key returned error, treating as Not Found for now", "key", key, "error", err)
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := struct {
		Data string `json:"data"`
	}{Data: value}

	if errEnc := json.NewEncoder(w).Encode(rsp); errEnc != nil {
		s.logger.Error("Could not encode response for cache", "key", key, "error", errEnc)
		return
	}
}

func (s *Service) atomicGetHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r)
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
