package service

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/dgraph-io/badger/v3"
)

func (s *Service) getHandler(w http.ResponseWriter, r *http.Request) {

	if !s.validateToken(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	value, err := s.fsm.Get(key)
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

func (s *Service) iterateKeysByTagsHandler(w http.ResponseWriter, r *http.Request) {

	if !s.validateToken(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	tag := r.URL.Query().Get("tag")
	if tag == "" {
		http.Error(w, "Missing tag parameter", http.StatusBadRequest)
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

	value, err := s.fsm.GetAllKeysWithTag(tag, offsetInt, limitInt)
	if err == badger.ErrKeyNotFound {
		http.NotFound(w, r)
		return
	} else if err != nil {
		s.logger.Error("Could not read tag key via FSM", "tag", tag, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := struct {
		Data []string `json:"data"`
	}{Data: value}

	if err := json.NewEncoder(w).Encode(rsp); err != nil {
		s.logger.Error("Could not encode response for tag key", "tag", tag, "error", err)
	}
}

func (s *Service) iterateKeysByPrefixHandler(w http.ResponseWriter, r *http.Request) {

	if !s.validateToken(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	if prefix == "" {
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

	value, err := s.fsm.Iterate(prefix, offsetInt, limitInt)
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

	if !s.validateToken(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	value, err := s.fsm.GetCache(key)
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
