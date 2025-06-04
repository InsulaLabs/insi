package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/InsulaLabs/insi/models"
	"github.com/dgraph-io/badger/v3"
)

const maxBatchItems = 1000                       // Limit the number of items in a single batch
const maxTotalBatchPayloadSize = 1 * 1024 * 1024 // 1MB limit for the entire batch JSON payload

// -- READ OPERATIONS --

func (s *Service) getHandler(w http.ResponseWriter, r *http.Request) {

	td, ok := s.ValidateToken(r, false)
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

	td, ok := s.ValidateToken(r, false)
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

	// All keys come back with the api key unique prefix so we need to remove it
	for i, key := range value {
		value[i] = strings.TrimPrefix(key, fmt.Sprintf("%s:", td.UUID))
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := struct {
		Data []string `json:"data"`
	}{Data: value}

	if err := json.NewEncoder(w).Encode(rsp); err != nil {
		s.logger.Error("Could not encode response for iterate keys by prefix", "prefix", prefix, "error", err)
	}
}

// -- WRITE OPERATIONS --

/*
	Handlers that update the "VALUES" database
*/

func (s *Service) setHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("SetHandler", "entity", td.Entity)

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
	p.Key = fmt.Sprintf("%s:%s", td.UUID, p.Key)

	if sizeTooLargeForStorage(p.Value) {
		http.Error(w, "Value is too large", http.StatusBadRequest)
		return
	}

	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	err = s.fsm.Set(p)
	if err != nil {
		s.logger.Error("Could not write key-value via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) deleteHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("DeleteHandler", "entity", td.Entity)

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
	p.Key = fmt.Sprintf("%s:%s", td.UUID, p.Key)

	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	err = s.fsm.Delete(p.Key)
	if err != nil {
		s.logger.Error("Could not unset value via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

/*
	Handlers for BATCH VALUE operations
*/

// Define request structures for batch operations

func (s *Service) batchSetHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("BatchSetHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxTotalBatchPayloadSize)
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for batch set request", "error", err)
		if errors.As(err, new(*http.MaxBytesError)) {
			http.Error(w, "Request payload too large for batch set", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.BatchSetRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Invalid JSON payload for batch set request", "error", err)
		http.Error(w, "Invalid JSON payload for batch set: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Items) == 0 {
		http.Error(w, "No items in batch set request", http.StatusBadRequest)
		return
	}
	if len(req.Items) > maxBatchItems {
		http.Error(w, fmt.Sprintf("Too many items in batch set request. Max allowed: %d", maxBatchItems), http.StatusBadRequest)
		return
	}

	// Prefix keys and validate individual items
	// The FSM's BatchSet expects []models.KVPayload directly
	itemsToSet := make([]models.KVPayload, len(req.Items))
	for i, item := range req.Items {
		if item.Key == "" {
			http.Error(w, fmt.Sprintf("Item at index %d has an empty key", i), http.StatusBadRequest)
			return
		}
		prefixedKey := fmt.Sprintf("%s:%s", td.UUID, item.Key)
		if sizeTooLargeForStorage(prefixedKey) {
			http.Error(w, fmt.Sprintf("Prefixed key '%s' (from item at index %d) is too large", prefixedKey, i), http.StatusBadRequest)
			return
		}
		if sizeTooLargeForStorage(item.Value) {
			http.Error(w, fmt.Sprintf("Value for key '%s' (from item at index %d) is too large", item.Key, i), http.StatusBadRequest)
			return
		}
		itemsToSet[i] = models.KVPayload{Key: prefixedKey, Value: item.Value}
	}

	if err := s.fsm.BatchSet(itemsToSet); err != nil {
		s.logger.Error("Could not batch set values via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) batchDeleteHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("BatchDeleteHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxTotalBatchPayloadSize)
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for batch delete request", "error", err)
		if errors.As(err, new(*http.MaxBytesError)) {
			http.Error(w, "Request payload too large for batch delete", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.BatchDeleteRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Invalid JSON payload for batch delete request", "error", err)
		http.Error(w, "Invalid JSON payload for batch delete: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Keys) == 0 {
		http.Error(w, "No keys in batch delete request", http.StatusBadRequest)
		return
	}
	if len(req.Keys) > maxBatchItems {
		http.Error(w, fmt.Sprintf("Too many keys in batch delete request. Max allowed: %d", maxBatchItems), http.StatusBadRequest)
		return
	}

	// Prefix keys and convert to []models.KeyPayload for FSM
	keysToDeleteFSM := make([]models.KeyPayload, len(req.Keys))
	for i, key := range req.Keys {
		if key == "" {
			http.Error(w, fmt.Sprintf("Key at index %d is empty", i), http.StatusBadRequest)
			return
		}
		prefixedKey := fmt.Sprintf("%s:%s", td.UUID, key)
		if sizeTooLargeForStorage(prefixedKey) {
			http.Error(w, fmt.Sprintf("Prefixed key '%s' (from key at index %d) is too large", prefixedKey, i), http.StatusBadRequest)
			return
		}
		keysToDeleteFSM[i] = models.KeyPayload{Key: prefixedKey}
	}

	if err := s.fsm.BatchDelete(keysToDeleteFSM); err != nil {
		s.logger.Error("Could not batch delete values via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
