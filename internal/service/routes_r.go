package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/dgraph-io/badger/v3"
)

func (s *Service) getHandler(w http.ResponseWriter, r *http.Request) {

	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("GetHandler", "entity", entity)

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	value, err := s.fsm.Get(fmt.Sprintf("%s:%s", uuid, key))
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

	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("IterateKeysByPrefixHandler", "entity", entity)

	prefix := r.URL.Query().Get("prefix")
	if entity != EntityRoot && prefix == "" {
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

	value, err := s.fsm.Iterate(fmt.Sprintf("%s:%s", uuid, prefix), offsetInt, limitInt)
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

	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("GetCacheHandler", "entity", entity)

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	value, err := s.fsm.GetCache(fmt.Sprintf("%s:%s", uuid, key))
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

/*
	Handlers for OBJECTS
*/

func (s *Service) getObjectHandler(w http.ResponseWriter, r *http.Request) {
	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("GetObjectHandler", "entity", entity)

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	prefixedKey := fmt.Sprintf("%s:%s", uuid, key)
	objectData, err := s.fsm.GetObject(prefixedKey)
	if err != nil {
		// Check if it's a tkv.ErrKeyNotFound specifically if you want to return 404
		// For now, treating all errors from GetObject as potential internal errors or not found.
		s.logger.Info("FSM GetObject for key returned error", "key", prefixedKey, "error", err)
		// Consider checking for specific error types like tkv.ErrKeyNotFound
		// if errors.As(err, &tkv.ErrKeyNotFound{}) { ... }
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, errWrite := w.Write(objectData)
	if errWrite != nil {
		s.logger.Error("Could not write object data to response", "key", prefixedKey, "error", errWrite)
		// Client has likely already received a 200 OK, so we can't change status here
	}
}

func (s *Service) getObjectListHandler(w http.ResponseWriter, r *http.Request) {
	entity, uuid, ok := s.validateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("GetObjectListHandler", "entity", entity)

	prefix := r.URL.Query().Get("prefix")
	// Unlike IterateKeysByPrefixHandler, an empty prefix for objects might be valid to list all objects for the UUID.
	// if entity != EntityRoot && prefix == "" { // Allow empty prefix for non-root to list all for that uuid
	// 	http.Error(w, "Missing prefix parameter for non-root entity", http.StatusBadRequest)
	// 	return
	// }

	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	offsetInt, err := strconv.Atoi(offsetStr)
	if err != nil && offsetStr != "" { // only error if offset was provided but invalid
		http.Error(w, "Invalid offset parameter", http.StatusBadRequest)
		return
	}
	if offsetStr == "" {
		offsetInt = 0 // Default offset
	}

	limitInt, err := strconv.Atoi(limitStr)
	if err != nil && limitStr != "" { // only error if limit was provided but invalid
		http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
		return
	}
	if limitStr == "" || limitInt <= 0 {
		limitInt = 100 // Default limit or if invalid limit provided
	}

	if offsetInt < 0 {
		offsetInt = 0 // Correct negative offset
	}

	// The FSM's GetObjectList expects the raw prefix; the UUID prefixing is done inside the FSM or TKV layer usually.
	// However, for consistency with other handlers that prepend UUID, we do it here.
	// If GetObjectList in FSM/TKV is already designed to handle UUID-based multi-tenancy by itself with the given prefix, then this might be double prefixing.
	// Assuming GetObjectList in FSM takes the user-provided prefix and internally handles UUID scoping if necessary.
	// For this example, we'll pass the user's prefix directly, and append the UUID, similar to other handlers.
	uuidPrefixedKey := fmt.Sprintf("%s:%s", uuid, prefix)

	keys, err := s.fsm.GetObjectList(uuidPrefixedKey, offsetInt, limitInt)
	if err != nil {
		// Check for specific errors like tkv.ErrKeyNotFound if that's a possible/distinct outcome
		s.logger.Error("Could not list objects via FSM", "prefix", uuidPrefixedKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if keys == nil {
		keys = []string{} // Ensure a non-null JSON array `[]` instead of `null`
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := struct {
		Data []string `json:"data"`
	}{Data: keys}

	if err := json.NewEncoder(w).Encode(rsp); err != nil {
		s.logger.Error("Could not encode response for object list", "prefix", uuidPrefixedKey, "error", err)
	}
}
