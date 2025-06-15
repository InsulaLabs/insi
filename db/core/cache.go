package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/db/tkv"
)

// -- READ OPERATIONS --

func (c *Core) getCacheHandler(w http.ResponseWriter, r *http.Request) {

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	c.logger.Debug("GetCacheHandler", "entity", td.Entity)

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	value, err := c.fsm.GetCache(fmt.Sprintf("%s:%s", td.DataScopeUUID, key))
	if err != nil {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := struct {
		Data string `json:"data"`
	}{Data: value}

	if errEnc := json.NewEncoder(w).Encode(rsp); errEnc != nil {
		c.logger.Error("Could not encode response for cache", "key", key, "error", errEnc)
		return
	}
}

// -- WRITE OPERATIONS --

func (c *Core) setCacheHandler(w http.ResponseWriter, r *http.Request) {

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	c.logger.Debug("SetCacheHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for set cache request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KVPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		c.logger.Error("Invalid JSON payload for set cache request", "error", err)
		http.Error(w, "Invalid JSON payload for set cache: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in set cache request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	if sizeTooLargeForStorage(p.Value) {
		http.Error(w, "Value is too large", http.StatusBadRequest)
		return
	}

	exists := false
	existingValue, err := c.fsm.GetCache(p.Key)
	if err != nil {
		if !tkv.IsErrKeyNotFound(err) {
			c.logger.Error("Could not get existing value for cache", "error", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		exists = false
	} else {
		exists = true
	}

	// Get the limit for memory usage
	limit, err := c.fsm.Get(WithApiKeyMaxMemoryUsage(td.KeyUUID))
	if err != nil {
		c.logger.Error("Could not get limit for memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	limitInt, err := strconv.ParseInt(limit, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse limit for memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// get the current memory usage
	currentMemoryUsage, err := c.fsm.Get(WithApiKeyMemoryUsage(td.KeyUUID))
	if err != nil {
		c.logger.Error("Could not get current memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	currentMemoryUsageInt, err := strconv.ParseInt(currentMemoryUsage, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse current memory usage", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if currentMemoryUsageInt+int64(p.TotalLength()) > limitInt {
		// Add headers to show them their current memory usage and the limit
		w.Header().Set("X-Current-Memory-Usage", currentMemoryUsage)
		w.Header().Set("X-Memory-Usage-Limit", limit)
		http.Error(w, "Memory usage limit exceeded", http.StatusBadRequest)
		return
	}

	// set the cache

	err = c.fsm.SetCache(p)
	if err != nil {
		c.logger.Error("Could not set cache via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if !exists {
		if err := c.fsm.BumpInteger(WithApiKeyMemoryUsage(td.KeyUUID), p.TotalLength()); err != nil {
			c.logger.Error("Could not bump integer via FSM for memory usage", "error", err)
			// continue on, we don't want to block the request on this
		}
	} else {
		if err := c.fsm.BumpInteger(
			WithApiKeyMemoryUsage(td.KeyUUID),
			CalculateDelta(models.KVPayload{Key: p.Key, Value: existingValue}, p)); err != nil {
			c.logger.Error("Could not bump integer via FSM for memory usage", "error", err)
			// continue on, we don't want to block the request on this
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Core) deleteCacheHandler(w http.ResponseWriter, r *http.Request) {

	/*

		NOTE: Due to limiting restrictions, caches have been set
			  to root-only operations

	*/
	td, ok := s.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("DeleteCacheHandler", "entity", td.Entity)

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
	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	existingValue, err := s.fsm.GetCache(p.Key)
	if err != nil {
		if tkv.IsErrKeyNotFound(err) {
			w.WriteHeader(http.StatusOK)
			return
		}
		s.logger.Error("Could not get existing value for cache deletion", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	err = s.fsm.DeleteCache(p.Key)
	if err != nil {
		s.logger.Error("Could not delete cache via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Decrement memory usage
	size := len(p.Key) + len(existingValue)
	if err := s.fsm.BumpInteger(WithApiKeyMemoryUsage(td.KeyUUID), -size); err != nil {
		s.logger.Error("Could not bump integer via FSM for memory usage on delete", "error", err)
		// Don't fail the whole request, but log it.
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Core) setCacheNXHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for set cache nx request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KVPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		http.Error(w, "Invalid JSON payload for set cache nx: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in set cache nx request payload", http.StatusBadRequest)
		return
	}

	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if err := c.fsm.SetCacheNX(p); err != nil {
		var keyExistsErr *tkv.ErrKeyExists
		if errors.As(err, &keyExistsErr) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		c.logger.Error("Could not set cache nx via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Core) compareAndSwapCacheHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.CASPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		http.Error(w, "Invalid JSON payload for cache cas: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in cache cas request payload", http.StatusBadRequest)
		return
	}

	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if err := c.fsm.CompareAndSwapCache(p); err != nil {
		var casFailedErr *tkv.ErrCASFailed
		if errors.As(err, &casFailedErr) {
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
			return
		}
		c.logger.Error("Could not cas cache via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func parseOffsetAndLimit(r *http.Request) (int, int) {
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	offset, err := strconv.Atoi(offsetStr)
	if err != nil || offset < 0 {
		offset = 0
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 100 // Default limit
	}
	if limit > 1000 { // Max limit
		limit = 1000
	}
	return offset, limit
}

func (c *Core) iterateCacheKeysByPrefixHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	offset, limit := parseOffsetAndLimit(r)

	fullPrefix := fmt.Sprintf("%s:%s", td.DataScopeUUID, prefix)

	keys, err := c.fsm.IterateCache(fullPrefix, offset, limit)
	if err != nil {
		http.Error(w, "Failed to iterate cache keys", http.StatusInternalServerError)
		return
	}

	// Trim the internal prefix from the keys before returning them
	prefixToTrim := td.DataScopeUUID + ":"
	trimmedKeys := make([]string, len(keys))
	for i, key := range keys {
		trimmedKeys[i] = strings.TrimPrefix(key, prefixToTrim)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(trimmedKeys); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}
