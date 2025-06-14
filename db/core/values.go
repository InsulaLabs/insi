package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/db/tkv"
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

// -- READ OPERATIONS --

func (c *Core) getHandler(w http.ResponseWriter, r *http.Request) {

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	c.logger.Debug("GetHandler", "entity", td.Entity)

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	value, err := c.fsm.Get(fmt.Sprintf("%s:%s", td.DataScopeUUID, key))
	if err != nil {
		c.logger.Info("FSM Get for key returned error, treating as Not Found for now", "key", key, "error", err)
		http.NotFound(w, r)
		return
	}
	if value == "" {
		c.logger.Info("FSM Get for key returned empty value, treating as Not Found", "key", key)
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := struct {
		Data string `json:"data"`
	}{Data: value}

	if err := json.NewEncoder(w).Encode(rsp); err != nil {
		c.logger.Error("Could not encode response for key", "key", key, "error", err)
	}
}

func (c *Core) iterateKeysByPrefixHandler(w http.ResponseWriter, r *http.Request) {

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	c.logger.Debug("IterateKeysByPrefixHandler", "entity", td.Entity)

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

	value, err := c.fsm.Iterate(fmt.Sprintf("%s:%s", td.DataScopeUUID, prefix), offsetInt, limitInt)
	if err == badger.ErrKeyNotFound {
		http.NotFound(w, r)
		return
	} else if err != nil {
		c.logger.Error("Could not iterate keys by prefix via FSM", "prefix", prefix, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// All keys come back with the api key unique prefix so we need to remove it
	for i, key := range value {
		value[i] = strings.TrimPrefix(key, fmt.Sprintf("%s:", td.DataScopeUUID))
	}

	w.Header().Set("Content-Type", "application/json")
	rsp := struct {
		Data []string `json:"data"`
	}{Data: value}

	if err := json.NewEncoder(w).Encode(rsp); err != nil {
		c.logger.Error("Could not encode response for iterate keys by prefix", "prefix", prefix, "error", err)
	}
}

// -- WRITE OPERATIONS --

/*
	Handlers that update the "VALUES" database
*/

func (c *Core) setHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	c.logger.Debug("SetHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for set request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KVPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		c.logger.Error("Invalid JSON payload for set request", "error", err)
		http.Error(w, "Invalid JSON payload for set: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in set request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if sizeTooLargeForStorage(p.Value) {
		http.Error(w, "Value is too large", http.StatusBadRequest)
		return
	}

	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	exists := false
	existingValue, err := c.fsm.Get(p.Key)
	if err != nil {
		var nfErr *tkv.ErrKeyNotFound
		// We only care about key not found, other errors should be returned.
		if !errors.As(err, &nfErr) {
			c.logger.Error("Could not get existing value for value", "error", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		exists = false
	} else {
		exists = true
	}

	err = c.fsm.Set(p)
	if err != nil {
		c.logger.Error("Could not write key-value via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if !exists {
		if err := c.fsm.BumpInteger(WithApiKeyDiskUsage(td.KeyUUID), p.TotalLength()); err != nil {
			c.logger.Error("Could not bump integer via FSM for disk usage", "error", err)
			// continue on, we don't want to block the request on this
		}
	} else {
		if err := c.fsm.BumpInteger(
			WithApiKeyDiskUsage(td.KeyUUID),
			CalculateDelta(models.KVPayload{Key: p.Key, Value: existingValue}, p)); err != nil {
			c.logger.Error("Could not bump integer via FSM for disk usage", "error", err)
			// continue on, we don't want to block the request on this
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Core) deleteHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	c.logger.Debug("DeleteHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for delete request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KVPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		c.logger.Error("Invalid JSON payload for unset request", "error", err)
		http.Error(w, "Invalid JSON payload for unset: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in unset request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	existingValue, err := c.fsm.Get(p.Key)
	if err != nil {
		var nfErr *tkv.ErrKeyNotFound
		if errors.As(err, &nfErr) {
			// Key doesn't exist, so the delete is a no-op.
			w.WriteHeader(http.StatusOK)
			return
		}
		c.logger.Error("Could not get existing value for value deletion", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	err = c.fsm.Delete(p.Key)
	if err != nil {
		c.logger.Error("Could not unset value via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Decrement memory usage
	size := len(p.Key) + len(existingValue)
	if err := c.fsm.BumpInteger(WithApiKeyDiskUsage(td.KeyUUID), -size); err != nil {
		c.logger.Error("Could not bump integer via FSM for disk usage on delete", "error", err)
		// Don't fail the whole request, but log it.
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Core) setNXHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	c.logger.Debug("SetNXHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for setnx request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KVPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		c.logger.Error("Invalid JSON payload for setnx request", "error", err)
		http.Error(w, "Invalid JSON payload for setnx: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in setnx request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if sizeTooLargeForStorage(p.Value) {
		http.Error(w, "Value is too large", http.StatusBadRequest)
		return
	}
	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	err = c.fsm.SetNX(p)
	if err != nil {
		var keyExistsErr *tkv.ErrKeyExists
		if errors.As(err, &keyExistsErr) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		c.logger.Error("Could not write key-value via FSM on SetNX", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Core) compareAndSwapHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	c.logger.Debug("CompareAndSwapHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for cas request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.CASPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		c.logger.Error("Invalid JSON payload for cas request", "error", err)
		http.Error(w, "Invalid JSON payload for cas: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in cas request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if sizeTooLargeForStorage(p.NewValue) {
		http.Error(w, "Value is too large", http.StatusBadRequest)
		return
	}
	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	err = c.fsm.CompareAndSwap(p)
	if err != nil {
		var casFailedErr *tkv.ErrCASFailed
		if errors.As(err, &casFailedErr) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		c.logger.Error("Could not write key-value via FSM on CAS", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
