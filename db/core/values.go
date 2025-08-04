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

	c.IndVSOp()

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
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
		http.NotFound(w, r)
		return
	}
	if value == "" {
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

	c.IndVSOp()

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
		return
	}

	c.logger.Debug("IterateKeysByPrefixHandler", "entity", td.Entity)

	prefix := r.URL.Query().Get("prefix")
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

	prefix = strings.TrimSuffix(prefix, "*")
	fullPrefix := fmt.Sprintf("%s:", td.DataScopeUUID)
	if prefix != "" {
		fullPrefix = fmt.Sprintf("%s:%s", td.DataScopeUUID, prefix)
	}

	value, err := c.fsm.Iterate(fullPrefix, offsetInt, limitInt, fmt.Sprintf("%s:", td.DataScopeUUID))
	if err == badger.ErrKeyNotFound {
		http.NotFound(w, r)
		return
	} else if err != nil {
		c.logger.Error("Could not iterate keys by prefix via FSM", "prefix", prefix, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
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

	c.IndVSOp()

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
		return
	}

	c.logger.Debug("SetHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.RequestURI(), rcPublic)
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
		if !errors.As(err, &nfErr) {
			c.logger.Error("Could not get existing value for value", "error", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		exists = false
	} else {
		exists = true
	}

	var delta int64
	if exists {
		delta = int64(len(p.Value) - len(existingValue))
	} else {
		delta = int64(len(p.Value))
	}

	ok, current, limit := c.CheckDiskUsage(td, delta)
	if !ok {
		w.Header().Set("X-Current-Disk-Usage", current)
		w.Header().Set("X-Disk-Usage-Limit", limit)
		http.Error(w, "Disk usage limit exceeded", http.StatusBadRequest)
		return
	}

	err = c.fsm.Set(p)
	if err != nil {
		c.logger.Error("Could not write key-value via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if err := c.AssignBytesToTD(td, StorageTargetDisk, delta); err != nil {
		c.logger.Error("could not assign bytes to td for disk", "error", err)
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Core) deleteHandler(w http.ResponseWriter, r *http.Request) {

	c.IndVSOp()

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
		return
	}

	c.logger.Debug("DeleteHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.RequestURI(), rcPublic)
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

	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	existingValue, err := c.fsm.Get(p.Key)
	if err != nil {
		var nfErr *tkv.ErrKeyNotFound
		if errors.As(err, &nfErr) {
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

	size := len(existingValue)
	if err := c.AssignBytesToTD(td, StorageTargetDisk, -int64(size)); err != nil {
		c.logger.Error("Could not bump integer via FSM for disk usage on delete", "error", err)
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Core) setNXHandler(w http.ResponseWriter, r *http.Request) {

	c.IndVSOp()

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
		return
	}

	c.logger.Debug("SetNXHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.RequestURI(), rcPublic)
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

	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if sizeTooLargeForStorage(p.Value) {
		http.Error(w, "Value is too large", http.StatusBadRequest)
		return
	}
	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	ok, current, limit := c.CheckDiskUsage(td, int64(len(p.Value)))
	if !ok {
		w.Header().Set("X-Current-Disk-Usage", current)
		w.Header().Set("X-Disk-Usage-Limit", limit)
		http.Error(w, "Disk usage limit exceeded", http.StatusBadRequest)
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

	if err := c.AssignBytesToTD(td, StorageTargetDisk, int64(len(p.Value))); err != nil {
		c.logger.Error("could not assign bytes to td for disk on setnx", "error", err)
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Core) compareAndSwapHandler(w http.ResponseWriter, r *http.Request) {

	c.IndVSOp()

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
		return
	}

	c.logger.Debug("CompareAndSwapHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.RequestURI(), rcPublic)
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

	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if sizeTooLargeForStorage(p.NewValue) {
		http.Error(w, "Value is too large", http.StatusBadRequest)
		return
	}
	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	existingValue, err := c.fsm.Get(p.Key)
	if err != nil {
		var nfErr *tkv.ErrKeyNotFound
		if errors.As(err, &nfErr) {
			http.Error(w, "key does not exist", http.StatusPreconditionFailed)
			return
		}
		c.logger.Error("Could not get existing value for CAS", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if existingValue != p.OldValue {
		http.Error(w, "conflict: value has changed", http.StatusConflict)
		return
	}

	delta := int64(len(p.NewValue) - len(existingValue))
	ok, current, limit := c.CheckDiskUsage(td, delta)
	if !ok {
		w.Header().Set("X-Current-Disk-Usage", current)
		w.Header().Set("X-Disk-Usage-Limit", limit)
		http.Error(w, "Disk usage limit exceeded", http.StatusBadRequest)
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

	if err := c.AssignBytesToTD(td, StorageTargetDisk, delta); err != nil {
		c.logger.Error("could not assign bytes to td for disk on cas", "error", err)
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Core) bumpHandler(w http.ResponseWriter, r *http.Request) {

	c.IndVSOp()

	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
		return
	}

	c.logger.Debug("BumpHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.RequestURI(), rcPublic)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for bump request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KVPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		c.logger.Error("Invalid JSON payload for bump request", "error", err)
		http.Error(w, "Invalid JSON payload for bump: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in bump request payload", http.StatusBadRequest)
		return
	}

	p.Key = fmt.Sprintf("%s:%s", td.DataScopeUUID, p.Key)

	if sizeTooLargeForStorage(p.Value) {
		http.Error(w, "Value is too large", http.StatusBadRequest)
		return
	}

	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	existingValue, err := c.fsm.Get(p.Key)
	if err != nil {
		var nfErr *tkv.ErrKeyNotFound
		if !errors.As(err, &nfErr) {
			c.logger.Error("Could not get existing value for value", "error", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		existingValue = "0"
	}

	existingValueInt, err := strconv.ParseInt(existingValue, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse existing value for bump. The value stored is not an integer.", "key", p.Key, "value", existingValue, "error", err)
		http.Error(w, "The existing value is not a valid integer, cannot bump.", http.StatusBadRequest)
		return
	}

	increment, err := strconv.ParseInt(p.Value, 10, 64)
	if err != nil {
		c.logger.Error("Could not parse value for bump. The provided value is not an integer.", "value", p.Value, "error", err)
		http.Error(w, "The value to bump by must be a valid integer.", http.StatusBadRequest)
		return
	}

	newValueStr := strconv.FormatInt(existingValueInt+increment, 10)
	delta := int64(len(newValueStr) - len(existingValue))

	ok, current, limit := c.CheckDiskUsage(td, delta)
	if !ok {
		w.Header().Set("X-Current-Disk-Usage", current)
		w.Header().Set("X-Disk-Usage-Limit", limit)
		http.Error(w, "Disk usage limit exceeded", http.StatusBadRequest)
		return
	}

	err = c.fsm.BumpInteger(p.Key, int(increment))
	if err != nil {
		c.logger.Error("Could not write key-value via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if err := c.AssignBytesToTD(td, StorageTargetDisk, delta); err != nil {
		c.logger.Error("could not assign bytes to td for disk", "error", err)
	}

	w.WriteHeader(http.StatusOK)
}
