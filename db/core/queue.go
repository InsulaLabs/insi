package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/db/tkv"
)

// -- READ OPERATIONS --

// -- WRITE OPERATIONS --

func (c *Core) queueNewHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	c.logger.Debug("QueueNewHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for queue new request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.QueueNewRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		c.logger.Error("Invalid JSON payload for queue new request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Missing key in queue new request payload", http.StatusBadRequest)
		return
	}

	prefixedKey := fmt.Sprintf("%s:%s", td.UUID, req.Key)
	// Consider if sizeTooLargeForStorage is relevant for queue keys, though they are in-memory.
	// For now, we assume key length constraints are less critical than for Badger stored keys.

	if err := c.fsm.QueueNew(prefixedKey); err != nil {
		// QueueNew in TKV is idempotent, so specific errors like ErrKeyExists are not expected from TKV layer.
		// Any error here would likely be an internal FSM or Raft error.
		c.logger.Error("Could not perform QueueNewRaft via FSM", "key", prefixedKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Core) queuePushHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	c.logger.Debug("QueuePushHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for queue push request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.QueuePushRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		c.logger.Error("Invalid JSON payload for queue push request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Missing key in queue push request payload", http.StatusBadRequest)
		return
	}
	// req.Value can be empty, that's a valid item to push.

	prefixedKey := fmt.Sprintf("%s:%s", td.UUID, req.Key)
	// Value size for in-memory queues might also have practical limits, but not enforced via sizeTooLargeForStorage yet.

	newLength, err := c.fsm.QueuePush(prefixedKey, req.Value)
	if err != nil {
		var qnfErr *tkv.ErrQueueNotFound
		if errors.As(err, &qnfErr) {
			c.logger.Warn("QueuePush failed because queue does not exist", "key", prefixedKey, "error", err)
			http.Error(w, err.Error(), http.StatusNotFound) // 404 Not Found
			return
		}
		// Other errors are likely internal.
		c.logger.Error("Could not perform QueuePushRaft via FSM", "key", prefixedKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	resp := models.QueuePushResponse{
		Key:       req.Key, // Return original non-prefixed key
		NewLength: newLength,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		c.logger.Error("Failed to encode QueuePushResponse", "error", err)
	}
}

func (c *Core) queuePopHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	c.logger.Debug("QueuePopHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for queue pop request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.QueueKeyPayload // Pop only needs the key
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		c.logger.Error("Invalid JSON payload for queue pop request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Missing key in queue pop request payload", http.StatusBadRequest)
		return
	}

	prefixedKey := fmt.Sprintf("%s:%s", td.UUID, req.Key)

	poppedValue, err := c.fsm.QueuePop(prefixedKey)
	if err != nil {
		var qnfErr *tkv.ErrQueueNotFound
		var qeErr *tkv.ErrQueueEmpty
		if errors.As(err, &qnfErr) {
			c.logger.Warn("QueuePop failed because queue does not exist", "key", prefixedKey, "error", err)
			http.Error(w, err.Error(), http.StatusNotFound) // 404 Not Found
			return
		} else if errors.As(err, &qeErr) {
			c.logger.Info("QueuePop on empty queue", "key", prefixedKey, "error", err)
			// Depending on desired behavior, could return 404 or a specific code for empty.
			// Let's use 404 for consistency with queue not found, client can differentiate by error message if needed.
			http.Error(w, err.Error(), http.StatusNotFound) // Or http.StatusConflict (409) if more appropriate
			return
		}
		c.logger.Error("Could not perform QueuePopRaft via FSM", "key", prefixedKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	resp := models.QueuePopResponse{
		Key:   req.Key, // Return original non-prefixed key
		Value: poppedValue,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		c.logger.Error("Failed to encode QueuePopResponse", "error", err)
	}
}

func (c *Core) queueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := c.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	c.logger.Debug("QueueDeleteHandler", "entity", td.Entity)

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for queue delete request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.QueueDeleteRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		c.logger.Error("Invalid JSON payload for queue delete request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Missing key in queue delete request payload", http.StatusBadRequest)
		return
	}

	prefixedKey := fmt.Sprintf("%s:%s", td.UUID, req.Key)

	if err := c.fsm.QueueDelete(prefixedKey); err != nil {
		// QueueDelete in TKV is idempotent, so specific errors like ErrKeyNotFound are not expected from TKV layer.
		c.logger.Error("Could not perform QueueDeleteRaft via FSM", "key", prefixedKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
