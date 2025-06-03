package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	// "net" // No longer needed here if LeaderHTTPAddress provides the correct host:port
	"net/http"

	"github.com/InsulaLabs/insi/internal/tkv"
	"github.com/InsulaLabs/insi/models"
)

const maxBatchItems = 1000                       // Example: Limit the number of items in a single batch
const maxTotalBatchPayloadSize = 1 * 1024 * 1024 // Example: 1MB limit for the entire batch JSON payload

// Badger limits on key and value sizes are 1MB.
func sizeTooLargeForStorage(value string) bool {
	return len(value) >= 1024*1024
}

// Helper function for redirection
func (s *Service) redirectToLeader(w http.ResponseWriter, r *http.Request, originalPath string) {
	// leaderConnectAddress is now expected to be "host:port"
	// (e.g., "db-0.insula.dev:443" or "134.122.121.148:443" if ClientDomain isn't set for leader)
	// as returned by the updated fsm.LeaderHTTPAddress()
	leaderConnectAddress, err := s.fsm.LeaderHTTPAddress()
	if err != nil {
		s.logger.Error("Failed to get leader's connect address for redirection", "original_path", originalPath, "error", err)
		http.Error(w, "Failed to determine cluster leader for redirection: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	// Construct the absolute redirect URL, always using HTTPS for production security.
	// originalPath should already include the leading "/" (e.g., "/db/api/v1/set")
	redirectURL := "https://" + leaderConnectAddress + originalPath
	if r.URL.RawQuery != "" {
		redirectURL += "?" + r.URL.RawQuery
	}

	s.logger.Info("Issuing redirect to leader",
		"current_node_is_follower", true,
		"leader_connect_address_from_fsm", leaderConnectAddress,
		"final_redirect_url", redirectURL)

	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect) // Client follows this Location header
}

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

func (s *Service) setCacheHandler(w http.ResponseWriter, r *http.Request) {

	/*

		NOTE: Due to limiting restrictions, caches have been set
			  to root-only operations

	*/
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("SetCacheHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}
	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for set cache request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.CachePayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for set cache request", "error", err)
		http.Error(w, "Invalid JSON payload for set cache: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.Key == "" {
		http.Error(w, "Missing key in set cache request payload", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	p.Key = fmt.Sprintf("%s:%s", td.UUID, p.Key)

	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	if sizeTooLargeForStorage(p.Value) {
		http.Error(w, "Value is too large", http.StatusBadRequest)
		return
	}

	err = s.fsm.SetCache(p)
	if err != nil {
		s.logger.Error("Could not set cache via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) deleteCacheHandler(w http.ResponseWriter, r *http.Request) {

	/*

		NOTE: Due to limiting restrictions, caches have been set
			  to root-only operations

	*/
	td, ok := s.ValidateToken(r, false)
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
	p.Key = fmt.Sprintf("%s:%s", td.UUID, p.Key)

	if sizeTooLargeForStorage(p.Key) {
		http.Error(w, "Key is too large", http.StatusBadRequest)
		return
	}

	err = s.fsm.DeleteCache(p.Key)
	if err != nil {
		s.logger.Error("Could not delete cache via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) eventsHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	fmt.Println("DEV> eventsHandler", td.Entity, td.UUID)

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for events request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.Event
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		s.logger.Error("Invalid JSON payload for events request", "error", err)
		http.Error(w, "Invalid JSON payload for events: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Prefix the topic with the entity's UUID to scope it
	prefixedTopic := fmt.Sprintf("%s:%s", td.UUID, p.Topic)
	s.logger.Debug("Publishing event with prefixed topic", "original_topic", p.Topic, "prefixed_topic", prefixedTopic, "entity_uuid", td.UUID)

	err = s.fsm.Publish(prefixedTopic, p.Data)
	if err != nil {
		s.logger.Error("Could not publish event via FSM", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
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

/*
	Handlers for ATOMIC operations
*/

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

/*
	Handlers for QUEUE operations (In-Memory)
*/

func (s *Service) queueNewHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	s.logger.Debug("QueueNewHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for queue new request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.QueueNewRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Invalid JSON payload for queue new request", "error", err)
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

	if err := s.fsm.QueueNew(prefixedKey); err != nil {
		// QueueNew in TKV is idempotent, so specific errors like ErrKeyExists are not expected from TKV layer.
		// Any error here would likely be an internal FSM or Raft error.
		s.logger.Error("Could not perform QueueNewRaft via FSM", "key", prefixedKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) queuePushHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	s.logger.Debug("QueuePushHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for queue push request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.QueuePushRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Invalid JSON payload for queue push request", "error", err)
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

	newLength, err := s.fsm.QueuePush(prefixedKey, req.Value)
	if err != nil {
		var qnfErr *tkv.ErrQueueNotFound
		if errors.As(err, &qnfErr) {
			s.logger.Warn("QueuePush failed because queue does not exist", "key", prefixedKey, "error", err)
			http.Error(w, err.Error(), http.StatusNotFound) // 404 Not Found
			return
		}
		// Other errors are likely internal.
		s.logger.Error("Could not perform QueuePushRaft via FSM", "key", prefixedKey, "error", err)
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
		s.logger.Error("Failed to encode QueuePushResponse", "error", err)
	}
}

func (s *Service) queuePopHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	s.logger.Debug("QueuePopHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for queue pop request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.QueueKeyPayload // Pop only needs the key
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Invalid JSON payload for queue pop request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Missing key in queue pop request payload", http.StatusBadRequest)
		return
	}

	prefixedKey := fmt.Sprintf("%s:%s", td.UUID, req.Key)

	poppedValue, err := s.fsm.QueuePop(prefixedKey)
	if err != nil {
		var qnfErr *tkv.ErrQueueNotFound
		var qeErr *tkv.ErrQueueEmpty
		if errors.As(err, &qnfErr) {
			s.logger.Warn("QueuePop failed because queue does not exist", "key", prefixedKey, "error", err)
			http.Error(w, err.Error(), http.StatusNotFound) // 404 Not Found
			return
		} else if errors.As(err, &qeErr) {
			s.logger.Info("QueuePop on empty queue", "key", prefixedKey, "error", err)
			// Depending on desired behavior, could return 404 or a specific code for empty.
			// Let's use 404 for consistency with queue not found, client can differentiate by error message if needed.
			http.Error(w, err.Error(), http.StatusNotFound) // Or http.StatusConflict (409) if more appropriate
			return
		}
		s.logger.Error("Could not perform QueuePopRaft via FSM", "key", prefixedKey, "error", err)
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
		s.logger.Error("Failed to encode QueuePopResponse", "error", err)
	}
}

func (s *Service) queueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r, false)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	s.logger.Debug("QueueDeleteHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for queue delete request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var req models.QueueDeleteRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Error("Invalid JSON payload for queue delete request", "error", err)
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Missing key in queue delete request payload", http.StatusBadRequest)
		return
	}

	prefixedKey := fmt.Sprintf("%s:%s", td.UUID, req.Key)

	if err := s.fsm.QueueDelete(prefixedKey); err != nil {
		// QueueDelete in TKV is idempotent, so specific errors like ErrKeyNotFound are not expected from TKV layer.
		s.logger.Error("Could not perform QueueDeleteRaft via FSM", "key", prefixedKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
