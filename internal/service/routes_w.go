package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	// "net" // No longer needed here if LeaderHTTPAddress provides the correct host:port
	"net/http"

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
	td, ok := s.ValidateToken(r)
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
	td, ok := s.ValidateToken(r)
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
	td, ok := s.ValidateToken(r)
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
	td, ok := s.ValidateToken(r)
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

	td, ok := s.ValidateToken(r)
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
	Handlers for OBJECTS
*/

const maxObjectSize = 5 * 1024 * 1024 // 16 MB

// root only
func (s *Service) setObjectHandler(w http.ResponseWriter, r *http.Request) {

	/*

		NOTE:
		    Larger objects sent over raft will have a higher impact
			than regular key-value updates.

			For this reason, only root key can set objects.



	*/
	td, ok := s.ValidateToken(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("SetObjectHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	// Limit the size of the request body to prevent excessively large uploads
	r.Body = http.MaxBytesReader(w, r.Body, maxObjectSize)
	defer r.Body.Close()

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error("Could not read body for set object request", "error", err)
		// Check if the error is due to the body being too large
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			http.Error(w, "Request entity too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	// We expect the client to send a models.ObjectPayload with the key in the JSON body
	// and the raw object data as the value. However, for HTTP, it's more common to get the raw bytes
	// directly from the body if it's purely binary, or use multipart/form-data for metadata + binary.
	// For simplicity here, we'll assume a JSON payload that contains the key, and the value is the raw object data.
	// This means the client needs to base64 encode the object if sending as part of JSON string, or we adjust the model.

	// Let's assume the request body *is* the object, and key is a query param for simplicity with large objects.
	// This deviates from other set handlers but is more practical for binary blobs.
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter for set object request", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	pKey := fmt.Sprintf("%s:%s", td.UUID, key)

	// Note: Size check for objects might be complex as they are chunked.
	// The underlying TKV SetObject will handle chunking. We might want a total size limit here.
	// For now, we rely on TKV's internal handling.

	/*
		NOTE: This root-only operation is not size-limited and will NOT be

	*/
	err = s.fsm.SetObject(pKey, bodyBytes)
	if err != nil {
		s.logger.Error("Could not write object via FSM", "key", pKey, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// root only
func (s *Service) deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	td, ok := s.ValidateToken(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.logger.Debug("DeleteObjectHandler", "entity", td.Entity)

	if !s.fsm.IsLeader() {
		s.redirectToLeader(w, r, r.URL.Path)
		return
	}

	// Expect key in query parameters for DELETE operations on objects
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter for delete object request", http.StatusBadRequest)
		return
	}

	// prefix to lock to the api key holding entity
	pKey := fmt.Sprintf("%s:%s", td.UUID, key)

	err := s.fsm.DeleteObject(pKey)
	if err != nil {
		s.logger.Error("Could not delete object via FSM", "key", pKey, "error", err)
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
	td, ok := s.ValidateToken(r)
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
	td, ok := s.ValidateToken(r)
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
