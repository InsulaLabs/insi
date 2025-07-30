package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/badge"
	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/db/tkv"
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
)

const (
	TopicBlobUploaded = "_:blob:uploaded"
	TopicBlobDeleted  = "_:blob:deleted"

	// idea is to put node identity here and then we can concat the blob id then map to hash. then we can iterate over the node to see if they own the blob
	KeyPrimitiveBlobInstance          = "_:blob:instance:"            // _:blob:instance:node_id:blob_id => record struct { original name, owner uuid, uploaded time, size, hash, etc }
	KeyPrimitiveBlobTombstone         = "_:blob:tombstone:"           // _:blob:tombstone:blob_id => record struct { deleted time, deleted by uuid } ; only first node to receive needs to set to db. then all nodes runners can clean it up.
	KeyPrimitiveBlobTombstoneNodeFlag = "_:blob:tombstone:node_flag:" // _:blob:tombstone:node_flag:blob_id:node_id => timestamp ; last node to delete MUST clean all tombstones. we use node flag to indicate when a node marks complete. if on complete, we iterate over the flags for a tombstone and all nodes are done, delete the tombstone.
	// along with tombstones the last node to delete MUST clean the "instance" record. This will finalize deletion of the blob across the cluster.

	KeyPrefixBlobROS = "_:blob:ros:" // _:blob:ros:node_id

	KeyPrefixNodeIdentityChallenge = "_:node:identity:challenge:" // _:node:identity:challenge:node_id => UUID(random) when connecting to a peer we present our node id, they must return the UUID from the db to confirm they have access to the same level of data.

	TombstoneCycleFrequency = time.Second * 30

	MaxBlobDownloadRetries = 5
)

func blobMetadataKey(dataScopeUUID, key string) string {
	return fmt.Sprintf("blob:%s:%s", dataScopeUUID, key)
}

func (c *Core) blobPath(dataScopeUUID, key string) string {
	// Hash the key to avoid issues with special characters in file names
	hasher := sha256.New()
	hasher.Write([]byte(key))
	keyHash := hex.EncodeToString(hasher.Sum(nil))
	return filepath.Join(c.cfg.InsidHome, c.nodeName, "blobs", dataScopeUUID, keyHash)
}

type PeerChallengeRequest struct {
	UUID              string
	RequesterIdentity string // challenger
	TargetIdentity    string // challengee
}

type PeerChallengeResponse struct {
	NodeId string
}

type peer struct {
	binding        string
	nodeId         string
	lastSeen       time.Time // last time we got a hearbeat FROM them
	lastChallenged time.Time // last time they proved data access to us
	endpoint       client.Endpoint
}

type blobService struct {
	logger     *slog.Logger
	insiClient *client.Client
	identity   badge.Badge // this nodes crypto identity
	peers      []peer
	core       *Core
}

func newBlobService(logger *slog.Logger, core *Core, insiClient *client.Client, identity badge.Badge, peers []client.Endpoint) (*blobService, error) {

	peerList := []peer{}

	// Create blob storage directory using the logical node name for consistency
	blobDir := filepath.Join(core.cfg.InsidHome, core.nodeName, "blobs")
	if err := os.MkdirAll(blobDir, 0755); err != nil {
		return nil, fmt.Errorf("could not create blob directory at %s: %w", blobDir, err)
	}

	// Use the node map from the core config to build the peer list.
	// This ensures the `nodeId` is the logical name (e.g., "node0") used in events.
	for nodeName, nodeConfig := range core.cfg.Nodes {
		peerList = append(peerList, peer{
			binding: nodeConfig.PublicBinding,
			nodeId:  nodeName, // Use the logical node name for peer ID
			endpoint: client.Endpoint{
				PublicBinding:  nodeConfig.PublicBinding,
				PrivateBinding: nodeConfig.PrivateBinding,
				ClientDomain:   nodeConfig.ClientDomain,
			},
			lastSeen:       time.Time{},
			lastChallenged: time.Time{},
		})
	}

	return &blobService{
		logger:     logger,
		core:       core,
		insiClient: insiClient,
		identity:   identity,
		peers:      peerList,
	}, nil
}

func (x *blobService) start(ctx context.Context) error {
	x.logger.Info("Starting blob service subsystems")
	go x.startEventSystem(ctx)
	go x.startTombstoneSystem(ctx)
	return nil
}

func (x *blobService) stop() error {
	x.logger.Info("Stopping blob service")
	return nil
}

func (x *blobService) startEventSystem(ctx context.Context) {
	x.logger.Debug("Starting blob event system")

	localNodeRetryTopic := fmt.Sprintf("%s:blob:retry", x.core.nodeName)

	handleBlobRetrieval := func(event models.Event, isRetry bool) {

		x.logger.Debug("Received blob uploaded event")

		eventData, ok := event.Data.(string)
		if !ok {
			x.logger.Error("Could not cast event data to string")
			return
		}

		var blobMeta models.Blob
		if err := json.Unmarshal([]byte(eventData), &blobMeta); err != nil {
			x.logger.Error("Could not unmarshal blob metadata from event", "error", err)
			return
		}

		// If this node is the one that uploaded the blob, do nothing.
		if blobMeta.NodeIdentityUUID == x.identity.GetID() {
			x.logger.Debug("Skipping blob download for self", "blob_key", blobMeta.Key)
			return
		}

		// Check if we already have this blob
		blobDiskPath := x.core.blobPath(blobMeta.DataScopeUUID, blobMeta.Key)
		if _, err := os.Stat(blobDiskPath); err == nil {
			x.logger.Debug("Blob already exists on this node, skipping download", "blob_key", blobMeta.Key)
			// TODO: We should probably verify the hash here and re-download if it's different.
			return
		}

		x.logger.Debug("Downloading blob from peer", "blob_key", blobMeta.Key, "source_node", blobMeta.NodeID)

		var downloadErr error
	RetryLoop:
		for i := 0; i < MaxBlobDownloadRetries; i++ {
			downloadErr = x.downloadBlobFromPeer(ctx, blobMeta)
			if downloadErr == nil {
				x.logger.Debug("Successfully downloaded blob from peer", "blob_key", blobMeta.Key, "source_node", blobMeta.NodeID)
				break // Success
			}
			x.logger.Warn("Failed to download blob from peer, will retry",
				"attempt", i+1,
				"max_retries", MaxBlobDownloadRetries,
				"blob_key", blobMeta.Key,
				"source_node", blobMeta.NodeID,
				"error", downloadErr)

			// Simple linear backoff. Check context to avoid blocking on shutdown.
			select {
			case <-time.After(time.Second * time.Duration(i+1)):
				// continue to next retry
			case <-ctx.Done():
				x.logger.Warn("Context cancelled during blob download retry backoff", "blob_key", blobMeta.Key)
				downloadErr = ctx.Err() // Store context error
				break RetryLoop
			}
		}

		if downloadErr != nil {
			x.logger.Error("Could not download blob from peer after all retries",
				"blob_key", blobMeta.Key,
				"source_node", blobMeta.NodeID,
				"error", downloadErr)

			// Attempt a full retry in 10 seconds if this is not a retry already
			if !isRetry {
				go func() {
					select {
					case <-x.core.appCtx.Done():
						x.logger.Warn("Context cancelled during blob download retry backoff", "blob_key", blobMeta.Key)
					case <-time.After(time.Second * 10):
						x.logger.Debug("Publishing blob download retry event", "blob_key", blobMeta.Key)
						x.core.fsm.Publish(localNodeRetryTopic, eventData)
					}
				}()
			}
		}
	}

	// Only this node will emit this and receive this, its for local node download retries
	x.core.SubscribeInternally(localNodeRetryTopic, func(event models.Event) {
		x.logger.Debug("Received blob retry event")
		handleBlobRetrieval(event, true)
	})

	// This is for replication
	x.core.SubscribeInternally(TopicBlobUploaded, func(event models.Event) {
		handleBlobRetrieval(event, false)
	})

	// This is for deletion
	x.core.SubscribeInternally(TopicBlobDeleted, func(event models.Event) {
		x.logger.Debug("Received blob deleted event")

		eventData, ok := event.Data.(string)
		if !ok {
			x.logger.Error("Could not cast event data to string for deletion")
			return
		}

		var blobMeta models.Blob
		if err := json.Unmarshal([]byte(eventData), &blobMeta); err != nil {
			x.logger.Error("Could not unmarshal blob metadata from deletion event", "error", err)
			return
		}

		// All nodes (including leader) will execute this.
		blobPath := x.core.blobPath(blobMeta.DataScopeUUID, blobMeta.Key)
		if err := os.Remove(blobPath); err != nil && !os.IsNotExist(err) {
			x.logger.Error("Could not delete blob file from disk via event", "path", blobPath, "error", err)
		} else {
			x.logger.Debug("Successfully deleted blob file from disk via event", "path", blobPath)
		}
	})
}

func (x *blobService) downloadBlobFromPeer(ctx context.Context, blobMeta models.Blob) error {

	x.core.IndBlobsOp()

	// Find the peer to download from
	var sourcePeer *peer
	for i, p := range x.peers {
		if p.nodeId == blobMeta.NodeID {
			sourcePeer = &x.peers[i]
			break
		}
	}

	if sourcePeer == nil {
		return fmt.Errorf("could not find peer with node id %s", blobMeta.NodeID)
	}

	// Correctly build the download URL using the client domain for TLS verification,
	// and the port from the private binding.
	_, port, err := net.SplitHostPort(sourcePeer.endpoint.PrivateBinding)
	if err != nil {
		return fmt.Errorf("could not parse private binding for peer %s: %w", sourcePeer.nodeId, err)
	}

	connectHost := sourcePeer.endpoint.ClientDomain
	if connectHost == "" {
		// Fallback for safety, though ClientDomain should always be set in prod.
		host, _, _ := net.SplitHostPort(sourcePeer.endpoint.PrivateBinding)
		connectHost = host
	}

	downloadURL := fmt.Sprintf("https://%s/db/internal/v1/blob/download?key=%s&scope=%s",
		net.JoinHostPort(connectHost, port), blobMeta.Key, blobMeta.DataScopeUUID)

	req, err := http.NewRequestWithContext(
		ctx,
		"GET",
		downloadURL,
		nil,
	)
	if err != nil {
		x.logger.Error("Could not create request to download blob", "error", err, "url", downloadURL)
		return fmt.Errorf("could not create request to download blob: %w", err)
	}
	req.Header.Set("Authorization", x.insiClient.GetApiKey())

	// We MUST use the insi client's internal http client, which is configured
	// to handle the cluster's TLS (e.g., skip verification for self-signed certs).
	// Using http.DefaultClient will fail TLS handshakes.
	resp, err := x.insiClient.GetObjectHttpClient().Do(req)
	if err != nil {
		return fmt.Errorf("could not download blob: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bad status code from peer when downloading blob: %d, body: %s", resp.StatusCode, string(body))
	}

	// Save to a temp file first
	blobDir := filepath.Join(x.core.cfg.InsidHome, x.core.nodeName, "blobs", blobMeta.DataScopeUUID)
	if err := os.MkdirAll(blobDir, 0755); err != nil {
		return fmt.Errorf("could not create blob directory: %w", err)
	}

	tempFile, err := os.CreateTemp(blobDir, "download-*.tmp")
	if err != nil {
		return fmt.Errorf("could not create temp file for blob download: %w", err)
	}
	defer os.Remove(tempFile.Name())

	hasher := sha256.New()
	teeReader := io.TeeReader(resp.Body, hasher)

	_, err = io.Copy(tempFile, teeReader)
	if err != nil {
		return fmt.Errorf("could not write blob to temp file: %w", err)
	}
	tempFile.Close()

	// Verify hash
	downloadedHash := hex.EncodeToString(hasher.Sum(nil))
	if downloadedHash != blobMeta.Hash {
		return fmt.Errorf("downloaded blob hash mismatch: expected %s, got %s", blobMeta.Hash, downloadedHash)
	}

	// Move temp file to final destination
	finalPath := x.core.blobPath(blobMeta.DataScopeUUID, blobMeta.Key)
	if err := os.Rename(tempFile.Name(), finalPath); err != nil {
		return fmt.Errorf("could not move blob to final destination: %w", err)
	}

	return nil
}

func (x *blobService) startTombstoneSystem(ctx context.Context) {
	ticker := time.NewTicker(TombstoneCycleFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !x.core.fsm.IsLeader() {
				continue
			}
			x.logger.Info("Blob tombstone runner executing deletion cycle")
			x.execTombstoneDeletion()
		}
	}
}

func (x *blobService) execTombstoneDeletion() {
	// 1. Iterate over all tombstone keys
	tombstoneKeys, err := x.core.fsm.Iterate(KeyPrimitiveBlobTombstone, 0, BlobMaxTombstoneLimit, "")
	if err != nil {
		x.logger.Error("Could not get blob tombstone keys", "error", err)
		return
	}

	if len(tombstoneKeys) == 0 {
		return
	}

	x.logger.Debug("Found blob tombstones to process", "count", len(tombstoneKeys))

	for _, tombstoneKeyBytes := range tombstoneKeys {
		tombstoneKey := string(tombstoneKeyBytes)
		blobId := strings.TrimPrefix(tombstoneKey, KeyPrimitiveBlobTombstone)
		// blobId is blob:<data_scope_uuid>:<key>
		// We need to parse this to get the parts
		parts := strings.SplitN(blobId, ":", 3)
		if len(parts) == 3 && parts[0] == "blob" {
			parts = parts[1:] // Remove "blob:" prefix to get [data_scope_uuid, key]
		}
		if len(parts) != 2 {
			x.logger.Error("Invalid blob ID in tombstone key", "key", tombstoneKey)
			continue
		}
		// dataScopeUUID, blobKey := parts[0], parts[1]

		metadataJSON, err := x.core.fsm.Get(blobId) // Use blobId as the metadata key
		if err != nil {
			x.logger.Warn("Could not find metadata for tombstoned blob. Deleting tombstone.", "metadatakey", blobId, "tombstonekey", tombstoneKey)
			if err := x.core.fsm.Delete(tombstoneKey); err != nil {
				x.logger.Error("Could not delete orphan blob tombstone", "key", tombstoneKey, "error", err)
			}
			continue
		}

		var blobMeta models.Blob
		if err := json.Unmarshal([]byte(metadataJSON), &blobMeta); err != nil {
			x.logger.Error("Could not unmarshal blob metadata for tombstone processing", "key", blobId, "error", err)
			continue
		}

		// Publish a deletion event so all nodes can remove the physical file.
		eventPayload, err := json.Marshal(blobMeta)
		if err != nil {
			x.logger.Error("Could not marshal blob metadata for deletion event", "key", blobMeta.Key, "error", err)
			continue // Don't proceed if we can't tell other nodes
		}

		err = x.core.fsm.Publish(TopicBlobDeleted, string(eventPayload))
		if err != nil {
			x.logger.Error("Could not publish blob deleted event", "key", blobMeta.Key, "error", err)
			continue // Don't proceed if we can't tell other nodes
		}
		x.logger.Debug("Published blob deleted event", "key", blobMeta.Key)

		// The local file deletion is now handled by the event system for all nodes, including the leader.

		// Delete metadata
		if err := x.core.fsm.Delete(blobId); err != nil {
			x.logger.Error("Could not delete blob metadata", "key", blobId, "error", err)
			continue
		}

		// Delete tombstone
		if err := x.core.fsm.Delete(tombstoneKey); err != nil {
			x.logger.Error("Could not delete blob tombstone", "key", tombstoneKey, "error", err)
		}

		x.logger.Debug("Successfully processed blob tombstone", "key", blobMeta.Key)
	}
	x.logger.Info("Successfully processed all blob tombstones")
}

// ------------------------------- core routes -------------------------------

func (c *Core) uploadBlobHandler(w http.ResponseWriter, r *http.Request) {
	c.IndBlobsOp()
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.RequestURI(), rcPublic)
		return
	}

	// Let's use a large number for max memory. The file is streamed to disk.
	if err := r.ParseMultipartForm(32 << 20); err != nil { // 32MB max memory
		c.logger.Error("Could not parse multipart form", "error", err)
		http.Error(w, "Could not parse multipart form: "+err.Error(), http.StatusBadRequest)
		return
	}

	key := r.FormValue("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	file, handler, err := r.FormFile("blob")
	if err != nil {
		http.Error(w, "Invalid file upload: 'blob' field missing", http.StatusBadRequest)
		return
	}
	defer file.Close()

	ok, current, limit := c.CheckDiskUsage(td, handler.Size)
	if !ok {
		w.Header().Set("X-Current-Disk-Usage", current)
		w.Header().Set("X-Disk-Usage-Limit", limit)
		http.Error(w, "Disk usage limit exceeded", http.StatusBadRequest)
		return
	}

	// Save to a temp file first
	blobDir := filepath.Join(c.cfg.InsidHome, c.nodeName, "blobs")
	tempFile, err := os.CreateTemp(blobDir, "upload-*.tmp")
	if err != nil {
		c.logger.Error("Could not create temp file for blob upload", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name()) // Clean up temp file

	hasher := sha256.New()
	teeReader := io.TeeReader(file, hasher)

	written, err := io.Copy(tempFile, teeReader)
	if err != nil {
		c.logger.Error("Could not write blob to temp file", "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if written != handler.Size {
		c.logger.Error("Blob size mismatch", "expected", handler.Size, "written", written)
		http.Error(w, "Blob size mismatch", http.StatusInternalServerError)
		return
	}

	blobHash := hex.EncodeToString(hasher.Sum(nil))

	finalPath := c.blobPath(td.DataScopeUUID, key)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		c.logger.Error("Could not create blob directory", "path", filepath.Dir(finalPath), "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Close the file before renaming it.
	tempFile.Close()
	if err := os.Rename(tempFile.Name(), finalPath); err != nil {
		c.logger.Error("Could not move blob to final destination", "from", tempFile.Name(), "to", finalPath, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	blobMetadata := models.Blob{
		Key:              key,
		OwnerUUID:        td.KeyUUID,
		DataScopeUUID:    td.DataScopeUUID,
		NodeID:           c.nodeName,
		NodeIdentityUUID: c.identity.GetID(),
		Size:             handler.Size,
		Hash:             blobHash,
		UploadedAt:       time.Now().UTC(),
		OriginalName:     handler.Filename,
	}

	metadataKey := blobMetadataKey(td.DataScopeUUID, key)
	metadataValue, err := json.Marshal(blobMetadata)
	if err != nil {
		c.logger.Error("Could not marshal blob metadata", "error", err)
		os.Remove(finalPath) // cleanup
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	err = c.fsm.Set(models.KVPayload{
		Key:   metadataKey,
		Value: string(metadataValue),
	})
	if err != nil {
		c.logger.Error("Could not set blob metadata in FSM", "error", err)
		os.Remove(finalPath)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	if err := c.AssignBytesToTD(td, StorageTargetDisk, handler.Size); err != nil {
		c.logger.Error("could not assign bytes to td for blob", "error", err)
		// Don't fail the request, but log it.
	}

	eventPayload, err := json.Marshal(blobMetadata)
	if err != nil {
		c.logger.Error("Could not marshal blob metadata for event", "error", err)
	} else {
		err = c.fsm.Publish(TopicBlobUploaded, string(eventPayload))
		if err != nil {
			c.logger.Error("Could not publish blob uploaded event", "error", err)
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Core) internalDownloadBlobHandler(w http.ResponseWriter, r *http.Request) {
	c.IndBlobsOp()
	_, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	key := r.URL.Query().Get("key")
	scope := r.URL.Query().Get("scope")
	if key == "" || scope == "" {
		http.Error(w, "Missing key or scope parameter", http.StatusBadRequest)
		return
	}

	metadataKey := blobMetadataKey(scope, key)

	// Check for tombstone first
	tombstoneKey := KeyPrimitiveBlobTombstone + metadataKey
	if _, err := c.fsm.Get(tombstoneKey); err == nil {
		// Tombstone exists, treat as not found
		http.NotFound(w, r)
		return
	}

	// Also check if metadata exists, otherwise ServeFile will 404 and we won't know why.
	_, err := c.fsm.Get(metadataKey)
	if err != nil {
		c.logger.Warn("Internal blob download failed: metadata not found", "key", key, "metadata_key", metadataKey, "error", err)
		http.NotFound(w, r)
		return
	}

	blobPath := c.blobPath(scope, key)
	http.ServeFile(w, r, blobPath)
}

func (c *Core) getBlobHandler(w http.ResponseWriter, r *http.Request) {
	c.IndBlobsOp()
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	metadataKey := blobMetadataKey(td.DataScopeUUID, key)

	// Check for tombstone first
	tombstoneKey := KeyPrimitiveBlobTombstone + metadataKey
	tombstoneVal, err := c.fsm.Get(tombstoneKey)
	if err == nil {
		c.logger.Debug("Blob has tombstone, denying access", "key", key, "tombstone_key", tombstoneKey, "tombstone_val", tombstoneVal)
		// Tombstone exists, treat as not found
		http.NotFound(w, r)
		return
	}

	_, err = c.fsm.Get(metadataKey)
	if err != nil {
		c.logger.Info("Blob metadata not found", "key", key, "metadata_key", metadataKey)
		http.NotFound(w, r)
		return
	}

	blobPath := c.blobPath(td.DataScopeUUID, key)
	c.logger.Debug("Serving blob from path", "key", key, "path", blobPath)
	http.ServeFile(w, r, blobPath)
}

func (c *Core) deleteBlobHandler(w http.ResponseWriter, r *http.Request) {
	c.IndBlobsOp()
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
		return
	}

	if !c.fsm.IsLeader() {
		c.redirectToLeader(w, r, r.URL.RequestURI(), rcPublic)
		return
	}

	defer r.Body.Close()
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		c.logger.Error("Could not read body for blob delete request", "error", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var p models.KeyPayload
	if err := json.Unmarshal(bodyBytes, &p); err != nil {
		c.logger.Error("Invalid JSON payload for blob delete request", "error", err)
		http.Error(w, "Invalid JSON payload for blob delete: "+err.Error(), http.StatusBadRequest)
		return
	}

	if p.Key == "" {
		http.Error(w, "Missing key in blob delete request payload", http.StatusBadRequest)
		return
	}

	metadataKey := blobMetadataKey(td.DataScopeUUID, p.Key)
	metadataJSON, err := c.fsm.Get(metadataKey)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	var blobMeta models.Blob
	if err := json.Unmarshal([]byte(metadataJSON), &blobMeta); err != nil {
		c.logger.Error("Could not unmarshal blob metadata for deletion", "key", p.Key, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Create a tombstone record. The tombstone runner will handle the actual deletion.
	tombstoneKey := KeyPrimitiveBlobTombstone + metadataKey
	err = c.fsm.Set(models.KVPayload{
		Key:   tombstoneKey,
		Value: time.Now().UTC().Format(time.RFC3339), // Store deletion time
	})
	if err != nil {
		c.logger.Error("Could not create blob tombstone", "key", p.Key, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Decrement disk usage
	if err := c.AssignBytesToTD(td, StorageTargetDisk, -blobMeta.Size); err != nil {
		c.logger.Error("could not deduct bytes from td for blob deletion", "error", err)
	}

	w.WriteHeader(http.StatusAccepted) // Accepted for deletion
}

func (c *Core) iterateBlobKeysByPrefixHandler(w http.ResponseWriter, r *http.Request) {
	c.IndBlobsOp()
	td, ok := c.ValidateToken(r, AnyUser())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if !c.CheckRateLimit(w, r, td.KeyUUID, limiterTypeData) {
		return
	}

	prefix := r.URL.Query().Get("prefix")
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		offset = 0
	}
	if offset < 0 {
		offset = 0
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 100
	}
	if limit <= 0 {
		limit = 100
	}

	searchPrefix := fmt.Sprintf("blob:%s:%s", td.DataScopeUUID, prefix)
	keys, err := c.fsm.Iterate(searchPrefix, offset, limit, "")
	if err != nil {
		var nfErr *tkv.ErrKeyNotFound
		if errors.As(err, &nfErr) || errors.Is(err, badger.ErrKeyNotFound) {
			http.NotFound(w, r)
			return
		}
		c.logger.Error("Could not iterate blob keys", "prefix", searchPrefix, "error", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Filter out tombstoned keys
	finalKeys := []string{}
	stripPrefix := fmt.Sprintf("blob:%s:", td.DataScopeUUID)
	for _, metadataKey := range keys {
		tombstoneKey := KeyPrimitiveBlobTombstone + metadataKey
		_, err := c.fsm.Get(tombstoneKey)
		if err == nil {
			// Tombstone exists, skip.
			continue
		}

		var nfErr *tkv.ErrKeyNotFound
		if errors.As(err, &nfErr) || errors.Is(err, badger.ErrKeyNotFound) {
			// No tombstone, key is valid.
			keyWithoutScope := strings.TrimPrefix(metadataKey, stripPrefix)
			finalKeys = append(finalKeys, keyWithoutScope)
		} else {
			// Unexpected error, log it and skip the key for safety.
			c.logger.Error("Error checking for blob tombstone", "key", metadataKey, "error", err)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"data": finalKeys})
}
