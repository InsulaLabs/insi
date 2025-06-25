# Blob Storage System

This document provides a comprehensive overview of the distributed blob storage system within insi. It details how blobs are uploaded, replicated, accessed, and eventually deleted across a cluster of nodes. The system leverages the underlying Raft consensus protocol to ensure metadata consistency and coordinate actions like replication and garbage collection.

## 1. System Architecture

The blob storage system is composed of two main parts:

1.  **Physical Storage:** Each node in the cluster maintains a local directory for storing the binary data of blobs. The path is structured as `<insid_home>/<node_id>/blobs/<data_scope_uuid>/<sha256_of_key>`. Hashing the key prevents filesystem issues with special characters and distributes files within the directory.
2.  **Metadata Storage:** All metadata associated with a blob (e.g., its key, size, hash, owner) is stored within the distributed key-value store, which is managed and replicated by the Raft FSM (Finite State Machine). This ensures that every node in the cluster has a consistent view of the blob metadata.

The `blobService` is the core component on each node responsible for managing these two parts. It handles event processing for replication and runs background tasks for cleanup.

### 1.1. Blob Metadata Structure

Each blob's metadata is stored as a JSON object with the following structure:

```go
type Blob struct {
    Key              string    `json:"key"`               // User-defined identifier
    OwnerUUID        string    `json:"owner_uuid"`        // UUID of owning API key
    DataScopeUUID    string    `json:"data_scope_uuid"`   // Data scope for isolation
    NodeID           string    `json:"node_id"`           // Logical node name where uploaded
    NodeIdentityUUID string    `json:"node_identity_uuid"` // Cryptographic node identity
    Size             int64     `json:"size"`              // Size in bytes
    Hash             string    `json:"hash"`              // SHA256 content hash
    UploadedAt       time.Time `json:"uploaded_at"`       // Upload timestamp
    OriginalName     string    `json:"original_name"`     // Original filename (optional)
}
```

## 2. API Endpoints

The blob system exposes the following HTTP endpoints:

### 2.1. Public Endpoints (Require API key authentication)
- `POST /db/api/v1/blob/set` - Upload a blob with multipart/form-data
- `GET /db/api/v1/blob/get?key=<key>` - Download a blob
- `POST /db/api/v1/blob/delete` - Delete a blob (creates tombstone)
- `GET /db/api/v1/blob/iterate/prefix?prefix=<prefix>&offset=<n>&limit=<n>` - List blob keys by prefix

### 2.2. Internal Endpoints (Require root authentication)
- `GET /db/internal/v1/blob/download?key=<key>&scope=<scope>` - Internal blob download for replication

All public endpoints include rate limiting and quota enforcement.

## 3. Data Lifecycle

### 3.1. Blob Upload and Replication

The process of uploading a blob is leader-driven and designed for strong consistency.

1.  **Client Request:** A client sends an HTTP `POST` request with `multipart/form-data` (containing the blob's `key` and its binary `blob` data) to any node in the cluster.
2.  **Leader Redirection:** If the receiving node is not the Raft leader, it transparently redirects the client to the current leader. Only the leader is permitted to ingest new data to avoid conflicts.
3.  **Leader Processing:** The leader node performs the following steps:
    a. **Rate Limiting:** Checks API key rate limits for data operations.
    b. **Quota Check:** It verifies if the owner's API key has sufficient disk quota using `CheckDiskUsage()`.
    c. **Temporary Storage:** The blob is streamed to a temporary file on the leader's local disk. During this stream, a `SHA256` hash of the blob's content is calculated.
    d. **Final Storage:** The temporary file is atomically moved to its final, permanent location in the blob storage directory.
    e. **Metadata Persistence:** The leader creates a metadata record for the blob. This record is a `models.Blob` struct containing the key, size, content hash, owner UUID, and the leader's own node ID. This metadata is committed to the distributed key-value store via a Raft `Set` command.
    f. **Disk Usage Tracking:** The blob's size is added to the API key's disk usage quota via `AssignBytesToTD()`.
    g. **Replication Event:** The leader then publishes an event with the topic `_:blob:uploaded` to the Raft log. The payload of this event is the JSON-serialized blob metadata.

4.  **Follower Replication:**
    a. As the Raft log is replicated, every follower node receives and applies the `Set` command for the metadata and the `Publish` command for the `_:blob:uploaded` event.
    b. The `blobService` on each follower node is subscribed to this topic. Upon receiving the event, it checks if it already has the blob and if the event came from itself (skip self-replication).
    c. If the blob doesn't exist locally, the follower initiates a download from the source node (whose ID is in the event metadata) via a secure, internal-only root-level HTTP endpoint (`/db/internal/v1/blob/download`).
    d. The download uses the cluster's configured TLS client for proper certificate handling and verification.
    e. After downloading to a temporary file, the follower verifies the blob's content against the SHA256 hash from the metadata and atomically moves it to its final location.

This process ensures that the blob's binary data and its associated metadata are consistently replicated across all nodes in the cluster.

### 3.2. Blob Access (Get)

When a client requests to download a blob:

1.  The request is handled by any node (no leader redirection required).
2.  Rate limiting is applied to the requesting API key.
3.  The node first checks for the existence of a "tombstone" record for the requested blob key using the pattern `_:blob:tombstone:<metadata_key>`.
4.  If a tombstone exists, the blob is considered deleted, and the node returns an HTTP `404 Not Found` error.
5.  If no tombstone is found, the node retrieves the blob's metadata from the (local) Raft FSM.
6.  If the metadata exists, the node serves the blob file directly from its local filesystem path using `http.ServeFile()`.

### 3.3. Blob Deletion (Tombstone Mechanism)

Deletion is a two-phase process that uses a "tombstone" pattern to gracefully handle deletion in a distributed environment.

1.  **Marking for Deletion:**
    a. A client sends a delete request, which is redirected to the leader.
    b. Rate limiting is applied to the requesting API key.
    c. The leader **does not** immediately delete the blob file or its metadata.
    d. Instead, it creates a **tombstone record** in the distributed key-value store via a Raft `Set` command. The key for this record is prefixed with `_:blob:tombstone:`, and the value contains the timestamp of the deletion request.
    e. The blob's size is deducted from the API key's disk usage quota.
    f. This tombstone is replicated to all nodes via Raft, effectively making the blob inaccessible for all future `get` requests (as described above).

2.  **Garbage Collection:**
    a. The `blobService` on each node runs a periodic background task every **30 seconds** (`TombstoneCycleFrequency`).
    b. On the **leader node only**, this task (`execTombstoneDeletion`) scans the key-value store for tombstone records.
    c. For each tombstone found, the leader orchestrates the final cluster-wide cleanup:
        i. It publishes an event with the topic `_:blob:deleted` to the Raft log. The payload contains the metadata of the blob to be deleted.
        ii. As the event is replicated, every node in the cluster (including the leader) receives it. The `blobService` on each node is subscribed to this topic and proceeds to delete the physical blob file from its local disk (`os.Remove`).
        iii. After publishing the event, the leader commits a `Delete` command to Raft to remove the original blob metadata.
        iv. Finally, the leader commits another `Delete` command to Raft to remove the tombstone record itself.
    d. This two-step process, using a combination of a leader-driven garbage collection task and a distributed event, ensures that both the physical blob files and their associated metadata are consistently and cleanly removed from all nodes across the cluster.
    e. **Orphan Tombstone Handling:** If a tombstone exists but the corresponding blob metadata is missing, the tombstone is automatically cleaned up.

## 4. Error Handling and Resilience

### 4.1. Blob Clone Failure and Retry Logic

In the event of a replication failure immediately following an upload, the system implements a robust retry mechanism:

1. **Initial Retries:** The system will retry blob downloads **5 times** (`MaxBlobDownloadRetries`) with linear backoff (1, 2, 3, 4, 5 seconds).
2. **Back Burner Retry:** If all initial retries fail, a "back burner" event is fired in a parallel goroutine after **10 seconds** to perform another complete 5-retry cycle.
3. **Local Retry Topic:** Each node maintains a local retry topic (`<node_name>:blob:retry`) for managing its own retry attempts without affecting other nodes.
4. **Context Cancellation:** All retry operations respect context cancellation for graceful shutdown.

### 4.2. Hash Verification and Data Integrity

- All blob downloads are verified against their SHA256 hash before being moved to final storage locations
- Temporary files are cleaned up automatically on any failure
- Hash mismatches trigger retry attempts or failure logging

### 4.3. TLS and Security

- Internal blob downloads use the cluster's configured TLS client
- Proper certificate verification is maintained for inter-node communication
- Root-only authentication is required for internal download endpoints

## 5. Integration Points

### 5.1. API Key Lifecycle Integration

When an API key is deleted, the system automatically:
- Identifies all blob metadata associated with that key
- Publishes `_:blob:deleted` events for each blob
- Ensures physical blob files are removed from all nodes

### 5.2. Metrics and Monitoring

- All blob operations are tracked via `IndBlobsOp()` for metrics collection
- Operations per second counters are maintained for performance monitoring
- Rate limiting events are logged and tracked

### 5.3. Quota and Resource Management

- Disk usage is tracked per API key for quota enforcement
- Upload requests are rejected if quota would be exceeded
- Disk usage headers are returned in error responses for client awareness

## 6. TODO and Known Limitations

### 6.1. Watchdog System

**Status:** Not yet implemented

Create a watchdog thread similar to the tombstone executor that ensures complete replication across all nodes. The challenge is that writing to the FSM requires leader status, so depending on the failure scenario, coordination may require back-channel communication (like internal events) to ensure completeness.

**Note:** This hasn't been prioritized yet because stress testing has not yet revealed any failures that the current system cannot handle.

### 6.2. Performance Considerations

- **Leader Stickiness:** Clients can enable leader stickiness to reduce redirects, but this has resilience trade-offs
- **Replication Bandwidth:** Large blobs consume significant bandwidth during replication
- **Storage Distribution:** Blob storage is evenly distributed across all nodes (no selective replication)

## 7. Constants and Configuration

- `TombstoneCycleFrequency`: 30 seconds
- `MaxBlobDownloadRetries`: 5 attempts
- Internal retry delay: 10 seconds
- Retry backoff: Linear (1-5 seconds)
- Topics:
  - `_:blob:uploaded`: Replication events
  - `_:blob:deleted`: Deletion events
  - `<node_name>:blob:retry`: Local retry events