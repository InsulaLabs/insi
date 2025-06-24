# Blob Storage System

This document provides a comprehensive overview of the distributed blob storage system within insi. It details how blobs are uploaded, replicated, accessed, and eventually deleted across a cluster of nodes. The system leverages the underlying Raft consensus protocol to ensure metadata consistency and coordinate actions like replication and garbage collection.

## 1. System Architecture

The blob storage system is composed of two main parts:

1.  **Physical Storage:** Each node in the cluster maintains a local directory for storing the binary data of blobs. The path is structured as `<insid_home>/<node_id>/blobs/<data_scope_uuid>/<sha256_of_key>`. Hashing the key prevents filesystem issues with special characters and distributes files within the directory.
2.  **Metadata Storage:** All metadata associated with a blob (e.g., its key, size, hash, owner) is stored within the distributed key-value store, which is managed and replicated by the Raft FSM (Finite State Machine). This ensures that every node in the cluster has a consistent view of the blob metadata.

The `blobService` is the core component on each node responsible for managing these two parts. It handles event processing for replication and runs background tasks for cleanup.

## 2. Data Lifecycle

### 2.1. Blob Upload and Replication

The process of uploading a blob is leader-driven and designed for strong consistency.

1.  **Client Request:** A client sends an HTTP `POST` request with `multipart/form-data` (containing the blob's `key` and its binary `blob` data) to any node in the cluster.
2.  **Leader Redirection:** If the receiving node is not the Raft leader, it transparently redirects the client to the current leader. Only the leader is permitted to ingest new data to avoid conflicts.
3.  **Leader Processing:** The leader node performs the following steps:
    a. **Quota Check:** It verifies if the owner's API key has sufficient disk quota.
    b. **Temporary Storage:** The blob is streamed to a temporary file on the leader's local disk. During this stream, a `SHA256` hash of the blob's content is calculated.
    c. **Final Storage:** The temporary file is atomically moved to its final, permanent location in the blob storage directory.
    d. **Metadata Persistence:** The leader creates a metadata record for the blob. This record is a `models.Blob` struct containing the key, size, content hash, owner UUID, and the leader's own node ID. This metadata is committed to the distributed key-value store via a Raft `Set` command.
    e. **Replication Event:** The leader then publishes an event with the topic `_:blob:uploaded` to the Raft log. The payload of this event is the JSON-serialized blob metadata.

4.  **Follower Replication:**
    a. As the Raft log is replicated, every follower node receives and applies the `Set` command for the metadata and the `Publish` command for the `_:blob:uploaded` event.
    b. The `blobService` on each follower node is subscribed to this topic. Upon receiving the event, it checks if it already has the blob.
    c. If the blob doesn't exist locally, the follower initiates a download from the source node (whose ID is in the event metadata) via a secure, internal-only root-level HTTP endpoint (`/db/internal/v1/blob/download`).
    d. After downloading, the follower verifies the blob's content against the hash from the metadata and saves it to its local storage path.

This process ensures that the blob's binary data and its associated metadata are consistently replicated across all nodes in the cluster.

### 2.2. Blob Access (Get)

When a client requests to download a blob:

1.  The request is handled by any node.
2.  The node first checks for the existence of a "tombstone" record for the requested blob key.
3.  If a tombstone exists, the blob is considered deleted, and the node returns an HTTP `404 Not Found` error.
4.  If no tombstone is found, the node retrieves the blob's metadata from the (local) Raft FSM.
5.  If the metadata exists, the node serves the blob file directly from its local filesystem path.

### 2.3. Blob Deletion (Tombstone Mechanism)

Deletion is a two-phase process that uses a "tombstone" pattern to gracefully handle deletion in a distributed environment.

1.  **Marking for Deletion:**
    a. A client sends a delete request, which is redirected to the leader.
    b. The leader **does not** immediately delete the blob file or its metadata.
    c. Instead, it creates a **tombstone record** in the distributed key-value store via a Raft `Set` command. The key for this record is prefixed with `_:blob:tombstone:`, and the value contains the timestamp of the deletion request.
    d. This tombstone is replicated to all nodes via Raft, effectively making the blob inaccessible for all future `get` requests (as described above).

2.  **Garbage Collection:**
    a. The `blobService` on each node runs a periodic background task.
    b. On the **leader node only**, this task (`execTombstoneDeletion`) scans the key-value store for tombstone records.
    c. For each tombstone found, the leader orchestrates the final cluster-wide cleanup:
        i. It publishes an event with the topic `_:blob:deleted` to the Raft log. The payload contains the metadata of the blob to be deleted.
        ii. As the event is replicated, every node in the cluster (including the leader) receives it. The `blobService` on each node is subscribed to this topic and proceeds to delete the physical blob file from its local disk (`os.Remove`).
        iii. After publishing the event, the leader commits a `Delete` command to Raft to remove the original blob metadata.
        iv. Finally, the leader commits another `Delete` command to Raft to remove the tombstone record itself.
    d. This two-step process, using a combination of a leader-driven garbage collection task and a distributed event, ensures that both the physical blob files and their associated metadata are consistently and cleanly removed from all nodes across the cluster.
