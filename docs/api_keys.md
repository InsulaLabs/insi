# API Key System

The InsiDB API key system provides secure access to the database. It includes features for resource management, key lifecycle (creation, deletion), and flexible access patterns through aliases. All API key data and metadata are stored within the distributed key-value store, ensuring consistency across the cluster.

## Key Concepts

### Resource Tracking & Limits

Every API key is subject to resource limits to ensure fair usage and prevent abuse. The system tracks the following for each key:

*   **Memory Usage**: The total size of key-value pairs stored in the in-memory cache.
*   **Disk Usage**: The total size of key-value pairs and blobs stored on disk.
*   **Events**: The number of events a key can publish within a certain time window.
*   **Subscriptions**: The number of concurrent event stream subscriptions a key can have.

These are tracked using specific internal key prefixes (e.g., `internal:api_key_memory_usage:<key_uuid>`).

Limits for each of these resources are also stored and can be configured on a per-key basis by an administrator.

### Key Lifecycle Management (Admin Only)

The creation and deletion of primary API keys are administrative tasks that can only be performed by a client using the system's root API key.

#### Key Creation

*   **Endpoint**: `POST /db/api/v1/admin/api/create`
*   **Authentication**: System Root Key

An administrator can create a new API key by providing a `KeyName`. The system generates a new, unique API key and associated UUID, initializes its resource trackers, and returns the new key to the admin.

#### Key Deletion & Tombstones

*   **Endpoint**: `POST /db/api/v1/admin/api/delete`
*   **Authentication**: System Root Key

When an API key is deleted, it is not immediately purged from the system. Instead, it is "tombstoned."

1.  A tombstone record is created (e.g., `internal:api_key_tombstone:<deleted_key_uuid>`). This record holds the unique data scope ID associated with the key.
2.  A background process on the leader node periodically scans for these tombstones.
3.  Upon finding a tombstone, the runner iteratively deletes all data associated with that key's data scope ID.
4.  Once all user data is deleted, the runner cleans up the key's metadata (resource trackers, limits) and the API key itself.
5.  Finally, the tombstone record is removed.

This asynchronous process ensures that deleting a key with a large amount of data does not block the system.

## API Key Aliases

Aliases allow a primary API key to generate disposable, subordinate keys. These aliases share the exact same identity, permissions, and resource limits as their "root" key.

**Purpose**: The primary use case is to create keys that can be safely used in less secure environments (e.g., client-side applications, third-party integrations). If an alias is compromised, it can be revoked without affecting the primary key or the services that depend on it.

### Creating an Alias

*   **Endpoint**: `POST /db/api/v1/alias/set`
*   **Authentication**: Any User Key (The key creating the alias)

Any valid API key (except the system root key) can create an alias for itself.

1.  The system generates a brand new, unique API key to serve as the alias.
2.  It creates two mappings in the database:
    *   A link from the alias to its root key (`internal:alias_to_root:<alias_key> -> <root_key_uuid>`). This is used during token validation to resolve the alias to its true identity.
    *   A link from the root key to the alias (`internal:root_to_alias:<root_key_uuid>:<alias_key> -> "1"`). This is used for listing.
3.  The new alias key is returned in the response.

A single key can have a maximum of **16** aliases.

### Deleting an Alias

*   **Endpoint**: `POST /db/api/v1/alias/delete`
*   **Authentication**: The **Root Key** of the alias.

To delete an alias, the request must be authenticated with the original key that created the alias, not the alias itself. This prevents a compromised alias from deleting itself. The process removes the mappings and the alias key from the system.

### Listing Aliases

*   **Endpoint**: `GET /db/api/v1/alias/list`
*   **Authentication**: Any User Key

A key can retrieve a list of all its active aliases.
