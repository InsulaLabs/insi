# API Key System

The InsiDB API key system provides secure access to the database. It includes features for resource management, key lifecycle (creation, deletion), and flexible access patterns through aliases. All API key data and metadata are stored within the distributed key-value store, ensuring consistency across the cluster.

## Key Concepts

### Resource Tracking & Limits

Every API key is subject to resource limits to ensure fair usage and prevent abuse. The system tracks the following for each key:

*   **Bytes in Memory** (`bytes_in_memory`): The total size of key-value pairs stored in the in-memory cache.
*   **Bytes on Disk** (`bytes_on_disk`): The total size of key-value pairs and blobs stored on disk.
*   **Events Emitted** (`events_emitted`): The number of events published by this key within a 24-hour period. This counter automatically resets every 24 hours.
*   **Subscribers** (`subscribers`): The number of concurrent event stream subscriptions a key currently has.
*   **Data RPS Limit** (`rate_per_second_data_limit`): The number of data-plane requests per second a key can make (e.g., set, get, delete operations).
*   **Event RPS Limit** (`rate_per_second_event_limit`): The number of event-plane requests per second a key can make (e.g., publish, subscribe operations).

These are tracked using specific internal key prefixes (e.g., `internal:api_key_memory_usage:<key_uuid>`).

Limits for each of these resources are also stored and can be configured on a per-key basis by an administrator. When a rate limit is exceeded, the API will respond with a `429 Too Many Requests` status code, including a `Retry-After` header to indicate when the client may retry the request.

**Default Resource Limits:**
- Memory Usage: 250 MB
- Disk Usage: 1 GB
- Events Emitted: 1,000 per 24-hour period
- Subscribers: 100 concurrent connections
- Data RPS Limit: 25 requests per second
- Event RPS Limit: 10 requests per second

### Key Lifecycle Management (Admin Only)

The creation and deletion of primary API keys are administrative tasks that can only be performed by a client using the system's root API key (derived from the cluster's instance secret).

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

This asynchronous process ensures that deleting a key with a large amount of data does not block the system. This cleanup also includes the automatic deletion of any aliases created by the key.

### Resource Limit Management

While resource limits are set by default upon key creation, they can be viewed and modified via specific API endpoints.

#### Setting Resource Limits (Admin Only)

*   **Endpoint**: `POST /db/api/v1/admin/limits/set`
*   **Authentication**: System Root Key

An administrator can set or update any resource limit for a specific API key. The request body must include the target `api_key` and a `limits` object containing the values to be changed. Any fields omitted from the `limits` object will remain unchanged.

#### Viewing Resource Limits

There are two ways to view limits: for the calling key, or for a specific key (admin only).

*   **Endpoint (Caller)**: `GET /db/api/v1/limits`
*   **Authentication**: Any User Key

This endpoint returns an object showing the maximum configured limits and the current resource usage for the key making the request. **Note**: Current usage for RPS limits is not included in the response as it represents instantaneous rate data.

*   **Endpoint (Admin)**: `POST /db/api/v1/admin/limits/get`
*   **Authentication**: System Root Key

This allows an administrator to view the limits and usage for any key by specifying it in the request body. **Note**: Like the caller endpoint, current RPS usage is not included in the response.

## API Key Aliases

Aliases allow a primary API key (entity root key) to generate disposable, subordinate keys. These aliases share the exact same identity, permissions, and resource limits as their parent key.

**Purpose**: The primary use case is to create keys that can be safely used in less secure environments (e.g., client-side applications, third-party integrations). If an alias is compromised, it can be revoked without affecting the primary key or the services that depend on it.

### Creating an Alias

*   **Endpoint**: `POST /db/api/v1/alias/set`
*   **Authentication**: Any User Key (The entity root key creating the alias)

Any valid API key (except the system root key) can create an alias for itself. **Important**: Only entity root keys can create aliases; alias keys cannot create further aliases.

1.  The system generates a brand new, unique API key to serve as the alias.
2.  It creates two mappings in the database:
    *   A link from the alias to its root key (`internal:alias_to_root:<alias_key> -> <root_key_uuid>`). This is used during token validation to resolve the alias to its true identity.
    *   A link from the root key to the alias (`internal:root_to_alias:<root_key_uuid>:<alias_key> -> "1"`). This is used for listing.
3.  The new alias key is returned in the response.

A single key can have a maximum of **16** aliases.

### Deleting an Alias

*   **Endpoint**: `POST /db/api/v1/alias/delete`
*   **Authentication**: The **Entity Root Key** of the alias.

To delete an alias, the request must be authenticated with the original entity root key that created the alias, not the alias itself. This prevents a compromised alias from deleting itself. The process removes the mappings and the alias key from the system.

### Listing Aliases

*   **Endpoint**: `GET /db/api/v1/alias/list`
*   **Authentication**: Any User Key

A key can retrieve a list of all its active aliases.

## Key Terminology

**System Root Key**: The cluster-wide administrative key derived from the instance secret. This key has administrative privileges and can create/delete other API keys and modify limits.

**Entity Root Key**: A user's primary API key created via the admin API. This is the "parent" key from which aliases can be created. Also referred to as a "root API key" in the context of a specific user entity.

**Alias Key**: A subordinate key created from an entity root key. Aliases inherit all permissions and limits from their parent but can be revoked independently.
