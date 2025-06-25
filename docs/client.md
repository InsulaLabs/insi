# Insi Client API Specification

This document provides a technical specification of the Insi API endpoints as consumed by the official Go client. It is intended for developers who want to build custom clients or understand the client-server communication protocol.

## General Concepts

### Authentication

All API requests must be authenticated by including an API key in the `Authorization` header.

`Authorization: your-api-key`

Some administrative endpoints require a root API key.

### Base URLs

The client can connect to a public and a private endpoint. Most operations go through the public endpoint. Administrative and cluster operations (like `Join`) use the private endpoint. The client determines which to use based on the path.

-   Private paths: `db/api/v1/join`, `db/api/v1/admin/*`
-   All other paths are public.

### Content Type

For all `POST` requests with a JSON body, the `Content-Type` header must be set to `application/json`. Blob uploads use `multipart/form-data`.

### JSON Field Naming Convention

All JSON field names in request and response bodies use **snake_case** convention (e.g., `key_name`, `old_value`, `api_key`, `root_api_key`). This applies consistently across all API endpoints.

### Redirects & Leader Stickiness

The server uses redirects to guide clients to the current Raft leader for write operations.

-   **Status Codes**: `301`, `302`, `303` (general redirects), `307` (Temporary Redirect), `308` (Permanent Redirect).
-   **Header**: The `Location` header contains the URL to redirect to.
-   Clients should follow these redirects. Statuses `307` and `308` specifically indicate a leader change. A client can "stick" to this new leader's address for subsequent requests to avoid the redirect overhead on every write.

### Error Handling

The API uses standard HTTP status codes for errors.

-   **`400 Bad Request`**: Malformed request or a resource limit was exceeded. The response body may be a simple string or a JSON object:
    `{ "error_type": "string", "message": "string" }`
    -   **Resource Limit Headers**: If a limit is hit, the `400` response will include specific headers:
        -   `X-Current-Disk-Usage`, `X-Disk-Usage-Limit`
        -   `X-Current-Memory-Usage`, `X-Memory-Usage-Limit`
        -   `X-Current-Events`, `X-Events-Limit`

-   **`401 Unauthorized`**: The API key is invalid or missing. The error response body might be a JSON object like:
    `{ "error_type": "API_KEY_NOT_FOUND", "message": "..." }`

-   **`404 Not Found`**:
    -   For GET operations on data (e.g., `db/api/v1/get`), this semantically means the requested key/resource was not found.
    -   For other operations, it means the API endpoint path itself does not exist.

-   **`409 Conflict` / `412 Precondition Failed`**: Used for failed atomic operations (e.g., `SetNX`, `CompareAndSwap`). The existing state of the resource prevented the operation from completing.

-   **`429 Too Many Requests`**: The client is rate-limited.
    -   **Headers**:
        -   `Retry-After`: The number of seconds to wait before retrying.
        -   `X-RateLimit-Limit`: The rate limit ceiling.
        -   `X-RateLimit-Burst`: The burst allowance.

---

## Endpoints

### Key-Value Store

#### Get Value

-   **Description**: Retrieves a value for a given key.
-   **Method**: `GET`
-   **Path**: `/db/api/v1/get`
-   **Query Parameters**:
    -   `key`: (string) The key to retrieve.
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "data": "value"
    }
    ```
-   **Errors**: Returns a semantic "key not found" error on `404 Not Found`.

#### Set Value

-   **Description**: Sets a value for a given key.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/set`
-   **Request Body**:
    ```json
    {
      "key": "string",
      "value": "string"
    }
    ```

#### Set Value If Not Exists (SetNX)

-   **Description**: Sets a value for a key only if the key does not already exist.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/setnx`
-   **Request Body**:
    ```json
    {
      "key": "string",
      "value": "string"
    }
    ```
-   **Errors**: `409 Conflict` if the key already exists.

#### Compare and Swap (CAS)

-   **Description**: Atomically swaps a key's value from an old value to a new one.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/cas`
-   **Request Body**:
    ```json
    {
      "key": "string",
      "old_value": "string",
      "new_value": "string"
    }
    ```
-   **Errors**: `409 Conflict` or `412 Precondition Failed` if the current value does not match `old_value`.

#### Bump (Atomic Increment)

-   **Description**: Atomically increments the integer value of a key.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/bump`
-   **Request Body**:
    ```json
    {
      "key": "string",
      "value": "integer-as-string"
    }
    ```

#### Delete Value

-   **Description**: Deletes a key-value pair.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/delete`
-   **Request Body**:
    ```json
    {
      "key": "string"
    }
    ```
-   **Note**: A `404 Not Found` is treated as a successful deletion (no error).

#### Iterate By Prefix

-   **Description**: Retrieves a list of keys matching a prefix.
-   **Method**: `GET`
-   **Path**: `/db/api/v1/iterate/prefix`
-   **Query Parameters**:
    -   `prefix`: (string) The key prefix to scan.
    -   `offset`: (integer) The number of keys to skip.
    -   `limit`: (integer) The maximum number of keys to return.
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "data": ["key1", "key2", ...]
    }
    ```
-   **Errors**: Returns a semantic "key not found" if no keys match the prefix.

### Cache Store

Cache operations mirror the key-value store operations but act on a separate in-memory (non-persistent) space. The paths are prefixed with `/db/api/v1/cache`.

-   **SetCache**: `POST /db/api/v1/cache/set`
-   **GetCache**: `GET /db/api/v1/cache/get`
-   **DeleteCache**: `POST /db/api/v1/cache/delete`
-   **SetCacheNX**: `POST /db/api/v1/cache/setnx`
-   **CompareAndSwapCache**: `POST /db/api/v1/cache/cas` (uses same `old_value`/`new_value` payload as regular CAS)
-   **IterateCacheByPrefix**: `GET /db/api/v1/cache/iterate/prefix` (Response body is a direct JSON array of strings, not wrapped in a `data` object).

Payloads, parameters, and error semantics are identical to their key-value store counterparts.

### Blob Store

#### Upload Blob

-   **Description**: Uploads a binary blob.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/blob/set`
-   **Content-Type**: `multipart/form-data`
-   **Request Body (Multipart Form)**:
    -   `key`: (string) The key for the blob.
    -   `blob`: (file) The binary data of the blob.
-   **Errors**: Can return `400 Bad Request` with `X-Disk-Usage-Limit` headers if storage quotas are exceeded.

#### Get Blob

-   **Description**: Downloads a binary blob.
-   **Method**: `GET`
-   **Path**: `/db/api/v1/blob/get`
-   **Query Parameters**:
    -   `key`: (string) The key of the blob to retrieve.
-   **Response Body (Success `200 OK`)**: The raw binary data of the blob (`io.ReadCloser`).
-   **Errors**: `404 Not Found` if the blob key does not exist.

#### Delete Blob

-   **Description**: Deletes a blob.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/blob/delete`
-   **Request Body**:
    ```json
    {
      "key": "string"
    }
    ```
-   **Errors**: `404 Not Found` if the key does not exist.

#### Iterate Blob Keys By Prefix

-   **Description**: Retrieves a list of blob keys matching a prefix.
-   **Method**: `GET`
-   **Path**: `/db/api/v1/blob/iterate/prefix`
-   **Query Parameters**:
    -   `prefix`: (string)
    -   `offset`: (integer)
    -   `limit`: (integer)
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "data": ["key1", "key2", ...]
    }
    ```
-   **Errors**: Returns an empty list if no keys match, not an error.

### Events

#### Publish Event

-   **Description**: Publishes a message to a topic.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/events`
-   **Request Body**:
    ```json
    {
      "topic": "string",
      "data": {}
    }
    ```
-   **Errors**: Can return `400 Bad Request` with `X-Events-Limit` headers if event quotas are exceeded.

#### Subscribe to Events

-   **Description**: Subscribes to a topic for real-time events.
-   **Protocol**: WebSocket (`ws://` or `wss://`)
-   **Path**: `/db/api/v1/events/subscribe`
-   **Query Parameters**:
    -   `topic`: (string) The topic to subscribe to.
-   **Connection Headers**:
    -   `Authorization`: `your-api-key`
-   **Behavior**:
    -   Handles HTTP redirects (`30x`) during the initial handshake.
    -   Receives JSON messages from the server.
-   **Errors**: `503 Service Unavailable` if subscriber limits are exceeded.

### Alias Operations

Aliases are human-readable names associated with an API key.

#### Set Alias

-   **Description**: Creates a new, unique alias for the API key being used.
-   **Method**: `GET`
-   **Path**: `/db/api/v1/alias/set`
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "alias": "generated-alias-string"
    }
    ```

#### Delete Alias

-   **Description**: Deletes a specific alias.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/alias/delete`
-   **Request Body**:
    ```json
    {
      "alias": "string"
    }
    ```

#### List Aliases

-   **Description**: Lists all aliases for the current API key.
-   **Method**: `GET`
-   **Path**: `/db/api/v1/alias/list`
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "aliases": ["alias1", "alias2", ...]
    }
    ```

### System & Admin (Private Endpoints)

These endpoints require a **root API key** and should be accessed via the **private** address.

#### Join Cluster

-   **Description**: (Leader-only) Instructs the leader to add a new follower node to the cluster.
-   **Method**: `GET`
-   **Path**: `/db/api/v1/join`
-   **Query Parameters**:
    -   `followerId`: (string) The unique ID of the new node.
    -   `followerAddr`: (string) The address of the new node.

#### Ping

-   **Description**: A simple health check endpoint.
-   **Method**: `GET`
-   **Path**: `/db/api/v1/ping`
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "message": "pong"
    }
    ```

#### Create API Key

-   **Description**: Creates a new API key.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/admin/api/create`
-   **Request Body**: `{"key_name": "string"}`
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "key": "new-api-key",
      "key_name": "string"
    }
    ```

#### Delete API Key

-   **Description**: Deletes an API key.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/admin/api/delete`
-   **Request Body**: `{"key": "api-key-to-delete"}`

#### Get Limits

-   **Description**: Retrieves usage limits for the API key making the request.
-   **Method**: `GET`
-   **Path**: `/db/api/v1/limits`
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "usage": {
        "bytes_on_disk": 1048576,
        "bytes_in_memory": 262144,
        "events_emitted": 150,
        "subscribers": 5,
        "rate_per_second_data_limit": 25,
        "rate_per_second_event_limit": 10
      },
      "max_limits": {
        "bytes_on_disk": 1073741824,
        "bytes_in_memory": 268435456,
        "events_emitted": 10000,
        "subscribers": 100,
        "rate_per_second_data_limit": 25,
        "rate_per_second_event_limit": 10
      }
    }
    ```

#### Set Limits

-   **Description**: Sets usage limits for a specific API key.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/admin/limits/set`
-   **Request Body**:
    ```json
    {
      "api_key": "string",
      "limits": {
        "bytes_on_disk": 1073741824,
        "bytes_in_memory": 268435456,
        "events_emitted": 10000,
        "subscribers": 100,
        "rate_per_second_data_limit": 25,
        "rate_per_second_event_limit": 10
      }
    }
    ```

#### Get Limits For Key

-   **Description**: Retrieves usage limits for a specific API key.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/admin/limits/get`
-   **Request Body**: `{"api_key": "string"}`
-   **Response Body**: See response format in **Get Limits** section below.

#### Get Ops Per Second

-   **Description**: Retrieves node-level operations-per-second metrics.
-   **Method**: `GET`
-   **Path**: `/db/api/v1/admin/metrics/ops`
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "op_vs": 12.34,
      "op_cache": 5.67,
      "op_events": 8.90,
      "op_subscribers": 1.23,
      "op_blobs": 4.56,
      "op_system": 7.89
    }
    ```

#### Get Entity

-   **Description**: Retrieves a single entity by its root API key.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/admin/insight/entity`
-   **Request Body**: `{"root_api_key": "string"}`
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "entity": {
        "root_api_key": "root-api-key-here",
        "aliases": ["alias1", "alias2"],
        "data_scope_uuid": "uuid-here",
        "key_uuid": "key-uuid-here",
        "usage": {
          "usage": { /* same structure as Get Limits */ },
          "max_limits": { /* same structure as Get Limits */ }
        }
      }
    }
    ```

#### Get Entities

-   **Description**: Retrieves a list of all entities.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/admin/insight/entities`
-   **Request Body**: `{"offset": 0, "limit": 100}`
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "entities": [
        {
          "root_api_key": "root-api-key-1",
          "aliases": ["alias1", "alias2"],
          "data_scope_uuid": "uuid-1",
          "key_uuid": "key-uuid-1",
          "usage": { /* same structure as Get Limits */ }
        },
        {
          "root_api_key": "root-api-key-2",
          "aliases": [],
          "data_scope_uuid": "uuid-2", 
          "key_uuid": "key-uuid-2",
          "usage": { /* same structure as Get Limits */ }
        }
      ]
    }
    ```

#### Get Entity By Alias

-   **Description**: Retrieves a single entity by its alias.
-   **Method**: `POST`
-   **Path**: `/db/api/v1/admin/insight/entity_by_alias`
-   **Request Body**: `{"alias": "string"}`
-   **Response Body (Success `200 OK`)**:
    ```json
    {
      "entity": {
        "root_api_key": "root-api-key-here",
        "aliases": ["alias1", "alias2"],
        "data_scope_uuid": "uuid-here",
        "key_uuid": "key-uuid-here",
        "usage": {
          "usage": { /* same structure as Get Limits */ },
          "max_limits": { /* same structure as Get Limits */ }
        }
      }
    }
    ```
