# Event & Subscription System

This document provides a comprehensive overview of the distributed real-time eventing and subscription system within `insi`. It details how events are published, how clients can subscribe to topics via WebSockets, and how the cluster manages subscription limits in a distributed fashion. The system is built on the Raft consensus protocol to ensure event delivery and consistent state management.

## 1. System Architecture

The event system consists of three primary components:

1.  **Event Publishing:** A mechanism for clients to publish arbitrary JSON data to a named topic. This is a leader-driven operation that ensures events are consistently logged and ordered by the Raft FSM.
2.  **Event Subscribing:** A WebSocket-based system allowing clients to connect to any node and listen for events on a specific topic. The system handles multiplexing events from the core to all interested WebSocket clients on that node.
3.  **Distributed Subscription Management:** A crucial backend component that coordinates subscription counts across the entire cluster. To enforce API key limits (e.g., "max 5 concurrent connections"), nodes must coordinate with the leader to atomically "reserve" and "release" subscription slots.

### 1.1. Event Structure

An event is a simple JSON object with a topic and a data payload.

```go
type Event struct {
    Topic string `json:"topic"`
    Data  any    `json:"data"`
}
```

### 1.2. Core Data Structures

The system uses several key data structures:

```go
// A session of someone connected wanting to receive events from one of the topics
type eventSession struct {
    conn    *websocket.Conn
    topic   string        // The topic this session is subscribed to
    send    chan []byte   // Buffered channel of outbound messages
    service *Core         // Service pointer to access logger, etc.
    keyUUID string        // The UUID of the API key that created the session
}

// Event subsystem that implements rft.EventReceiverIF
type eventSubsystem struct {
    service *Core
    eventCh chan models.Event
    // the Service will handle dispatch based on its eventSubscribers map
}

// Internal request structure for slot management
type subscriptionSlotRequest struct {
    KeyUUID string `json:"key_uuid"`
}
```

### 1.3. Tenant Scoping

To ensure data isolation, all topics are automatically and transparently prefixed with the `DataScopeUUID` of the authenticating API key. For example, if a client with data scope `entity-123` publishes to `user:updates`, the actual topic in the system becomes `entity-123:user:updates`. Likewise, subscribing to `user:updates` will only yield events published under that same data scope.

## 2. API Endpoints

The event system exposes the following HTTP endpoints:

### 2.1. Public Endpoints (Require API key authentication)

-   `POST /db/api/v1/events` - Publish a JSON payload to a topic.
-   `GET /db/api/v1/events/subscribe?topic=<topic>` - Establish a WebSocket connection to subscribe to a topic.

### 2.2. Internal Endpoints (Require root authentication)

These endpoints are used for inter-node communication to manage subscription counts and should not be used by end-users.

-   `POST /db/internal/v1/subscriptions/request_slot` - Used by a follower to ask the leader to grant a subscription slot.
-   `POST /db/internal/v1/subscriptions/release_slot` - Used by a follower to tell the leader a subscription slot has been freed.

## 3. Data Flow and Lifecycle

### 3.1. Publishing an Event

The process of publishing an event is leader-driven to ensure strong consistency.

1.  **Client Request:** A client sends an HTTP `POST` request with a JSON body to `/db/api/v1/events`. The body must contain `topic` and `data` fields.
2.  **Leader Redirection:** If the receiving node is not the Raft leader, it transparently redirects the client to the current leader.
3.  **Leader Processing:** The leader node performs the following steps:
    a. **Authentication & Rate Limiting:** Validates the API key and checks rate limits for event operations.
    b. **Quota Check:** It verifies if the owner's API key has exceeded its daily event publishing quota (`max_events`). The quota uses a sophisticated reset mechanism:
       - When the limit is first reached, a timestamp is recorded
       - If the limit is already reached, the system checks if 24 hours have passed since the timestamp
       - If 24+ hours have passed, both the usage counter and timestamp are reset, allowing new events
       - If still within 24 hours, the request is rejected with `400 Bad Request` and headers `X-Current-Events` and `X-Events-Limit`
    c. **Topic Scoping:** The leader prefixes the requested topic with the key's `DataScopeUUID`.
    d. **Raft Publish:** The leader commits the event to the Raft log using `fsm.Publish()`. This command, once applied, guarantees that the `Receive()` method of the `eventSubsystem` will be called on **every node** in the cluster with the event data.
    e. **Usage Tracking:** The leader atomically increments the daily event usage counter for the API key using `fsm.BumpInteger()`.
4.  **Event Fan-out:** On every node (including the leader), the received event is passed to a central dispatch channel (`eventCh`). A goroutine on each node is responsible for reading from this channel and forwarding the event data to any local WebSocket clients subscribed to the matching prefixed topic.

### 3.2. Subscribing to a Topic (The Slot-Request Flow)

The subscription process is more complex due to the need to globally enforce connection limits.

1.  **Client Request:** A client initiates a WebSocket upgrade handshake to `/db/api/v1/events/subscribe?topic=<topic>` on any node.
2.  **Authentication & Rate Limiting:** The node validates the API key and applies rate limits.
3.  **Global Connection Check:** The node first checks if the global WebSocket connection limit (`cfg.Sessions.MaxConnections`) has been reached. If so, it returns `503 Service Unavailable`.
4.  **Subscription Slot Request:** This is the critical distributed step.
    a. **If the node is the leader:** It atomically checks the API key's current subscription count against its limit using `getSubscriptionUsage()`. If space is available, it increments the count in the FSM using `fsm.BumpInteger()` and proceeds.
    b. **If the node is a follower:** It cannot safely check the limit itself. Instead, it makes an internal, root-authenticated HTTPS call to the leader's `/db/internal/v1/subscriptions/request_slot` endpoint. The leader performs the atomic check-and-increment on behalf of the follower and returns a success or failure response.
5.  **Connection Failure:** If the slot request is denied (either locally or by the leader), the node returns an HTTP `503 Service Unavailable` and the allocated slot is released.
6.  **WebSocket Upgrade:** If the slot was successfully granted, the node proceeds to upgrade the HTTP connection to a WebSocket connection using the configured `wsUpgrader`.
7.  **Session Registration:** A new `eventSession` is created and registered locally via `registerSubscriber()`. The node now knows to forward events for the scoped topic to this connection. Two goroutines, `readPump` and `writePump`, are started to manage the connection.

### 3.3. Unsubscribing & Disconnection

Disconnection handling is vital for freeing up subscription slots.

1.  **Connection Closure:** The `readPump` goroutine detects when the client's WebSocket connection is closed (either gracefully or unexpectedly).
2.  **Local Unregistration:** The `unregisterSubscriber` function is called, which:
    - Removes the session from the node's local topic-to-subscriber map (`eventSubscribers`)
    - Decrements the global WebSocket connection counter (`activeWsConnections`)
    - Closes the session's send channel
    - Removes empty topic maps if no more subscribers exist
3.  **Subscription Slot Release:** Crucially, the node then calls `releaseSubscriptionSlot()`:
    a. **If the node is the leader:** It simply decrements the API key's subscription count in the FSM using `fsm.BumpInteger()` with a value of -1.
    b. **If the node is a follower:** It spawns a separate goroutine to send a "fire-and-forget" internal HTTPS call to the leader's `/db/internal/v1/subscriptions/release_slot` endpoint. This tells the leader to decrement the count, freeing up the slot for future subscribers. This is truly fire-and-forget because the slot will eventually be freed; a failure here is logged but does not block the cleanup process.

## 4. WebSocket Connection Management

The system uses a robust WebSocket implementation with sophisticated connection lifecycle management.

### 4.1. Connection Lifecycle

-   **Keep-Alive:** The server sends periodic `Ping` messages to the client to keep the connection alive and detect unresponsive clients. The client is expected to respond with a `Pong` message.
-   **Pumps:** Each connection is managed by a `readPump` and a `writePump` running in separate goroutines:
    - **`readPump`:** Handles incoming messages (mostly for close detection), pong replies, and sets read deadlines. It automatically calls `unregisterSubscriber` and closes the connection when the loop exits.
    - **`writePump`:** Manages outgoing event data and pings using a ticker. It handles write deadlines and graceful connection closure.
-   **Graceful Shutdown:** When a connection is terminated or the service context is cancelled, all associated goroutines are cleaned up, and the `unregisterSubscriber` flow is triggered to release resources.

### 4.2. Message Handling

-   **Read Limits:** WebSocket connections have a maximum message size limit (`maxMessageSize`) and read deadlines are managed dynamically.
-   **Write Management:** The system uses a buffered send channel (`sendBufferSize`) for each session to handle outbound messages without blocking.
-   **Error Detection:** The system distinguishes between expected close errors and unexpected errors, logging appropriately.

## 5. Leader Address Resolution

When follower nodes need to communicate with the leader for slot management, they use a sophisticated address resolution mechanism:

1. Get leader info via `fsm.LeaderHTTPAddress()`
2. Parse the leader's private binding to extract the port
3. Use `localhost` as default host, but fall back to `ClientDomain` if configured
4. Construct HTTPS URLs for internal communication
5. Authenticate using the node's loopback client credentials

## 6. Security and Resilience

-   **Authentication:** All public endpoints require a valid API key. Internal slot-management endpoints require the cluster's root key.
-   **Data Scoping:** Topic prefixing ensures tenants cannot access each other's event streams.
-   **Rate Limiting & Quotas:** The system enforces limits on both the rate of publishing and the total number of events per day, as well as the number of concurrent WebSocket subscriptions. This protects the cluster from abuse.
-   **Leader Coordination:** Centralizing the management of finite resources (like subscription slots) on the leader is a standard pattern for achieving strong consistency in a distributed system. The system uses locking (`subscriptionSlotLock`) to ensure atomic operations.
-   **Connection Limits:** Dual-layer connection limiting with both global node limits and per-API-key subscription limits.

## 7. Error Handling and Status Codes

The system provides detailed error responses:

-   **401 Unauthorized:** Invalid or missing API key
-   **400 Bad Request:** Missing topic parameter or malformed JSON
-   **503 Service Unavailable:** Connection limits exceeded (global or per-key)
-   **500 Internal Server Error:** System errors (FSM failures, etc.)
-   **405 Method Not Allowed:** Wrong HTTP method for event publishing

When quota limits are exceeded, the system includes helpful headers:
-   `X-Current-Events`: Current event count for the API key
-   `X-Events-Limit`: Maximum events allowed for the API key

## 8. Constants and Configuration

-   `writeWait`: 10 seconds (time allowed for a write to complete)
-   `pongWait`: 60 seconds (time allowed for a client to respond to a ping)
-   `pingPeriod`: 54 seconds (frequency of pings, calculated as `(pongWait * 9) / 10`)
-   `maxMessageSize`: 512 bytes (maximum WebSocket message size)
-   `sendBufferSize`: 256 (buffer size for the send channel per session)
-   Global Configuration:
    -   `cfg.Sessions.MaxConnections`: Maximum total WebSocket connections per node
-   API Key Quotas (Configured by administrators):
    -   `max_events`: The maximum number of events that can be published in a 24-hour rolling window
    -   `max_subscriptions`: The maximum number of concurrent WebSocket subscriptions allowed per API key

## 9. Event Reception and Dispatch

The `eventSubsystem` implements the `rft.EventReceiverIF` interface:

-   **`Receive()` Method:** Called once per node per event when the FSM applies an event. It places the event on the service's central event channel (`eventCh`).
-   **Central Dispatch:** A separate goroutine reads from `eventCh` and forwards events to all local WebSocket subscribers for matching topics.
-   **Metrics:** Each event reception increments operational metrics via `IndEventsOp()`.

## 10. Subscription Slot Management

The distributed slot management system ensures global consistency:

-   **Atomic Operations:** All slot reservations and releases use atomic FSM operations
-   **Leader Coordination:** Only the leader can make slot decisions to prevent race conditions
-   **Graceful Degradation:** Follower-to-leader communication failures are logged but don't prevent cleanup
-   **Usage Tracking:** The system tracks both current usage and limits using key-value pairs in the FSM
