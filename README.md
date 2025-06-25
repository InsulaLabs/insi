# Insi 

Insi is a high-performance, distributed data platform designed for developers who need a scalable and secure backend for their applications. It provides a simple yet powerful set of features, including persistent key-value storage, volatile caching, and a real-time eventing system, all accessible through a unified HTTP API.

Built with Go, Insi is designed to be run as a cluster of nodes, providing fault tolerance and high availability through the Raft consensus algorithm. It features a unique data scoping model based on "Entities" and API keys, allowing for secure multi-tenancy and granular control over resource usage.

## Core Concepts

### Entities & API Keys

The fundamental unit of data isolation in Insi is the **Entity**. Think of an Entity as a dedicated data scope or a workspace for your application. Each Entity you create is assigned a unique API key. All data operations—whether to the Value Store, Cache Store, or Events system—are automatically namespaced to the Entity associated with the API key used for the request.

This model provides several key benefits:
- **Multi-Tenancy**: You can run multiple independent applications on the same Insi cluster, each with its own Entity, without worrying about data collisions.
- **Security**: An API key for one Entity cannot access data belonging to another.
- **Resource Management**: Usage limits (like storage and event quotas) are applied on a per-Entity basis, allowing you to control the resources consumed by each application.

An administrative **Root API Key** is used to manage the cluster and create new Entities. This key is derived from the `instanceSecret` you define in your `cluster.yaml` configuration.

### Data Models & HTTP Endpoints

Insi offers three primary data services. All endpoints are prefixed with `/db/api/v1`.

#### 1. Value Store (Persistent)

The Value Store provides durable, persistent key-value storage. It's ideal for storing user data, configuration, state, and any other information that needs to survive server restarts.

**Endpoints:**
- `POST /set`: Sets a key-value pair.
- `POST /get`: Retrieves the value for a given key.
- `POST /delete`: Deletes a key-value pair.
- `POST /setnx`: "Set if Not Exists." Sets a key only if it does not already exist.
- `POST /cas`: "Compare and Swap." Updates a key's value only if the current value matches a provided value.
- `POST /bump`: Atomically increments or decrements a numeric value. Flooring at 0.
- `POST /iterate/prefix`: Iterates over keys with a given prefix, returning all keys containing the prefix.

#### 2. Cache Store (Volatile)

The Cache Store is a volatile, in-memory key-value store designed for high-speed, temporary data. Data stored in the cache is maintained on restarts utilizing the
raft snapshots, but it should still be considered volatile in usage.

**Endpoints:**
- `POST /cache/set`: Sets a key-value pair in the cache.
- `POST /cache/get`: Retrieves a value from the cache.
- `POST /cache/delete`: Deletes a value from the cache.
- `POST /cache/setnx`: Sets a cache key only if it does not already exist.
- `POST /cache/cas`: Compare and swaps a value in the cache.
- `POST /cache/iterate/prefix`: Iterates over cache keys with a given prefix, returning all keys containing the prefix.

#### 3. Events (Pub/Sub)

Insi includes a real-time eventing system that allows for powerful publish/subscribe communication patterns. Clients can subscribe to topics and receive messages in real-time as they are published by other clients.

**Endpoints:**
- `POST /events`: Publishes a message to a specific topic.
- `GET /events/subscribe`: Establishes a WebSocket connection to subscribe to a topic.

### Limits and Metering

Insi provides a robust system for controlling resource consumption. Limits are managed at two levels:

1.  **Global Rate Limiting**: Configured in `cluster.yaml`, these serve as a baseline to control the overall number of requests per second for major service categories (`values`, `cache`, `events`, `system`).

2.  **Per-Key Quotas & Rate Limits**: These are granular limits applied to each individual API key, allowing for fine-grained control. When a rate limit is exceeded, the API will respond with a `429 Too Many Requests` status code, including a `Retry-After` header. These quotas can be managed by an administrator and include:
    - `BytesOnDisk`: The total size of data the Entity can store in the persistent Value Store.
    - `BytesInMemory`: The total size of data the Entity can store in the volatile Cache Store.
    - `EventsEmitted`: The total number of events an Entity can publish.
    - `Subscribers`: The maximum number of concurrent event subscribers an Entity can have.
    - `RPSDataLimit`: The number of data-plane requests per second the key can make.
    - `RPSEventLimit`: The number of event-plane requests per second the key can make.

    The usage against these quotas is tracked in real-time. The `EventsEmitted` quota is designed to reset on a periodic basis.

#### Managing Limits via API

Resource limits for individual keys can be managed through the following endpoints:

-   **Set Limits**: `POST /db/api/v1/admin/limits/set` (Admin only)
-   **View Your Limits**: `GET /db/api/v1/limits` (Any key)
-   **View Specific Key's Limits**: `POST /db/api/v1/admin/limits/get` (Admin only)

## Getting Started

### Configuration

The cluster is configured using a `cluster.yaml` file. This file defines the nodes in the cluster, their addresses, security settings, rate limits, and other operational parameters. A well-documented example can be found at `cluster.yaml` in the root of this repository.

### Running a Server

The main application for running an Insi node is `insid`. 

```bash
# Start a multi-node cluster as defined in cluster.yaml
./insid --host --config cluster.yaml
```

### Interacting with Insi

#### `insic` CLI Tool

- Start a server (`insid server` (Above)).
Use insic "see `insic --help`"

#### `fwi` Go Library

For programmatic access from your Go applications, the `fwi` library (`fwi/fwi.go`) provides a high-level, developer-friendly interface. It abstracts away the direct client communication and simplifies the management of Entities and their associated data stores.

A comprehensive example of how to use the `fwi` library to configure a cluster, create an Entity, and use the Value, Cache, and Events stores can be found in `examples/fwi-usage/main.go`.

## Development & Testing

To see how Insi performs under pressure, you can use the `fwit-t` stress testing tool located in `cmd/fwit-t/main.go`. This tool spins up a test cluster, creates multiple entities, and bombards the server with a configurable mix of operations, printing a detailed metrics summary at the end. It's an excellent example of advanced `fwi` usage and cluster testing.

Example:

```bash

./fwit-t --cluster-size 1 --entities 10 --duration 1m                 
📂 Created temporary directory for cluster data at /tmp/insi-stress-test-1417733845
📜 Generated cluster configuration: /tmp/insi-stress-test-1417733845/cluster.yaml
⏳ Waiting for cluster to initialize and for root trackers to be set... (15s)

```

This example of the test shows a local spin-up of a 1-node cluster (you most likely wont need more unless you want to see insi in all its glory
or need an honsest distributed eventually consistent data-store and distribute insi over real nodes.)

Feel free to scale up the entities to something silly. Idk the useful upper limit but I ran fine locally with 200 entities for a duration of 30m with the chaos monkey feature enabled. 

Note: if chaos monkey rug-pulls an entity from the DB the end result will show a fire emoji and the number of failures it experienced as the test client continues attempting to make requests as an entity that doesn't exist anymore.
When this occurs you'll see errors regarding "tombstones" until the tombstone runner executes to garble up all the
deleted entity's data. Once this occurs, the key is hard-deleted and the errors will morph into "unknown api key."

```log
"time":"2025-06-17T16:21:05.449730248-04:00","level":"INFO","msg":"WebSocket writePump finished","service":"insidRuntime","node":"node0","service":{"remote_addr":{"IP":"127.0.0.1","Port":58208,"Zone":""},"topic":"aefcc389-afbb-4652-8068-e8a485b8bf82:stress-topic"}}
✅ Stress test finished.

--- 📊 Stress Test Summary ---
Entity          | Sets (avg)       | Gets (avg)       | Deletes (avg)    | Caches (avg)     | Publishes (avg)  | Bumps (avg)      | Failures  
--------------------------------------------------------------------------------------------------------------------------------------------------
entity-3        | 69    (5.930659ms) | 73    (480.591µs) | 67    (5.360793ms) | 89    (6.536629ms) | 71    (7.047386ms) | 79    (5.859257ms) | 0         
entity-7        | 71    (5.571089ms) | 78    (535.703µs) | 68    (5.917301ms) | 68    (6.493481ms) | 80    (6.361145ms) | 77    (5.90206ms) | 0         
entity-4        | 69    (5.892177ms) | 81    (470.662µs) | 69    (6.166047ms) | 80    (5.884271ms) | 72    (6.141769ms) | 82    (5.729615ms) | 0         
entity-9        | 60    (6.047571ms) | 76    (495.317µs) | 60    (6.189402ms) | 86    (6.399739ms) | 87    (6.264928ms) | 63    (5.726122ms) | 0         
entity-8        | 72    (6.539324ms) | 74    (474.951µs) | 61    (5.462008ms) | 84    (6.709667ms) | 90    (6.535239ms) | 71    (6.139155ms) | 0         
entity-0        | 79    (5.490788ms) | 63    (493.465µs) | 62    (5.915106ms) | 84    (5.954539ms) | 77    (5.784273ms) | 90    (5.678164ms) | 0         
entity-5        | 79    (6.052522ms) | 74    (457.014µs) | 65    (5.631906ms) | 81    (6.773045ms) | 79    (5.764073ms) | 74    (6.557542ms) | 0         
entity-1        | 77    (6.04764ms) | 72    (488.269µs) | 72    (6.127342ms) | 74    (5.482665ms) | 73    (6.823378ms) | 80    (5.675574ms) | 0         
entity-6 🔥      | 19    (6.850591ms) | 30    (516.897µs) | 19    (5.890368ms) | 29    (6.727713ms) | 24    (7.768916ms) | 25    (5.128697ms) | 258       
entity-2        | 74    (6.155249ms) | 88    (486.24µs) | 65    (5.667509ms) | 82    (5.774498ms) | 76    (6.77848ms) | 69    (5.985519ms) | 0         
--------------------------------------------------------------------------------------------------------------------------------------------------
TOTALS          | 669   (5.991402ms) | 709   (488.21µs) | 608   (5.83197ms) | 757   (6.249902ms) | 729   (6.426831ms) | 710   (5.880776ms) | 258       

--- 📡 Subscriber Summary ---
📡 Subscriber 0 (listening to entity-1) received 73 events
📡 Subscriber 1 (listening to entity-7) received 80 events
📡 Total Events Received by All Subscribers: 153

🛑 Shutting down cluster...
{"time":"2025-06-17T16:21:05.552313833-04:00","level":"INFO","msg":"Runtime stop requested.","service":"insidRuntime"}
{"time":"2025-06-17T16:21:05.552339848-04:00","level":"INFO","msg":"Shutdown signal received or all services completed. Exiting host mode.","service":"insidRuntime"}
✅ Cluster shut down successfully.
🧹 Cleaning up temporary directory: /tmp/insi-stress-test-1417733845
{"time":"2025-06-17T16:21:05.552448134-04:00","level":"INFO","msg":"Service event processing loop shutting down","service":"insidRuntime","node":"node0"}
{"time":"2025-06-17T16:21:05.552539848-04:00","level":"INFO","msg":"Waiting for server to stop - this may take a moment","service":"insidRuntime","node":"node0"}
{"time":"2025-06-17T16:21:05.55264852-04:00","level":"INFO","msg":"Server stopped","service":"insidRuntime","node":"node0"}
badger 2025/06/17 16:21:05 INFO: Lifetime L0 stalled for: 0s
```

    Note: The speeds shown obviously are only reflective of the system I was on, the fact that its local, and that it was only a single node.

# Development

## TESTING

If you look in `tests/run-all.sh` you'll see how I started testing. I started off by just making bash scripts to run
against the `insic` command line application to ensure each route was tested and we got the expected values.
Bash scripts made testing the node cluster really simple as we could just spin them up in the configuration we needed
and then had direct access to logs.

Eventually the complexities grew but it capped off at a manageable level and now rest in their current form.


# DB Implementation

The backend of Insi is a custom-built distributed database system designed for high performance and fault tolerance.

-   **Storage Engine**: At its core, Insi uses **BadgerDB**, an embeddable, persistent, and fast key-value store written purely in Go. This allows for direct, efficient data storage without the overhead of a separate database server process.

-   **Distributed Consensus**: To ensure data consistency and high availability across multiple nodes, Insi implements the **Raft consensus algorithm**, leveraging the robust `hashicorp/raft` library. All write operations (like setting a value, creating an API key, or publishing an event) are not directly written to disk. Instead, they are submitted as commands to the Raft log.

-   **Finite State Machine (FSM)**: The Raft leader distributes these log entries to its followers. Each node in the cluster then processes the log entries through a **Finite State Machine (FSM)**. The FSM's `Apply` function is the single, serialized point where commands are executed against the local BadgerDB instance. This mechanism guarantees that every node in the cluster applies the same operations in the same order, resulting in a replicated and consistent state across the entire cluster.

-   **Dual-Store Model**: Insi maintains two separate BadgerDB instances:
    1.  A **persistent, on-disk store** for the durable Value Store.
    2.  A **volatile, in-memory store** for the high-speed Cache Store. This provides the performance benefits of an in-memory cache while still leveraging Badger's efficient engine and Raft's snapshotting capabilities for warm restarts.

-   **Abstraction Layers**:
    -   The `tkv` package provides a foundational abstraction over BadgerDB, offering the primitive operations (`Get`, `Set`, `SetNX`, `CompareAndSwap`, `BumpInteger`) used by the rest of the system.
    -   The `rft` package contains the FSM implementation, managing the interaction between the Raft consensus layer and the `tkv` storage layer. It handles command processing, snapshotting for log compaction, and cluster membership.
    -   The `core` package serves as the API layer, validating requests and dispatching write operations to the Raft FSM.

## Building

The project includes a comprehensive `Makefile` to simplify the build process. All compiled binaries and necessary assets are placed in the `build/` directory.

### Main Commands

-   `make all`: (Default) Compiles all development binaries:
    -   `insid`: The Insi server daemon.
    -   `insic`: The legacy command-line client.
    -   `fwit-t`: The stress testing tool.
-   `make prod`: Compiles all binaries optimized for production (smaller and stripped of debug information).
-   `make test`: Runs the full Go test suite for all packages.
-   `make clean`: Removes the `build/` directory and all compiled artifacts.

### Individual Components

You can also build each component individually for both development and production environments:

-   **Server**: `make server` or `make server-prod`
-   **Client**: `make client` or `make client-prod`
-   **Stress Test Tool**: `make fwit` or `make fwit-prod`

# Testing

The bash tests don't require you to launch a cluster yourself, it handles everything
as long as `make build` has been run, then you can simply:

```
cd tests && bash run-all.sh
```

I recommend:

```
bash run-all.sh > out.log
```