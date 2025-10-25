# Insi

A high-performance, distributed data platform that provides persistent storage, caching, and real-time events through a unified HTTP API. Built for developers who need a scalable, secure backend without operational complexity.

## What is Insi?

Insi is a distributed database designed to replace multiple services with a single, coherent platform:

- **Key-Value Store**: Durable, persistent storage for your application data
- **Cache**: High-speed, in-memory storage for temporary data and performance optimization  
- **Events**: Real-time pub/sub messaging for live updates and coordination
- **Multi-Tenancy**: Secure data isolation using API keys and "Entities"
- **Clustering**: Built-in Raft consensus for fault tolerance and high availability

Perfect for web applications, microservices, IoT systems, and any project needing a reliable data backend.

## Quick Start

### 1. Build Insi

```bash
git clone https://github.com/InsulaLabs/insi
cd insi
make all
```

This creates three binaries in `build/`:
- `insid` - The database server
- `insic` - Command-line client  

### 2. Start a Single Node

```bash
# Start server with default configuration
./build/insid --host --config cluster.yaml
```

### 3. Get Your Root API Key

```bash
# Extract the root key from your cluster configuration
grep instanceSecret cluster.yaml
# Use this to derive your administrative key
```

### 4. Create Your First Entity (API Key)

```bash
# Create a new API key for your application
./build/insic admin create-key myapp
```

### 5. Store and Retrieve Data

```bash
# Set a value
curl -X POST http://localhost:8080/db/api/v1/set \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"key": "user", "value": "Alice"}'

# Get it back
curl "http://localhost:8080/db/api/v1/get?key=user" \
  -H "Authorization: Bearer YOUR_API_KEY"
```

## Core Concepts

### Entities & Data Isolation

Every application using Insi gets its own **Entity** with a unique API key. Think of an Entity as a completely isolated workspace:

- **Automatic Namespacing**: Your data can't collide with other applications
- **Resource Limits**: Control memory, storage, and request rates per Entity
- **Security**: API keys for one Entity cannot access another's data

### Three Data Services

All accessible via HTTP endpoints at `/db/api/v1`:

#### 1. **Value Store** (Persistent)
```bash
POST /set          # Store key-value pairs permanently
POST /get          # Retrieve values
POST /delete       # Remove keys
POST /cas          # Compare-and-swap for atomic updates
POST /bump         # Atomic integer increment/decrement
POST /iterate/prefix  # List keys by prefix
```

#### 2. **Cache** (Fast, Volatile)
```bash
POST /cache/set    # Store in high-speed memory
POST /cache/get    # Retrieve from cache
POST /cache/delete # Remove from cache
# Plus: /cache/cas, /cache/setnx, /cache/iterate/prefix
```

#### 3. **Events** (Real-time Pub/Sub)
```bash
POST /events           # Publish messages to topics
GET  /events/subscribe # WebSocket subscription to topics
POST /events/purge     # Remove all subscribers from the specific node
POST /events/shake     # Lock all subscription slots cluster-wide and drop all current subscribers, resets sub count
```

### Built-in Resource Management

Every Entity automatically gets:
- **Storage Quotas**: Separate limits for persistent and cache storage
- **Rate Limiting**: Configurable requests per second
- **Event Limits**: Publish/subscribe quotas
- **Connection Limits**: Maximum concurrent subscribers

## Examples

### Go Application with `fwi` Library

```go
package main

import (
    "github.com/InsulaLabs/insi/pkg/fwi"
    "log"
)

func main() {
    // Connect to cluster
    cluster := fwi.Connect("http://localhost:8080", "your-api-key")
    
    // Use persistent storage
    err := cluster.Values.Set("user:123", "Alice")
    if err != nil {
        log.Fatal(err)
    }
    
    value, err := cluster.Values.Get("user:123")
    if err != nil {
        log.Fatal(err)
    }
    
    // Use fast cache
    cluster.Cache.Set("session:abc", "user:123")
    
    // Publish real-time events
    cluster.Events.Publish("user-updates", "User Alice logged in")
}
```

### JavaScript/Node.js

```javascript
const apiKey = 'your-api-key';
const baseUrl = 'http://localhost:8080/db/api/v1';

// Store data
await fetch(`${baseUrl}/set`, {
  method: 'POST',
  headers: {
    'Authorization': `Bearer ${apiKey}`,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    key: 'user:123:profile',
    value: JSON.stringify({ name: 'Alice', email: 'alice@example.com' })
  })
});

// Subscribe to events
const ws = new WebSocket(`ws://localhost:8080/db/api/v1/events/subscribe?topic=notifications`);
ws.onmessage = (event) => {
  console.log('Received:', event.data);
};
```

## Documentation

Comprehensive documentation is available in the [`docs/`](docs/) directory:

- **[Getting Started Guide](docs/operational_guide.md)** - Detailed setup and configuration
- **[API Keys & Security](docs/api_keys.md)** - Authentication and entity management  
- **[Value Store & Cache](docs/value_store_and_cache.md)** - Data storage APIs
- **[Events System](docs/events.md)** - Real-time messaging
- **[Clustering](docs/clustering_and_distribution.md)** - Multi-node deployment
- **[Admin APIs](docs/admin_and_system_apis.md)** - System management
- **[Rate Limiting](docs/rate_limiting_and_security.md)** - Resource controls

## Production Deployment

### Multi-Node Cluster

Configure a `cluster.yaml` for high availability:

```yaml
default_leader: "node0"
instance_secret: "your-secure-secret"
nodes:
  node0:
    public_binding: "0.0.0.0:8080"
    private_binding: "0.0.0.0:8081" 
    raft_binding: "0.0.0.0:7000"
  node1:
    public_binding: "0.0.0.0:8082"
    private_binding: "0.0.0.0:8083"
    raft_binding: "0.0.0.0:7001"
  # Add more nodes...
```

Start each node:
```bash
./insid --host --config cluster.yaml
```

Follower nodes automatically join the cluster using Raft consensus.

### Resource Limits & Monitoring

Monitor your deployment:
```bash
# Check cluster health
curl -H "Authorization: Bearer $ROOT_KEY" \
  http://localhost:8081/db/api/v1/admin/metrics/ops

# View entity resource usage  
curl -H "Authorization: Bearer $ROOT_KEY" \
  http://localhost:8081/db/api/v1/admin/insight/entities
```

## Development & Testing

### Stress Testing

Test your cluster under load:

```bash
./build/fwit-t --cluster-size 3 --entities 50 --duration 5m
```

This spins up a temporary cluster, creates test entities, and runs mixed workloads while reporting performance metrics.

### Running Tests

```bash
# Go unit tests
make test

# Integration tests
cd tests && bash run-all.sh
```

## Why Choose Insi?

- **Simplicity**: One API for storage, caching, and events instead of managing multiple services
- **Performance**: Built in Go with BadgerDB for speed and efficiency
- **Reliability**: Raft consensus ensures data consistency and fault tolerance  
- **Security**: Multi-tenant by design with comprehensive rate limiting
- **Operational**: Self-contained binary with minimal dependencies
- **Scalable**: Add nodes dynamically with automatic cluster joining

## Building

The project includes a comprehensive `Makefile`:

```bash
make all      # Build development binaries
make prod     # Build optimized production binaries  
make test     # Run test suite
make clean    # Clean build artifacts
```

Binaries are created in the `build/` directory.

## Architecture

Insi implements a custom distributed database:

- **Storage Engine**: BadgerDB for fast, embedded key-value storage
- **Consensus**: Raft protocol via HashiCorp's implementation  
- **Replication**: All writes go through Raft log for consistency
- **State Machine**: Finite State Machine applies operations in order
- **Dual Storage**: Separate persistent and volatile stores
- **Clustering**: Automatic node discovery and leader election
