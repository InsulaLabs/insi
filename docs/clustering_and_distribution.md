# Clustering and Distributed System Architecture

InsiDB is built as a distributed database system using the Raft consensus protocol to ensure strong consistency, fault tolerance, and high availability across multiple nodes. This document explains the clustering architecture, node management, and distributed operations.

## Architecture Overview

### Raft Consensus Protocol

InsiDB uses HashiCorp's Raft implementation to maintain a distributed state machine across all cluster nodes. This ensures:

- **Strong Consistency**: All writes are coordinated through a single leader
- **Fault Tolerance**: The cluster can survive node failures as long as a majority remains
- **Data Durability**: All changes are logged and replicated before being applied

### Node Types and Roles

#### Leader Node
- Handles all write operations and administrative functions
- Coordinates data replication to follower nodes
- Makes decisions about cluster membership changes
- Processes all FSM (Finite State Machine) operations

#### Follower Nodes
- Accept read operations (with potential for slight staleness)
- Automatically redirect write operations to the leader
- Participate in leader election when needed
- Replicate data from the leader

#### Default Leader
- The initial leader node specified in cluster configuration
- Bootstraps the cluster on first startup
- Other nodes automatically attempt to join this leader

## Cluster Configuration

### Node Configuration Structure

Each node in the cluster requires specific configuration:

```yaml
# Example cluster configuration
default_leader: "node0"
nodes:
  node0:
    public_binding: "0.0.0.0:8080"
    private_binding: "0.0.0.0:8081"
    raft_binding: "0.0.0.0:7000"
    client_domain: "node0.example.com"
  node1:
    public_binding: "0.0.0.0:8082"
    private_binding: "0.0.0.0:8083"
    raft_binding: "0.0.0.0:7001"
    client_domain: "node1.example.com"
```

### Binding Types

- **Public Binding**: Client-facing API endpoints
- **Private Binding**: Administrative and internal cluster communication
- **Raft Binding**: Raft protocol communication between nodes
- **Client Domain**: Optional domain name for client connections

## Auto-Join Mechanism

### How Auto-Join Works

Non-leader nodes automatically attempt to join the cluster on startup:

1. **Leader Discovery**: Node identifies the default leader from configuration
2. **Connection Attempt**: Establishes connection to leader's private binding
3. **Authentication**: Uses cluster instance secret for authentication
4. **Join Request**: Sends join request with its node ID and Raft address
5. **Cluster Integration**: Leader adds the node to the Raft configuration

### Auto-Join Process Flow

```
[Follower Node Startup]
        ↓
[Check if already in cluster]
        ↓
[If not in cluster]
        ↓
[Resolve leader address]
        ↓
[Send authenticated join request]
        ↓
[Leader processes join]
        ↓
[Node added to cluster]
```

### Auto-Join Configuration

The auto-join process respects several configuration options:

```yaml
# TLS Configuration for auto-join
tls:
  cert: "/path/to/cert.pem"
  key: "/path/to/key.pem"

# Skip TLS verification (development only)
client_skip_verify: true

# Cluster authentication
instance_secret: "your-cluster-secret"
```

## Manual Cluster Management

### Manual Node Join

For controlled cluster expansion or maintenance scenarios:

**Endpoint**: `GET /db/api/v1/join?followerId=<nodeId>&followerAddr=<raftAddr>`

**Example**:
```bash
curl -H "Authorization: Bearer <root_token>" \
  "https://leader:8081/db/api/v1/join?followerId=node2&followerAddr=192.168.1.100:7002"
```

### Cluster Membership

Check current cluster configuration programmatically or through Raft logs to see:
- Active nodes in the cluster
- Current leader
- Node health status
- Raft log status

## Leader Election and Failover

### Automatic Leader Election

When the current leader becomes unavailable:

1. **Detection**: Followers detect leader failure through heartbeat timeout
2. **Election**: Remaining nodes initiate leader election
3. **Voting**: Nodes vote for a new leader based on log completeness
4. **New Leader**: Node with majority votes becomes the new leader
5. **Client Redirection**: Clients are automatically redirected to new leader

### Client Redirection

When a client contacts a follower for a write operation:

1. **Request Received**: Follower receives write request
2. **Leader Resolution**: Follower determines current leader address
3. **Redirect Response**: Returns `307 Temporary Redirect` with leader URL
4. **Client Retry**: Client automatically retries request against leader

**Example Redirect Response**:
```http
HTTP/1.1 307 Temporary Redirect
Location: https://leader.example.com:8080/db/api/v1/set
```

## Data Replication and Consistency

### Write Operations

All write operations follow this pattern:

1. **Leader Validation**: Ensure operation is sent to current leader
2. **Raft Log Entry**: Create log entry for the operation
3. **Replication**: Replicate log entry to majority of followers
4. **Commitment**: Mark log entry as committed once majority confirms
5. **FSM Application**: Apply operation to finite state machine
6. **Response**: Return success to client

### Read Operations

Read operations can be handled by any node:

- **Leader Reads**: Always consistent, no additional latency
- **Follower Reads**: May have slight staleness but lower latency
- **Consistency Guarantee**: Eventual consistency across all nodes

## Network Partitions and Split-Brain Prevention

### Quorum Requirements

- **Minimum Cluster Size**: 3 nodes recommended for production
- **Quorum**: Majority of nodes (N/2 + 1) required for operations
- **Split-Brain Prevention**: No operations can proceed without quorum

### Partition Scenarios

#### Network Partition Examples

**3-Node Cluster**:
- Partition: [Node1] vs [Node2, Node3]
- Result: Majority partition (2 nodes) remains operational
- Minority partition (1 node) becomes read-only

**5-Node Cluster**:
- Partition: [Node1, Node2] vs [Node3, Node4, Node5]  
- Result: Majority partition (3 nodes) remains operational
- Minority partition (2 nodes) becomes read-only

## Internal Communication

### Private API Endpoints

Internal cluster coordination uses private endpoints:

```
/db/internal/v1/blob/download           - Blob replication
/db/internal/v1/subscriptions/request_slot  - Subscription management
/db/internal/v1/subscriptions/release_slot  - Subscription cleanup
```

### Authentication for Internal APIs

Internal communication uses the same cluster instance secret but requires additional validation:

- **Root Key Authentication**: Internal APIs require root-level access
- **IP Filtering**: Limited to configured permitted IPs
- **TLS Encryption**: All internal communication encrypted

## Operational Considerations

### Cluster Sizing Guidelines

| Cluster Size | Fault Tolerance | Use Case |
|--------------|-----------------|----------|
| 1 Node | None | Development only |
| 3 Nodes | 1 node failure | Small production |
| 5 Nodes | 2 node failures | Medium production |
| 7+ Nodes | 3+ node failures | Large production |

### Performance Characteristics

- **Write Latency**: Increases with cluster size (more replication)
- **Read Latency**: Can be optimized using follower reads
- **Network Bandwidth**: Scales with cluster size and write volume
- **Storage**: Each node stores complete dataset

### Monitoring Cluster Health

Monitor these metrics for cluster health:

1. **Leader Stability**: Frequency of leader elections
2. **Replication Lag**: Time between leader and follower state
3. **Network Partitions**: Connection failures between nodes
4. **Disk Usage**: Raft log growth over time
5. **Memory Usage**: FSM state size across nodes

## Troubleshooting

### Common Issues

#### Auto-Join Failures
- **Cause**: Network connectivity, authentication, or configuration issues
- **Solution**: Check network connectivity, verify instance secret, review logs

#### Split-Brain Detection
- **Cause**: Network partitions preventing quorum
- **Solution**: Restore network connectivity or manually recover from backup

#### Leader Election Loops
- **Cause**: Network instability or resource constraints
- **Solution**: Investigate network stability, check node resources

### Recovery Procedures

#### Complete Cluster Loss
1. Identify most recent backup
2. Restore data to new leader node
3. Bootstrap new cluster
4. Add follower nodes one by one

#### Partial Node Loss
1. Nodes will auto-recover when connectivity restored
2. Use manual join if auto-join fails
3. Monitor replication to ensure consistency

## Security Considerations

### Cluster Authentication
- **Instance Secret**: Shared secret for cluster authentication
- **TLS Encryption**: All inter-node communication encrypted
- **Network Security**: Isolate cluster traffic on private networks

### Access Control
- **IP Filtering**: Restrict access to administrative functions
- **Certificate Validation**: Verify node identities in production
- **Audit Logging**: Track all cluster membership changes

## Best Practices

1. **Always use odd number of nodes** for clear quorum decisions
2. **Deploy across availability zones** for geographic fault tolerance
3. **Monitor network latency** between nodes for performance
4. **Regular backup procedures** for disaster recovery
5. **Gradual scaling** when adding/removing nodes
6. **Network security** with firewalls and VPNs for cluster traffic 