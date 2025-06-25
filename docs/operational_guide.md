# Operational Guide and Advanced Features

This guide covers operational aspects, internal APIs, maintenance procedures, and advanced features of InsiDB that are essential for system administrators and operators but may not be immediately obvious from the basic API documentation.

## Internal APIs and Cluster Coordination

### Overview

InsiDB uses internal APIs for cluster coordination and distributed operations. These endpoints are not intended for direct client use but are essential for understanding system behavior and troubleshooting.

### Internal Blob Management

#### Internal Blob Download

**Endpoint**: `POST /db/internal/v1/blob/download`  
**Authentication**: Root key via private binding  
**Purpose**: Inter-node blob replication and synchronization

**Use Cases**:
- Automatic blob replication across cluster nodes
- Blob consistency maintenance
- Recovery operations for corrupted blob data

### Internal Subscription Management

#### Request Subscription Slot

**Endpoint**: `POST /db/internal/v1/subscriptions/request_slot`  
**Authentication**: Root key via private binding  
**Purpose**: Distributed subscription limit enforcement

**Request Body**:
```json
{
  "key_uuid": "api-key-uuid-here"
}
```

**Process Flow**:
1. Follower node receives WebSocket subscription request
2. Follower cannot safely check subscription limits locally
3. Follower sends internal request to leader for slot reservation
4. Leader atomically checks and increments subscription count
5. Leader responds with success/failure
6. Follower proceeds or rejects based on leader response

#### Release Subscription Slot

**Endpoint**: `POST /db/internal/v1/subscriptions/release_slot`  
**Authentication**: Root key via private binding  
**Purpose**: Clean up subscription slots when connections close

**Characteristics**:
- **Fire-and-forget**: Failures are logged but don't block cleanup
- **Asynchronous**: Runs in separate goroutine to avoid blocking
- **Eventually consistent**: Slots will be freed even if initial request fails

## Advanced Value Store Operations

### Compare-and-Swap (CAS) Operations

Beyond basic set/get operations, InsiDB supports atomic compare-and-swap for both value store and cache:

#### Value Store CAS

**Endpoint**: `POST /db/api/v1/cas`

**Use Cases**:
- Implementing optimistic locking
- Building atomic counters
- Creating distributed mutexes
- Coordinating between multiple clients

**Example - Atomic Counter**:
```javascript
async function incrementCounter(key) {
  let attempts = 0;
  const maxAttempts = 10;
  
  while (attempts < maxAttempts) {
    try {
      // Get current value
      const response = await fetch(`/db/api/v1/get?key=${key}`);
      const currentValue = response.ok ? 
        parseInt(await response.json().data) : 0;
      
      // Attempt atomic increment
      const casResponse = await fetch('/db/api/v1/cas', {
        method: 'POST',
        headers: {
          'Authorization': 'Bearer ' + apiKey,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          key: key,
          old_value: currentValue.toString(),
          new_value: (currentValue + 1).toString()
        })
      });
      
      if (casResponse.status === 200) {
        return currentValue + 1; // Success
      } else if (casResponse.status === 409) {
        attempts++;
        continue; // Retry with new value
      } else {
        throw new Error('CAS operation failed');
      }
    } catch (error) {
      attempts++;
      if (attempts >= maxAttempts) throw error;
    }
  }
}
```

#### Cache CAS

**Endpoint**: `POST /db/api/v1/cache/cas`

Similar to value store CAS but operates on in-memory cache data.

### SetNX (Set if Not Exists) Operations

Atomic "set if not exists" operations for both storage types:

#### Value Store SetNX

**Endpoint**: `POST /db/api/v1/setnx`

**Use Cases**:
- Acquiring distributed locks
- Ensuring unique resource creation
- Implementing leader election patterns

**Example - Distributed Lock**:
```javascript
class DistributedLock {
  constructor(lockKey, ttl = 30000) {
    this.lockKey = lockKey;
    this.ttl = ttl;
    this.lockValue = `${Date.now()}-${Math.random()}`;
  }
  
  async acquire() {
    try {
      const response = await fetch('/db/api/v1/setnx', {
        method: 'POST',
        headers: {
          'Authorization': 'Bearer ' + apiKey,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          key: this.lockKey,
          value: this.lockValue
        })
      });
      
      return response.status === 200;
    } catch (error) {
      return false;
    }
  }
  
  async release() {
    // Use CAS to safely release only if we own the lock
    const response = await fetch('/db/api/v1/cas', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + apiKey,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        key: this.lockKey,
        old_value: this.lockValue,
        new_value: ''
      })
    });
    
    if (response.status === 200) {
      // Now delete the key
      await fetch('/db/api/v1/delete', {
        method: 'DELETE',
        headers: {
          'Authorization': 'Bearer ' + apiKey,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ key: this.lockKey })
      });
    }
  }
}
```

### Integer Bump Operations

**Endpoint**: `POST /db/api/v1/bump`

Atomic integer increment/decrement with automatic floor at zero:

**Characteristics**:
- **Atomic**: Thread-safe across all cluster nodes
- **Type Safe**: Only works on valid integer strings
- **Floor Behavior**: Results cannot go below "0"
- **Auto-Initialize**: Missing keys treated as "0"

**Use Cases**:
- Resource usage tracking
- Rate limiting counters
- Quota management
- Statistics collection

## System Monitoring and Maintenance

### Operational Metrics Collection

InsiDB provides detailed operational metrics for monitoring system health:

#### Metrics Categories

1. **Operation Counters**: Track requests by category
   - Value store operations
   - Cache operations  
   - Event operations
   - Subscription operations
   - Blob operations
   - System operations

2. **Performance Metrics**: Operations per second calculations
   - Real-time rate calculations
   - Historical performance tracking
   - Category-specific rates

3. **Resource Usage**: Track consumption across API keys
   - Memory usage per key
   - Disk usage per key
   - Event quotas per key
   - Active subscriptions per key

### Health Check Endpoints

#### Authenticated Ping

**Endpoint**: `GET /db/api/v1/ping`

Provides comprehensive health information:
- **Authentication Status**: Confirms API key validity
- **Node Information**: Shows current node identifier
- **Leader Status**: Indicates current cluster leader
- **Uptime**: Shows how long the node has been running

### Log Analysis and Troubleshooting

#### Important Log Categories

1. **Authentication Failures**
   ```
   Token validation failed during ping remote_addr=192.168.1.100
   ```

2. **Rate Limiting Events**
   ```
   Rate limit exceeded category=values path=/db/api/v1/set remote_addr=192.168.1.100
   ```

3. **Cluster Operations**
   ```
   Successfully joined leader node_id=node1 leader_id=node0
   Auto-join completed node_id=node1
   ```

4. **FSM Operations**
   ```
   FSM applied set_value key=user:12345:profile
   TKV SetNX failed for valuesDb key=lock:resource:456 error=key already exists
   ```

#### Troubleshooting Common Issues

**Authentication Problems**:
```bash
# Check API key format
echo "insi_abc123" | base64 -d  # Should show encrypted data

# Verify cluster instance secret
# Check if key was created with same secret

# Look for tombstone records
# Key might be marked for deletion
```

**Rate Limiting Issues**:
```bash
# Check current limits for key
curl -H "Authorization: Bearer $API_KEY" \
  https://cluster:8080/db/api/v1/limits

# Monitor rate limit headers in responses
curl -v -H "Authorization: Bearer $API_KEY" \
  https://cluster:8080/db/api/v1/set
```

**Cluster Connectivity**:
```bash
# Check leader status from any node
curl -H "Authorization: Bearer $API_KEY" \
  https://node:8080/db/api/v1/ping

# Verify Raft connectivity
# Check private binding accessibility
# Confirm instance secret consistency
```

## Backup and Recovery Procedures

### Data Backup Strategy

InsiDB stores data in multiple locations that require backup:

1. **Raft Logs**: Contains all operation history
   - Location: `{insid_home}/{node_id}/raft/`
   - Importance: Critical for cluster consistency

2. **FSM State**: Current database state
   - Location: `{insid_home}/{node_id}/badger_data/`
   - Importance: Contains all current data

3. **Cache Data**: In-memory cache state  
   - Location: `{insid_home}/{node_id}/badger_cache/`
   - Importance: Performance optimization, can be rebuilt

4. **Blob Storage**: Physical blob files
   - Location: Configured blob storage directory
   - Importance: User content, cannot be rebuilt

### Backup Procedures

#### Full Cluster Backup

```bash
#!/bin/bash
# Stop the cluster gracefully
systemctl stop insid

# Backup each node's data
for node in node0 node1 node2; do
  tar -czf backup-${node}-$(date +%Y%m%d).tar.gz \
    /var/lib/insid/${node}/
done

# Backup blob storage
tar -czf backup-blobs-$(date +%Y%m%d).tar.gz \
  /var/lib/insid/blobs/

# Restart cluster
systemctl start insid
```

#### Rolling Backup (No Downtime)

```bash
#!/bin/bash
# Backup follower nodes while cluster runs
# Leader will handle all writes during backup

for node in node1 node2; do  # Skip leader (node0)
  # Create consistent snapshot
  tar -czf backup-${node}-$(date +%Y%m%d).tar.gz \
    /var/lib/insid/${node}/
done
```

### Recovery Procedures

#### Single Node Recovery

```bash
#!/bin/bash
# Stop failed node
systemctl stop insid@node1

# Restore from backup
tar -xzf backup-node1-20240101.tar.gz -C /var/lib/insid/

# Start node (will auto-join cluster)
systemctl start insid@node1
```

#### Complete Cluster Recovery

```bash
#!/bin/bash
# Restore leader node first
tar -xzf backup-node0-20240101.tar.gz -C /var/lib/insid/
tar -xzf backup-blobs-20240101.tar.gz -C /var/lib/insid/

# Start leader
systemctl start insid@node0

# Wait for leader to be ready, then restore followers
sleep 30

for node in node1 node2; do
  tar -xzf backup-${node}-20240101.tar.gz -C /var/lib/insid/
  systemctl start insid@${node}
  sleep 10  # Allow time for join
done
```

## Performance Tuning

### Configuration Optimization

#### Rate Limiter Tuning

```yaml
# High-throughput configuration
rate_limiters:
  values:
    limit: 1000
    burst: 1500
  cache:
    limit: 2000
    burst: 3000
  events:
    limit: 500
    burst: 750
```

#### Resource Allocation

```yaml
# Cache configuration
cache:
  keys: 300s  # API key cache duration
  
# Session limits  
sessions:
  max_connections: 10000  # Per-node WebSocket limit
```

### Monitoring Performance

Key metrics to monitor:

1. **Operations Per Second**: Track request rates
2. **Response Latency**: Monitor API response times  
3. **Memory Usage**: Watch for memory leaks
4. **Disk I/O**: Monitor storage performance
5. **Network Utilization**: Track cluster communication

### Scaling Considerations

#### Horizontal Scaling

- **Read Scaling**: Add follower nodes for read distribution
- **Geographic Distribution**: Deploy clusters across regions
- **Load Balancing**: Use load balancers for client distribution

#### Vertical Scaling

- **Memory**: Increase for larger caches and more API keys
- **CPU**: Scale for higher request rates
- **Storage**: Expand for larger datasets and longer retention
- **Network**: Ensure adequate bandwidth for cluster communication

## Security Operations

### Security Monitoring

Monitor these security events:

1. **Failed Authentication Attempts**
2. **Administrative API Usage**
3. **Unusual Rate Limit Violations**
4. **IP Address Changes**
5. **Cluster Membership Changes**

### Incident Response

#### Compromised API Key

```bash
# Immediately revoke the key
curl -X POST -H "Authorization: Bearer $ROOT_KEY" \
  https://cluster:8081/db/api/v1/admin/api/delete \
  -d '{"api_key": "compromised_key"}'

# Monitor for unusual activity
# Review access logs
# Check for data access patterns
```

#### Cluster Compromise

```bash
# Rotate instance secret
# Update configuration on all nodes
# Restart cluster with new secret
# All existing API keys will be invalidated
```