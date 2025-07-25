# Event System Test Coverage

## Overview
This document outlines the test coverage for the insi event system, including publish, subscribe, and purge functionality.

## Test Files

### 1. `events.sh` - Basic Event Functionality
Tests core event publishing and subscription features:
- ✅ WebSocket subscription connection
- ✅ Event publishing
- ✅ Event delivery to subscribers
- ✅ Multiple messages in sequence
- ✅ Basic purge functionality (3 subscribers)

### 2. `events-purge-advanced.sh` - Advanced Purge Testing
Comprehensive edge case and error condition testing:

#### Test 1: Empty Purge
- ✅ Purging with no active subscriptions
- ✅ Proper "No active subscriptions" message

#### Test 2: Idempotency
- ✅ Multiple purges in succession
- ✅ Second purge finds no subscriptions

#### Test 3: API Key Isolation
- ✅ Multiple API keys with different subscriptions
- ✅ Purge only affects caller's subscriptions
- ✅ Other API keys' subscriptions remain active

#### Test 4: Concurrency
- ✅ Purging while new subscriptions are being created
- ✅ Race condition handling
- ✅ Thread safety

#### Test 5: Network Issues
- ✅ Purging frozen/unresponsive clients
- ✅ Proper cleanup of stale connections
- ✅ Connection termination handling

#### Test 6: Rate Limiting
- ✅ Rate limiting enforcement on purge endpoint
- ✅ Proper 429 responses

#### Test 7: Per-Node Purge Isolation
- ✅ Subscribers distributed across multiple nodes
- ✅ Purge is node-local by design (correct isolation)
- ✅ Verifies purging from one node doesn't affect other nodes
- ✅ Iterative purging from each node
- ✅ Accurate disconnection count per node
- ✅ Node accessibility checks
- ✅ Cleanup of cross-node test resources

## Production Readiness Checklist

### Functionality ✅
- [x] Basic publish/subscribe working
- [x] Purge disconnects all sessions for API key
- [x] Purge returns accurate count
- [x] Empty purge handled gracefully
- [x] API key isolation verified
- [x] Cross-node purge functionality

### Error Handling ✅
- [x] No panics on concurrent operations
- [x] No double-close of channels
- [x] Graceful handling of connection errors
- [x] Proper cleanup on process termination

### Performance & Limits ✅
- [x] Rate limiting applied
- [x] Subscription slot management
- [x] Connection count tracking
- [x] Memory cleanup verified

### Security ✅
- [x] Authentication required
- [x] API key scoping enforced
- [x] No cross-key purging

### Operational ✅
- [x] Logging at appropriate levels
- [x] Metrics incremented correctly
- [x] Clean shutdown behavior

### Clustering ✅
- [x] Cross-node subscription verification
- [x] Per-node purge isolation (by design)
- [x] Node accessibility handling
- [x] Purge correctly affects only local subscriptions

## Additional Considerations for Production

### 1. Monitoring
- Monitor purge operation counts
- Track subscription slot usage
- Alert on high purge rates (potential abuse)
- Monitor cross-node purge latency

### 2. Load Testing
- Test with thousands of concurrent subscriptions
- Verify memory usage under load
- Test purge performance with many connections
- Test cross-node purge with large subscriber counts

### 3. Network Resilience
- Test with real network interruptions
- Verify behavior across node failures
- Test with slow/unstable connections
- Test purge during network partitions

### 4. Client Libraries
- Ensure client libraries handle purge gracefully
- Add reconnection logic if needed
- Document purge behavior for API users

## Test Execution

Run all event tests:
```bash
./run-all.sh
```

Run specific test:
```bash
./events.sh /path/to/insic
./events-purge-advanced.sh /path/to/insic
```

### Environment Variables
- `CLUSTER_NODES`: Space-separated list of nodes to test (default: "node0 node1 node2")
- `PURGE_FROM_NODE`: Node to issue purge command from (default: TARGET_NODE)
- `CONFIG_FILE`: Path to cluster configuration (default: /tmp/insi-test-cluster/cluster.yaml)
- `TARGET_NODE`: Default node for single-node tests (default: node0)

## Future Test Additions

1. **Performance Tests**
   - Purge with 1000+ active subscriptions
   - Measure purge completion time
   - Memory usage validation
   - Cross-node purge latency measurements

2. **Cluster Tests**
   - Purge behavior during leader election
   - Cross-node subscription management
   - Slot synchronization testing
   - Purge during network partitions

3. **Client Behavior**
   - Auto-reconnection after purge
   - Graceful degradation
   - Error message validation


