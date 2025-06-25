# Admin and System Management APIs

InsiDB provides a comprehensive suite of administrative and system management APIs that enable cluster operators and administrators to monitor, manage, and maintain the database system. These APIs require elevated privileges and provide deep insights into system operations, resource usage, and cluster health.

## Authentication Requirements

All admin APIs require authentication with the **System Root Key** - the cluster-wide administrative key derived from the instance secret. This key has administrative privileges across the entire cluster.

## Entity Management and Insights

The insight system provides detailed analytics and management capabilities for API keys (entities) and their resource consumption.

### Get Entity Information

**Endpoint**: `POST /db/api/v1/admin/insight/entity`  
**Authentication**: System Root Key  
**Description**: Retrieve detailed information about a specific API key entity.

**Request Body**:
```json
{
  "root_api_key": "insi_base64encodedkey..."
}
```

**Response**:
```json
{
  "entity": {
    "root_api_key": "insi_base64encodedkey...",
    "aliases": ["alias1", "alias2"],
    "data_scope_uuid": "uuid-for-data-isolation",
    "key_uuid": "uuid-for-key-identification",
    "usage": {
      "current_usage": {
        "bytes_in_memory": 1048576,
        "bytes_on_disk": 5242880,
        "events_emitted": 42,
        "subscribers": 3
      },
      "max_limits": {
        "bytes_in_memory": 262144000,
        "bytes_on_disk": 1073741824,
        "events_emitted": 1000,
        "subscribers": 100
      }
    }
  }
}
```

### List All Entities

**Endpoint**: `POST /db/api/v1/admin/insight/entities`  
**Authentication**: System Root Key  
**Description**: Retrieve a paginated list of all API key entities in the system.

**Request Body**:
```json
{
  "offset": 0,
  "limit": 50
}
```

**Response**: Array of entity objects (same structure as individual entity response).

### Get Entity by Alias

**Endpoint**: `POST /db/api/v1/admin/insight/entity_by_alias`  
**Authentication**: System Root Key  
**Description**: Look up an entity using one of its aliases.

**Request Body**:
```json
{
  "alias": "insi_aliaskeystring..."
}
```

## Resource Limit Management

### Set Resource Limits

**Endpoint**: `POST /db/api/v1/admin/limits/set`  
**Authentication**: System Root Key  
**Description**: Set or update resource limits for a specific API key.

**Request Body**:
```json
{
  "api_key": "insi_targetkey...",
  "limits": {
    "bytes_in_memory": 500000000,
    "bytes_on_disk": 2147483648,
    "events_emitted": 2000,
    "subscribers": 200,
    "rate_per_second_data_limit": 50,
    "rate_per_second_event_limit": 20
  }
}
```

**Notes**:
- Only include limits you want to change
- Omitted fields remain unchanged
- All limits are enforced in real-time

### Get Specific Key Limits

**Endpoint**: `POST /db/api/v1/admin/limits/get`  
**Authentication**: System Root Key  
**Description**: View limits and current usage for any specific API key.

**Request Body**:
```json
{
  "api_key": "insi_targetkey..."
}
```

## Operational Metrics

### Operations Per Second

**Endpoint**: `GET /db/api/v1/admin/metrics/ops`  
**Authentication**: System Root Key  
**Description**: Get real-time operations per second metrics across all categories.

**Response**:
```json
{
  "OP_VS": 45.67,
  "OP_Cache": 23.12,
  "OP_Events": 8.90,
  "OP_Subscribers": 2.34,
  "OP_Blobs": 12.45,
  "OP_System": 5.67
}
```

**Metrics Categories**:
- `OP_VS`: Value store operations (set, get, delete, iterate)
- `OP_Cache`: Cache operations
- `OP_Events`: Event publishing and subscription operations
- `OP_Subscribers`: WebSocket subscription management
- `OP_Blobs`: Blob storage operations
- `OP_System`: System and administrative operations

## Cluster Management

### Node Join

**Endpoint**: `GET /db/api/v1/join?followerId=<nodeId>&followerAddr=<raftAddr>`  
**Authentication**: System Root Key  
**Description**: Manually join a follower node to the Raft cluster.

**Parameters**:
- `followerId`: The unique identifier of the node to join
- `followerAddr`: The Raft address of the follower node

**Use Cases**:
- Manual cluster expansion
- Re-joining nodes after maintenance
- Cluster recovery scenarios

## System Health and Status

### Authenticated Ping

**Endpoint**: `GET /db/api/v1/ping`  
**Authentication**: Any valid API key  
**Description**: Health check with authentication verification and system information.

**Response**:
```json
{
  "status": "ok",
  "entity": "api-key-name",
  "node-badge-id": "unique-node-identifier",
  "leader": "node0",
  "uptime": "2h34m12s"
}
```

## Default Resource Limits

When creating new API keys, the following default limits are applied:

- **Memory Usage**: 250 MB
- **Disk Usage**: 1 GB  
- **Events Emitted**: 1,000 per 24-hour period
- **Subscribers**: 100 concurrent connections
- **Data RPS Limit**: 25 requests per second
- **Event RPS Limit**: 10 requests per second

## Error Responses

### Common Status Codes

- **401 Unauthorized**: Invalid or missing System Root Key
- **403 Forbidden**: IP address not permitted (see security documentation)
- **404 Not Found**: Entity or resource not found
- **500 Internal Server Error**: System-level errors
- **503 Service Unavailable**: Leader redirection required

### Leader Redirection

Write operations and administrative functions must be performed on the Raft leader. If you contact a follower node, you'll receive a `307 Temporary Redirect` response with the leader's address in the `Location` header.

## Security Considerations

1. **Root Key Protection**: The System Root Key grants full administrative access. Protect it carefully.

2. **Network Security**: Admin APIs are available on both public and private bindings, but are subject to IP filtering.

3. **Audit Trail**: All administrative operations are logged for security and compliance.

4. **Rate Limiting**: Even root operations are subject to rate limiting to prevent abuse.

## Monitoring and Alerting

Use the metrics endpoints to implement monitoring:

1. **Resource Usage**: Monitor entity usage against limits
2. **Operations Rate**: Track ops/second for performance tuning  
3. **Cluster Health**: Monitor leader status and node connectivity
4. **Limit Violations**: Alert on entities approaching resource limits

## Best Practices

1. **Regular Monitoring**: Implement automated monitoring of cluster health and resource usage
2. **Capacity Planning**: Use entity insights to plan for growth
3. **Limit Management**: Proactively adjust limits based on usage patterns
4. **Cluster Maintenance**: Use join APIs for controlled cluster scaling 