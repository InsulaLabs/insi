# Rate Limiting and Security Features

InsiDB implements a comprehensive multi-tier rate limiting system and security framework to protect against abuse, ensure fair resource allocation, and maintain system stability. This document covers the rate limiting architecture, security controls, and access management features.

## Multi-Tier Rate Limiting Architecture

InsiDB employs multiple layers of rate limiting to provide granular control over system access:

### 1. Global Rate Limiting (IP-Based)

Applied to all incoming requests based on client IP address, organized by endpoint categories:

#### Rate Limiting Categories

- **Values**: Value store operations (set, get, delete, iterate)
- **Cache**: Cache operations (set, get, delete, setnx, cas)
- **Events**: Event publishing and subscription operations
- **System**: Administrative and system operations
- **Default**: Fallback for uncategorized endpoints

#### Configuration Example

```yaml
rate_limiters:
  values:
    limit: 100    # requests per second
    burst: 150    # burst capacity
  cache:
    limit: 200
    burst: 300
  events:
    limit: 50
    burst: 75
  system:
    limit: 25
    burst: 50
  default:
    limit: 50
    burst: 100
```

### 2. Per-API-Key Rate Limiting

Individual API keys have their own rate limits, separate for data and event operations:

#### Data Operations Rate Limit
- **Default**: 25 requests per second
- **Scope**: Value store, cache, and blob operations
- **Configurable**: Per API key via admin APIs

#### Event Operations Rate Limit  
- **Default**: 10 requests per second
- **Scope**: Event publishing and subscription operations
- **Configurable**: Per API key via admin APIs

### 3. Resource-Based Rate Limiting

Beyond request rate limits, InsiDB enforces resource consumption limits:

#### Daily Event Quotas
- **Default**: 1,000 events per 24-hour period
- **Reset Mechanism**: Automatic reset 24 hours after first limit reached
- **Enforcement**: Blocks new events until quota resets

#### Concurrent Subscriptions
- **Default**: 100 concurrent WebSocket connections per API key
- **Global Limit**: Configurable maximum connections per node
- **Distributed Tracking**: Coordinated across cluster nodes

## Rate Limiting Behavior

### Request Processing Flow

```
[Incoming Request]
        ↓
[IP-Based Rate Check]
        ↓
[Authentication]
        ↓
[API-Key Rate Check]
        ↓
[Resource Quota Check]
        ↓
[Process Request]
```

### Rate Limit Headers

When rate limits are exceeded, responses include informative headers:

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 5
X-RateLimit-Limit: 100
X-RateLimit-Burst: 150
Content-Type: application/json

{
  "error": "Too Many Requests"
}
```

### Resource Limit Headers

For resource-based limits (quotas):

```http
HTTP/1.1 400 Bad Request
X-Current-Events: 1000
X-Events-Limit: 1000
Content-Type: application/json

{
  "error": "Event quota exceeded. Limit resets in 4h23m"
}
```

## IP-Based Access Control

### Permitted IP Configuration

InsiDB uses IP filtering to control access to public and private endpoints:

```yaml
permitted_ips:
  - "*"                    # Allow all IPs (public endpoints only)
  - "192.168.1.0/24"      # Private network
  - "10.0.0.0/8"          # Private network
  - "203.0.113.42"        # Specific IP
```

### IP Resolution and Trusted Proxies

The system intelligently handles reverse proxies and load balancers:

#### Trusted Proxy Configuration

```yaml
trusted_proxies:
  - "192.168.1.100"       # Load balancer
  - "10.0.0.50"           # Reverse proxy
```

#### X-Forwarded-For Processing

When requests come from trusted proxies:
1. System checks if request IP is in trusted proxy list
2. If trusted, extracts client IP from `X-Forwarded-For` header
3. Uses extracted IP for rate limiting and access control
4. Falls back to direct IP if no forwarding headers present

### Private vs Public Endpoints

#### Public Endpoints
- **Access**: Subject to IP filtering, but `*` allows global access
- **Examples**: `/db/api/v1/set`, `/db/api/v1/get`, `/db/api/v1/events`
- **Security**: Requires valid API key authentication

#### Private Endpoints  
- **Access**: Restricted to specific IPs only (no `*` wildcard)
- **Examples**: `/db/api/v1/admin/*`, `/db/internal/v1/*`
- **Security**: Requires root key authentication + IP filtering

## Authentication and Token Security

### API Key Structure

InsiDB API keys use encrypted, tamper-proof tokens:

```
Format: insi_<base64-encoded-encrypted-data>
Example: insi_eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### Token Encryption

API keys contain encrypted JSON payloads:

```json
{
  "data_scope_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "key_uuid": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "is_alias": false
}
```

**Security Features**:
- **AES-256-GCM Encryption**: Using cluster instance secret
- **Tamper Detection**: Any modification invalidates the token
- **No Sensitive Data**: Entity names not embedded in tokens
- **Cryptographic Binding**: Tied to specific cluster instance

### Token Validation Process

1. **Format Check**: Verify `insi_` prefix and base64 encoding
2. **Decryption**: Decrypt using cluster instance secret
3. **JSON Parsing**: Extract UUID and metadata
4. **FSM Lookup**: Verify token exists in cluster state
5. **Tombstone Check**: Ensure key hasn't been deleted
6. **Cache Storage**: Cache valid tokens for performance

### Token Caching

- **Cache Duration**: 1 minute TTL
- **Auto-Expiration**: No touch-on-hit for synchronization
- **Invalidation**: Automatic cleanup on key deletion

## Security Features

### Tombstoning and Secure Deletion

When API keys are deleted, they undergo secure tombstoning:

#### Tombstone Process

1. **Tombstone Creation**: Mark key for deletion with data scope UUID
2. **Background Cleanup**: Periodic process deletes associated data
3. **Iterative Deletion**: Process data in chunks to avoid blocking
4. **Blob Cleanup**: Coordinate physical file deletion
5. **Alias Cleanup**: Remove all associated alias keys
6. **Final Removal**: Delete key metadata and tombstone record

#### Tombstone Runner

- **Interval**: Runs every 30 seconds
- **Batch Size**: Processes up to 100 keys per iteration
- **Leader Only**: Only runs on cluster leader
- **Chunk Processing**: Deletes up to 100 data keys per cycle

### Audit and Logging

All security-relevant operations are logged:

#### Authentication Events
- **Failed Logins**: Invalid or expired API keys
- **Rate Limit Violations**: IP and key-based limit breaches
- **Administrative Actions**: Key creation, deletion, limit changes

#### Access Control Events
- **IP Filtering**: Blocked requests from unauthorized IPs
- **Privilege Escalation**: Attempts to access admin endpoints
- **Cluster Operations**: Node joins, leader elections

## Rate Limit Management

### Viewing Current Limits

Users can check their own limits:

**Endpoint**: `GET /db/api/v1/limits`

**Response**:
```json
{
  "max_limits": {
    "bytes_in_memory": 262144000,
    "bytes_on_disk": 1073741824,
    "events_emitted": 1000,
    "subscribers": 100,
    "rate_per_second_data_limit": 25,
    "rate_per_second_event_limit": 10
  },
  "current_usage": {
    "bytes_in_memory": 1048576,
    "bytes_on_disk": 5242880,
    "events_emitted": 42,
    "subscribers": 3
  }
}
```

### Administrative Limit Management

Root users can modify limits for any API key:

**Endpoint**: `POST /db/api/v1/admin/limits/set`

**Request**:
```json
{
  "api_key": "insi_targetkey...",
  "limits": {
    "rate_per_second_data_limit": 100,
    "rate_per_second_event_limit": 50,
    "events_emitted": 5000,
    "subscribers": 500
  }
}
```

## Best Practices for Rate Limiting

### Application Design

1. **Implement Backoff**: Use exponential backoff for rate-limited requests
2. **Respect Retry-After**: Honor the `Retry-After` header values
3. **Monitor Usage**: Track your usage against limits proactively
4. **Batch Operations**: Group operations to reduce request count

### Client Implementation Example

```javascript
async function makeRequest(url, data, retries = 3) {
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + apiKey,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(data)
    });
    
    if (response.status === 429) {
      const retryAfter = response.headers.get('Retry-After');
      if (retries > 0 && retryAfter) {
        await sleep(parseInt(retryAfter) * 1000);
        return makeRequest(url, data, retries - 1);
      }
    }
    
    return response;
  } catch (error) {
    console.error('Request failed:', error);
    throw error;
  }
}
```

### Monitoring and Alerting

Set up monitoring for:

1. **Rate Limit Violations**: Track 429 responses
2. **Quota Approaching**: Alert when nearing daily limits
3. **Authentication Failures**: Monitor for invalid tokens
4. **IP Blocking**: Track blocked requests by IP

## Security Recommendations

### Production Deployment

1. **IP Filtering**: Configure strict IP allowlists for private endpoints
2. **TLS Encryption**: Always use HTTPS in production
3. **Token Rotation**: Regularly rotate API keys
4. **Monitoring**: Implement comprehensive security monitoring
5. **Network Security**: Use VPNs or private networks for cluster traffic

### Instance Secret Management

1. **Strong Secrets**: Use cryptographically random instance secrets
2. **Secret Rotation**: Plan for instance secret rotation procedures
3. **Secure Storage**: Store secrets in secure key management systems
4. **Access Control**: Limit access to instance secrets

### Operational Security

1. **Log Analysis**: Regularly review security logs
2. **Rate Limit Tuning**: Adjust limits based on usage patterns
3. **Incident Response**: Have procedures for security incidents
4. **Regular Audits**: Periodically audit API key usage and access patterns 