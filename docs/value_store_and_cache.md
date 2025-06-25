# Value Store and Cache Systems

InsiDB provides two complementary key-value storage systems: the **Value Store** (persistent disk storage) and the **Cache** (in-memory storage). Both systems share similar APIs and operations but differ in their storage characteristics, performance profiles, and resource accounting.

## Overview

### Value Store
- **Storage**: Persistent disk-based storage
- **Durability**: Data survives restarts and crashes
- **Performance**: Slower access, optimized for reliability
- **Resource Accounting**: Counts against `bytes_on_disk` limits
- **Use Cases**: Long-term data storage, configuration, state management

### Cache  
- **Storage**: In-memory volatile storage
- **Durability**: Data is lost on restart/crash
- **Performance**: Fast access, optimized for speed
- **Resource Accounting**: Counts against `bytes_in_memory` limits  
- **Use Cases**: Temporary data, session storage, performance optimization

## Common Operations

Both systems support identical operations with similar semantics:

### Read Operations

#### Get Value
- **Value Store**: `GET /db/api/v1/vs?key=<key>`
- **Cache**: `GET /db/api/v1/cache?key=<key>`

Retrieves a single value by key. Returns 404 if key doesn't exist.

**Response Format:**
```json
{
  "data": "value"
}
```

#### Iterate Keys by Prefix
- **Value Store**: `GET /db/api/v1/vs/iterate?prefix=<prefix>&offset=<offset>&limit=<limit>`
- **Cache**: `GET /db/api/v1/cache/iterate?prefix=<prefix>&offset=<offset>&limit=<limit>`

Returns an array of keys matching the given prefix with pagination support.

**Parameters:**
- `prefix`: Key prefix to match (required for non-root users)
- `offset`: Number of keys to skip (default: 0)
- `limit`: Maximum keys to return (default: 100, max: 1000)

**Response Format:**
```json
["key1", "key2", "key3"]
```

### Write Operations

#### Set Value
- **Value Store**: `POST /db/api/v1/vs`
- **Cache**: `POST /db/api/v1/cache`

Creates or updates a key-value pair.

**Request Body:**
```json
{
  "key": "example_key",
  "value": "example_value"
}
```

#### Set if Not Exists (SETNX)
- **Value Store**: `POST /db/api/v1/vs/setnx`
- **Cache**: `POST /db/api/v1/cache/setnx`

Creates a key-value pair only if the key doesn't already exist. Returns 409 (Conflict) if key exists.

#### Compare and Swap (CAS)
- **Value Store**: `POST /db/api/v1/vs/cas`
- **Cache**: `POST /db/api/v1/cache/cas`

Atomically updates a value only if the current value matches the expected old value.

**Request Body:**
```json
{
  "key": "example_key",
  "old_value": "expected_current_value", 
  "new_value": "new_value"
}
```

Returns 409 (Conflict) if the current value doesn't match `old_value`.

#### Delete
- **Value Store**: `DELETE /db/api/v1/vs`
- **Cache**: `DELETE /db/api/v1/cache`

Removes a key-value pair.

**Request Body:**
```json
{
  "key": "example_key"
}
```

### Value Store Exclusive Operations

#### Bump Integer
- **Endpoint**: `POST /db/api/v1/vs/bump`

Atomically increments or decrements an integer value stored as a string. **Important**: When subtracting (negative values), the result is floored at "0" and cannot go below zero.

**Request Body:**
```json
{
  "key": "counter_key",
  "value": "5"  // Amount to add (can be negative for subtraction)
}
```

**Requirements:**
- Existing value must be a valid integer string
- Increment value must be a valid integer string
- If key doesn't exist, it's treated as "0"
- **Floor Behavior**: Results below 0 are automatically set to "0"

**Examples:**
- Current value: "10", bump: "-3" → Result: "7"
- Current value: "5", bump: "-10" → Result: "0" (floored)
- Current value: "0", bump: "-5" → Result: "0" (floored)

## Resource Management

### Storage Limits
Both systems enforce resource limits but track different metrics:

- **Value Store**: Operations count against the API key's `bytes_on_disk` limit
- **Cache**: Operations count against the API key's `bytes_in_memory` limit

### Size Calculations
Storage usage is calculated as:
```
total_size = len(key) + len(value)
```

For updates, only the delta (difference) is applied to usage tracking.

### Limit Exceeded Responses
When storage limits are exceeded, both systems return:
- **Status**: 400 Bad Request
- **Headers**: 
  - `X-Current-Memory-Usage` or `X-Current-Disk-Usage`: Current usage in bytes
  - `X-Memory-Usage-Limit` or `X-Disk-Usage-Limit`: Maximum allowed bytes
- **Body**: "Memory usage limit exceeded" or "Disk usage limit exceeded"

## Data Scoping and Security

### Automatic Key Prefixing
All keys are automatically prefixed with the API key's data scope UUID:
```
stored_key = "<data_scope_uuid>:<user_provided_key>"
```

This ensures complete data isolation between different API keys.

### Response Key Trimming
When returning keys to clients (e.g., in iterate operations), the internal prefix is automatically removed, so users only see their original key names.

### Authentication and Rate Limiting
Both systems require:
- **Authentication**: Valid API key via Authorization header
- **Rate Limiting**: Subject to the key's `rate_per_second_data_limit`
- **Leadership**: Write operations must be performed on the leader node

## Size Constraints

### Maximum Sizes
Both systems enforce size limits to prevent abuse:
- **Keys**: Must not exceed `MaxValueSize` 
- **Values**: Must not exceed `MaxValueSize`

Requests exceeding these limits return 400 Bad Request with "Key is too large" or "Value is too large".

## Performance Characteristics

### Value Store
- **Persistence**: Data survives restarts and failures
- **Consistency**: Strongly consistent across the cluster
- **Latency**: Higher latency due to disk I/O
- **Capacity**: Limited by available disk space and configured limits

### Cache
- **Volatility**: Data is lost on restart or failure
- **Consistency**: Strongly consistent across the cluster
- **Latency**: Lower latency due to memory access
- **Capacity**: Limited by available memory and configured limits

## Use Case Recommendations

### Use Value Store For:
- Configuration data
- User profiles and settings  
- Application state that must survive restarts
- Data that requires durability guarantees
- Large datasets where persistence is critical

### Use Cache For:
- Session data and temporary state
- Computed results and derived data
- Frequently accessed data requiring fast retrieval
- Data that can be regenerated if lost
- Short-lived data with time-based relevance

## Error Handling

### Common Error Responses
- **401 Unauthorized**: Invalid or missing API key
- **400 Bad Request**: Invalid request format, missing parameters, or size limits exceeded
- **404 Not Found**: Key doesn't exist (for GET operations)
- **409 Conflict**: Key already exists (SETNX) or value changed (CAS)
- **429 Too Many Requests**: Rate limit exceeded
- **500 Internal Server Error**: Server-side processing errors

### Resource Limit Headers
When limits are exceeded, responses include helpful headers showing current usage and limits, allowing clients to understand their resource consumption. 