# Insula Test Suite Architecture & Methodology

This comprehensive test suite provides black-box integration testing for the Insula distributed system (`insid`) through its command-line interface (`insic`). The tests validate system functionality across multiple domains while serving as both integration tests and validation of the `insic` CLI program itself.

## Overview & Execution Flow

The test suite is orchestrated by `run-all.sh`, which creates an isolated test environment and executes ten distinct test scenarios covering the full breadth of system capabilities. Each test run:

1. **Environment Setup**: Creates a temporary cluster environment in `/tmp/insi-test-cluster`
2. **Binary Deployment**: Copies built `insid` and `insic` binaries from `../build/`
3. **Configuration**: Uses `test-cluster.yaml` for a 3-node local cluster configuration
4. **Test Execution**: Runs each test script with automatic `insid` lifecycle management
5. **Cleanup**: Ensures clean state between tests with data directory removal

### Test Categories & Organization

The test suite is logically organized into three primary domains:

#### 1. TKV Data Tests (`tkv-data-tests/`)
- **Primary Operations**: `crud-iter.sh` - CRUD operations and iteration
- **Complex Features**: `cas-setnx.sh` - Compare-and-swap, Set-if-not-exists
- **Blob Storage**: `blob.sh` - Binary data upload/download operations  
- **API Security**: `api-keys.sh` - API key lifecycle, aliases, permissions
- **Resource Management**: `usage.sh` - Resource limits and quota enforcement
- **Analytics**: `insight.sh` - System insights and monitoring data

#### 2. TKV Cache Tests (`tkv-cache-tests/`)
- **Core Operations**: `get-set-delete.sh` - In-memory cache operations
- **Advanced Primitives**: `cache-cas-setnx.sh` - Atomic cache operations

#### 3. TKV Event Tests (`tkv-event-tests/`)
- **Pub/Sub System**: `events.sh` - Event publishing and subscription workflows

## Common Testing Patterns & Methodologies

### 1. **Consistent Test Structure**

All test scripts follow a standardized architecture:

```bash
# Configuration & Constants
INSIC_PATH=""
DEFAULT_CONFIG_PATH="/tmp/insi-test-cluster/cluster.yaml"

# Visual Feedback System
GREEN="\033[0;32m"
RED="\033[0;31m"
SUCCESS_EMOJI="✅"
FAILURE_EMOJI="❌"

# Test Accounting
SUCCESSFUL_TESTS_COUNT=0
FAILED_TESTS_COUNT=0
```

### 2. **Command Execution Wrapper**

Each test implements a standardized `run_insic()` function that:
- Provides consistent command execution with configuration injection
- Captures both stdout and stderr for analysis
- Implements intelligent log filtering (removes `slog` noise in cache tests)
- Provides detailed command tracing for debugging

### 3. **Assertion Framework**

Two core assertion functions provide consistent test validation:

- **`expect_success()`**: Validates successful operations with exit code 0
- **`expect_error()`**: Validates expected failures with non-zero exit codes

Both functions provide detailed output capture and contribute to test accounting.

### 4. **Temporal Considerations**

The tests account for distributed system realities:
- **State Propagation**: Strategic `sleep` calls allow Raft consensus to propagate changes
- **Cache TTL**: Test configurations use short TTLs (10s for API keys) to enable rapid testing
- **Connection Establishment**: Event tests wait for explicit connection confirmations

### 5. **Resource Isolation**

Each test ensures complete isolation through:
- **Unique Identifiers**: Timestamp-based key generation (`$(date +%s)`)
- **Process ID Integration**: Event tests use `$$` for topic uniqueness
- **Cleanup Strategies**: Explicit resource deletion and `trap` handlers

## Advanced Testing Techniques

### 1. **Multi-Node Validation**

The test cluster configuration defines three nodes (`node0`, `node1`, `node2`) enabling:
- **Leader Election**: Tests verify functionality across Raft leader changes
- **Data Consistency**: Operations on different nodes validate distributed consistency
- **Network Partitioning**: Tests can target specific nodes to validate cluster behavior

### 2. **Resource Limit Testing**

The `usage.sh` test implements sophisticated resource validation:
- **Disk Quotas**: Validates blob storage limits with precise byte accounting
- **Memory Limits**: Tests cache size restrictions with cumulative usage tracking  
- **Rate Limiting**: Implements token-bucket validation for event publishing
- **Concurrent Subscribers**: Tests global subscriber limits across cluster nodes

### 3. **Security Model Validation**

The `api-keys.sh` test provides comprehensive security testing:
- **Authentication**: Validates API key creation, verification, and deletion
- **Authorization**: Tests permission boundaries between different key types
- **Alias System**: Validates hierarchical key relationships and inheritance
- **Data Scoping**: Ensures proper isolation between different key scopes

### 4. **Event System Testing**

The `events.sh` test validates the pub/sub system through:
- **Background Process Management**: Sophisticated subprocess lifecycle management
- **Message Integrity**: Content validation across publish/subscribe boundaries
- **Connection Lifecycle**: Explicit connection state verification
- **Cleanup Orchestration**: Comprehensive resource cleanup via signal handlers

## Test Coverage Documentation

Each test domain includes comprehensive coverage documentation:

- **`usage-test-coverage.md`**: 22 distinct test cases covering resource management
- **`api-test-coverage.md`**: 25 test cases covering security and key management  
- **`events-test-coverage.md`**: 15 test cases covering pub/sub functionality

These documents provide checkbox-style validation matrices ensuring comprehensive coverage of system capabilities.

## Pre-Flight & Health Validation

Every test script implements robust pre-flight checks:

1. **Binary Validation**: Confirms executable presence and permissions
2. **Configuration Validation**: Verifies cluster configuration accessibility  
3. **Connectivity Testing**: Uses `insic ping` to validate server responsiveness
4. **Retry Logic**: Some tests implement retry mechanisms for distributed system readiness

## Error Handling & Diagnostics

The test suite provides comprehensive error handling:

- **Exit Code Propagation**: Failed tests immediately terminate with appropriate exit codes
- **Output Capture**: Full command output preservation for debugging
- **Visual Feedback**: Color-coded emoji system for rapid test result assessment
- **Summary Reporting**: Aggregate success/failure counts with detailed breakdowns

## Integration with Build System

The test suite integrates with the project's build system:
- **Build Dependency**: Requires binaries in `../build/` directory
- **Cluster Configuration**: Uses dedicated test configuration separate from production
- **Temporary Environment**: Complete isolation prevents interference with development environment

