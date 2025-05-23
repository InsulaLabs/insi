#!/bin/bash

# Configuration
BASE_URL_NODE0="https://127.0.0.1:8443" # Target node0 from cluster.yaml
INSTANCE_SECRET="secret-value-shared-by-all-nodes" # From cluster.yaml
AUTH_TOKEN=$(echo -n "$INSTANCE_SECRET" | sha256sum | awk '{print $1}')

# Helper for logging
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $@"
}

log_response() {
    echo "Response:"
    echo "$1"
    echo "----------------------------------------"
}

# --- Value Endpoints ---
set_value() { # Args: key value
    local key="$1"
    local value="$2"
    log "Setting value: Key='$key', Value='$value'"
    response=$(curl -k -s -w "\n%{http_code}" -X POST -H "Authorization: $AUTH_TOKEN" -H "Content-Type: application/json" \
         -d "{\"key\":\"$key\", \"value\":\"$value\"}" \
         "$BASE_URL_NODE0/set")
    log_response "$response"
}

get_value() { # Args: key
    local key="$1"
    log "Getting value: Key='$key'"
    response=$(curl -k -s -w "\n%{http_code}" -H "Authorization: $AUTH_TOKEN" "$BASE_URL_NODE0/get?key=$key")
    log_response "$response"
}

delete_value() { # Args: key
    local key="$1"
    log "Deleting value: Key='$key'"
    response=$(curl -k -s -w "\n%{http_code}" -X POST -H "Authorization: $AUTH_TOKEN" -H "Content-Type: application/json" \
         -d "{\"key\":\"$key\"}" \
         "$BASE_URL_NODE0/delete")
    log_response "$response"
}

iterate_prefix() { # Args: prefix offset limit
    local prefix="$1"
    local offset="$2"
    local limit="$3"
    log "Iterating prefix: Prefix='$prefix', Offset=$offset, Limit=$limit"
    response=$(curl -k -s -w "\n%{http_code}" -H "Authorization: $AUTH_TOKEN" "$BASE_URL_NODE0/iterate/prefix?prefix=$prefix&offset=$offset&limit=$limit")
    log_response "$response"
}

# --- Tag Endpoints ---
tag_key() { # Args: key tag
    local key="$1"
    local tag_val="$2" # Renamed to avoid conflict with tag command
    log "Tagging key: Key='$key', Tag='$tag_val'"
    response=$(curl -k -s -w "\n%{http_code}" -X POST -H "Authorization: $AUTH_TOKEN" -H "Content-Type: application/json" \
         -d "{\"key\":\"$key\", \"value\":\"$tag_val\"}" \
         "$BASE_URL_NODE0/tag") # Note: 'value' in payload is the tag
    log_response "$response"
}

untag_key() { # Args: key tag
    local key="$1"
    local tag_val="$2"
    log "Untagging key: Key='$key', Tag='$tag_val'"
    response=$(curl -k -s -w "\n%{http_code}" -X POST -H "Authorization: $AUTH_TOKEN" -H "Content-Type: application/json" \
         -d "{\"key\":\"$key\", \"value\":\"$tag_val\"}" \
         "$BASE_URL_NODE0/untag") # Note: 'value' in payload is the tag
    log_response "$response"
}

get_keys_by_tag() { # Args: tag offset limit
    local tag_val="$1"
    local offset="$2"
    local limit="$3"
    log "Getting keys by tag: Tag='$tag_val', Offset=$offset, Limit=$limit"
    response=$(curl -k -s -w "\n%{http_code}" -H "Authorization: $AUTH_TOKEN" "$BASE_URL_NODE0/iterate/tags?tag=$tag_val&offset=$offset&limit=$limit")
    log_response "$response"
}

# --- Cache Endpoints ---
set_cache() { # Args: key value ttl_seconds
    local key="$1"
    local value="$2"
    local ttl_seconds="$3"
    # Convert TTL to nanoseconds for the current CachePayload.TTL (time.Duration) interpretation by json.Unmarshal
    local ttl_nano=$((ttl_seconds * 1000000000))
    log "Setting cache: Key='$key', Value='$value', TTL=${ttl_seconds}s (${ttl_nano}ns)"
    response=$(curl -k -s -w "\n%{http_code}" -X POST -H "Authorization: $AUTH_TOKEN" -H "Content-Type: application/json" \
         -d "{\"key\":\"$key\", \"value\":\"$value\", \"ttl\":$ttl_nano}" \
         "$BASE_URL_NODE0/cache/set")
    log_response "$response"
}

get_cache() { # Args: key
    local key="$1"
    log "Getting cache: Key='$key'"
    response=$(curl -k -s -w "\n%{http_code}" -H "Authorization: $AUTH_TOKEN" "$BASE_URL_NODE0/cache/get?key=$key")
    log_response "$response"
}

delete_cache() { # Args: key
    local key="$1"
    log "Deleting cache: Key='$key'"
    response=$(curl -k -s -w "\n%{http_code}" -X POST -H "Authorization: $AUTH_TOKEN" -H "Content-Type: application/json" \
         -d "{\"key\":\"$key\"}" \
         "$BASE_URL_NODE0/cache/delete")
    log_response "$response"
}

# --- System Endpoints ---
join_node() { # Args: follower_id follower_addr (e.g., node1 127.0.0.1:2223)
    local follower_id="$1"
    local follower_addr="$2"
    log "Attempting to join node: FollowerID='$follower_id', FollowerAddr='$follower_addr'"
    response=$(curl -k -s -w "\n%{http_code}" -H "Authorization: $AUTH_TOKEN" \
         "$BASE_URL_NODE0/join?followerId=$follower_id&followerAddr=$follower_addr")
    log_response "$response"
}

# --- Test Sequence ---
log "Starting integration test script..."
echo "Using auth token: $AUTH_TOKEN"
echo "Targeting base URL: $BASE_URL_NODE0"
echo ""

# Test variables
TEST_KEY="integTestKey$(date +%s)"
TEST_VALUE="helloIntegrationWorld"
TEST_TAG="criticalData"
TEST_PREFIX="integTest"
CACHE_KEY="cacheIntegKey$(date +%s)"
CACHE_VALUE="superSonic"
CACHE_TTL_SECONDS=5 # Use a short TTL for testing expiry if desired, though script doesn't wait full duration.

# 1. Value operations
log "--- Testing Value Operations ---"
set_value "$TEST_KEY" "$TEST_VALUE"
get_value "$TEST_KEY"
iterate_prefix "$TEST_PREFIX" 0 10
echo ""

# 2. Tag operations
log "--- Testing Tag Operations ---"
tag_key "$TEST_KEY" "$TEST_TAG"
get_keys_by_tag "$TEST_TAG" 0 10
# (Manual check: Verify if TEST_KEY is in the list from the command above)
untag_key "$TEST_KEY" "$TEST_TAG"
log "Fetching keys for tag '$TEST_TAG' again (after untag)"
get_keys_by_tag "$TEST_TAG" 0 10 # TEST_KEY should ideally be gone, or the list empty for this tag.
echo ""

# 3. Cache operations
log "--- Testing Cache Operations ---"
set_cache "$CACHE_KEY" "$CACHE_VALUE" "$CACHE_TTL_SECONDS"
get_cache "$CACHE_KEY"
log "Waiting for 2 seconds (cache TTL is ${CACHE_TTL_SECONDS}s)..."
sleep 2
get_cache "$CACHE_KEY" # Should still exist if TTL > 2s
delete_cache "$CACHE_KEY"
get_cache "$CACHE_KEY" # Should be 404 or error indicating not found
echo ""

# 4. Delete value (after other operations)
log "--- Testing Delete Value Operation ---"
delete_value "$TEST_KEY"
get_value "$TEST_KEY" # Should be 404 or error indicating not found
echo ""

# 5. Join operation (basic call, actual join requires a running follower not in cluster)
log "--- Testing Join Operation (Basic Call) ---"
# This assumes node0 is the leader. If not, this specific call might fail as expected.
# This also assumes 'node1' with address '127.0.0.1:2223' is a valid potential follower.
join_node "node1" "127.0.0.1:2223"
echo ""

log "Integration test script finished."
