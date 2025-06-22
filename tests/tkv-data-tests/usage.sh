#!/bin/bash

# Script to test TKV API Key usage limits of insic CLI

# --- Configuration ---
INSIC_PATH=""
DEFAULT_CONFIG_PATH="/tmp/insi-test-cluster/cluster.yaml" # Relative to this script's location

# --- Colors and Emojis ---
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
BLUE="\033[0;34m"
NC="\033[0m" # No Color

SUCCESS_EMOJI="✅"
FAILURE_EMOJI="❌"
WARNING_EMOJI="⚠️"
INFO_EMOJI="ℹ️"

# --- Counters ---
SUCCESSFUL_TESTS_COUNT=0
FAILED_TESTS_COUNT=0

# --- Helper Functions ---

# Global variable to hold command output, since bash makes it hard to return both exit code and output
CMD_OUTPUT=""

# Function to print a section header
print_header() {
    echo -e "\n${BLUE}==== $1 ====${NC}"
}

# Function to run the insic command
# Always uses --root flag.
# $1: main command (e.g., "api", "ping")
# $2+: subcommand and its arguments (e.g., "add" "key_name" for api)
run_insic() {
    local main_command="$1"
    shift
    local sub_command_and_args=("$@")

    local insic_executable="${INSIC_PATH}"
    local config_arg="--config"
    local config_file="${DEFAULT_CONFIG_PATH}"
    local exit_code
    local full_command_array=()

    full_command_array+=("$insic_executable")
    full_command_array+=("--root") # Always include --root
    full_command_array+=("$config_arg" "$config_file")
    full_command_array+=("$main_command")
    if [ ${#sub_command_and_args[@]} -gt 0 ]; then
        full_command_array+=("${sub_command_and_args[@]}")
    fi

    echo -e "${INFO_EMOJI} Running: ${full_command_array[*]}" >&2
    CMD_OUTPUT=$("${full_command_array[@]}" 2>&1)
    exit_code=$? # Capture exit code immediately
    echo -e "${CMD_OUTPUT}" >&2 # Echo to stderr for live logging
    return ${exit_code}
}

# Function to run the insic command with a specific API key via environment variable
# $1: API Key
# $2: Target node (can be empty string for default)
# $3: main command (e.g., "api", "ping")
# $4+: subcommand and its arguments (e.g., "limits")
run_insic_with_key() {
    local api_key="$1"
    shift
    local target_node="$1"
    shift
    local main_command="$1"
    shift
    local sub_command_and_args=("$@")

    local insic_executable="${INSIC_PATH}"
    local config_arg="--config"
    local config_file="${DEFAULT_CONFIG_PATH}"
    local exit_code
    local full_command_array=()

    full_command_array+=("$insic_executable")
    if [ -n "$target_node" ]; then
        full_command_array+=("--target" "$target_node")
    fi
    # This version does NOT add --root
    full_command_array+=("$config_arg" "$config_file")
    full_command_array+=("$main_command")
    if [ ${#sub_command_and_args[@]} -gt 0 ]; then
        full_command_array+=("${sub_command_and_args[@]}")
    fi

    echo -e "${INFO_EMOJI} Running (with INSI_API_KEY set): ${full_command_array[*]}" >&2
    CMD_OUTPUT=$(INSI_API_KEY="$api_key" "${full_command_array[@]}" 2>&1)
    exit_code=$? # Capture exit code immediately
    echo -e "${CMD_OUTPUT}" >&2 # Echo to stderr for live logging
    return ${exit_code}
}

# Function to run the insic command with a timeout and a specific API key via environment variable
# $1: Timeout (e.g., "10s")
# $2: API Key
# $3: Target node (can be empty string for default)
# $4: main command
# $5+: subcommand and its arguments
run_insic_with_key_timeout() {
    local timeout_duration="$1"
    shift
    local api_key="$1"
    shift
    local target_node="$1"
    shift
    local main_command="$1"
    shift
    local sub_command_and_args=("$@")

    local insic_executable="${INSIC_PATH}"
    local config_arg="--config"
    local config_file="${DEFAULT_CONFIG_PATH}"
    local exit_code
    local full_command_array=()

    full_command_array+=("$insic_executable")
    if [ -n "$target_node" ]; then
        full_command_array+=("--target" "$target_node")
    fi
    full_command_array+=("$config_arg" "$config_file")
    full_command_array+=("$main_command")
    if [ ${#sub_command_and_args[@]} -gt 0 ]; then
        full_command_array+=("${sub_command_and_args[@]}")
    fi

    echo -e "${INFO_EMOJI} Running with timeout (with INSI_API_KEY set): timeout ${timeout_duration} ${full_command_array[*]}" >&2
    CMD_OUTPUT=$(timeout "$timeout_duration" env INSI_API_KEY="$api_key" "${full_command_array[@]}" 2>&1)
    exit_code=$? # Capture exit code immediately
    echo -e "${CMD_OUTPUT}" >&2 # Echo to stderr for live logging
    return ${exit_code}
}

# Function to assert successful execution
# $1: Command description
# $2: Exit code of the command
# $3: Output of the command
# $4: Expected string in output (optional)
expect_success() {
    local description="$1"
    local exit_code="$2"
    local output_content="$3"
    local expected_string="$4"

    if [ "$exit_code" -eq 0 ]; then
        if [ -n "$expected_string" ]; then
            if [[ "$output_content" == *"$expected_string"* ]]; then
                echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: ${description} (and output contains '${expected_string}')${NC}"
                SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
            else
                echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} (Output did not contain '${expected_string}')${NC}"
                FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
                echo -e "   Full Output: ${output_content}"
            fi
        else
            echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: ${description}${NC}"
            SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
        fi
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} (Exit code: $exit_code)${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        if [ -n "$output_content" ]; then
            echo -e "   Full Output: ${output_content}"
        fi
    fi
}

# Function to assert an expected error
# $1: Command description
# $2: Exit code of the command
# $3: Output of the command
# $4: Expected string in output (optional)
expect_error() {
    local description="$1"
    local exit_code="$2"
    local output_content="$3"
    local expected_string="$4"

    if [ "$exit_code" -ne 0 ]; then
        if [ -n "$expected_string" ]; then
            if [[ "$output_content" == *"$expected_string"* ]]; then
                echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS (Received Expected Error): ${description} (and output contains '${expected_string}')${NC}"
                SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
            else
                echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} (Expected error, but output did not contain '${expected_string}')${NC}"
                FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
                echo -e "   Full Output: ${output_content}"
            fi
        else
            echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS (Received Expected Error): ${description}${NC}"
            SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
        fi
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} - Expected an error, but command succeeded (Exit code: $exit_code)${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        if [ -n "$output_content" ]; then
            echo -e "   Full Output: ${output_content}"
        fi
    fi
}

# --- Test Functions ---

test_disk_usage_limit() {
    print_header "Test: Disk Usage Limit"
    local key_name="disklimit_key_$(date +%s)_$$"
    local api_key=""
    local test_blob_file_over="/tmp/insi_disk_over_blob.txt"
    local test_blob_file_under="/tmp/insi_disk_under_blob.txt"
    local blob_key_1="disk-limit-blob-1"
    local blob_key_2="disk-limit-blob-2"
    local blob_key_3="disk-limit-blob-3"

    # 1. Create a key
    echo -e "${INFO_EMOJI} Creating key for disk limit test"
    run_insic "api" "add" "$key_name"
    if [ $? -ne 0 ]; then echo "Failed to create API key"; return; fi
    api_key=$(echo "$CMD_OUTPUT" | grep "API Key:" | awk '{print $3}')
    expect_success "Create API key for disk limit test" 0 "$CMD_OUTPUT" "API Key:"

    sleep 5

    # 2. Set a low disk limit
    local disk_limit=100 # bytes
    echo -e "${INFO_EMOJI} Setting disk limit for '$api_key' to $disk_limit bytes"
    run_insic "api" "set-limits" "$api_key" "--disk" "$disk_limit"
    expect_success "Set disk limit to $disk_limit bytes" $? "$CMD_OUTPUT" "OK"

    sleep 10 # Allow time for limit to propagate

    # 3. Check initial usage is zero
    run_insic_with_key "$api_key" "" "api" "limits"
    expect_success "Get initial limits" $? "$CMD_OUTPUT" "Current Usage"
    expect_success "Check initial disk usage is zero" $? "$CMD_OUTPUT" "Bytes on Disk:     0"
    expect_success "Verify disk limit was set" $? "$CMD_OUTPUT" "Bytes on Disk:     $disk_limit"

    # 4. Create a file that is OVER the limit and try to upload it
    head -c 150 < /dev/urandom > "$test_blob_file_over"
    local file_size_over=$(wc -c < "$test_blob_file_over")
    echo -e "${INFO_EMOJI} Attempting to upload blob of size $file_size_over (should fail)"
    run_insic_with_key "$api_key" "" "blob" "upload" "$blob_key_1" "$test_blob_file_over"
    expect_error "Blob upload exceeds disk limit" $? "$CMD_OUTPUT" "disk limit exceeded"

    # 5. Check usage is still zero
    run_insic_with_key "$api_key" "" "api" "limits"
    expect_success "Get limits after failed upload" $? "$CMD_OUTPUT" "Current Usage"
    expect_success "Check disk usage is still zero" $? "$CMD_OUTPUT" "Bytes on Disk:     0"

    # 6. Create a file that is UNDER the limit and upload it
    head -c 80 < /dev/urandom > "$test_blob_file_under"
    local file_size_under=$(wc -c < "$test_blob_file_under")
    echo -e "${INFO_EMOJI} Attempting to upload blob of size $file_size_under (should succeed)"
    run_insic_with_key "$api_key" "" "blob" "upload" "$blob_key_2" "$test_blob_file_under"
    expect_success "Blob upload under disk limit" $? "$CMD_OUTPUT"

    sleep 5

    # 7. Check that usage has been updated
    run_insic_with_key "$api_key" "" "api" "limits"
    expect_success "Get limits after successful upload" $? "$CMD_OUTPUT" "Current Usage"
    expect_success "Check disk usage is now $file_size_under" $? "$CMD_OUTPUT" "Bytes on Disk:     $file_size_under"

    # 8. Attempt to upload another file that, combined with the first, would exceed the limit
    echo -e "${INFO_EMOJI} Attempting to upload another blob of size $file_size_under (should fail)"
    run_insic_with_key "$api_key" "" "blob" "upload" "$blob_key_3" "$test_blob_file_under"
    expect_error "Second blob upload exceeds remaining disk space" $? "$CMD_OUTPUT" "disk limit exceeded"

    # 9. Check usage is unchanged since the last successful upload
    run_insic_with_key "$api_key" "" "api" "limits"
    expect_success "Get limits after second failed upload" $? "$CMD_OUTPUT" "Current Usage"
    expect_success "Check disk usage is still $file_size_under" $? "$CMD_OUTPUT" "Bytes on Disk:     $file_size_under"


    # 10. Cleanup
    echo -e "${INFO_EMOJI} Cleanup for disk usage limit test"
    rm -f "$test_blob_file_over"
    rm -f "$test_blob_file_under"
    run_insic "api" "delete" "$api_key"
    expect_success "Delete API key for cleanup" $? "$CMD_OUTPUT" "OK"
}

test_memory_usage_limit() {
    print_header "Test: Memory (Cache) Usage Limit"
    local key_name="memlimit_key_$(date +%s)_$$"
    local api_key=""
    local value_over=$(head -c 250 < /dev/urandom | base64)
    local value_under=$(head -c 50 < /dev/urandom | base64) # ~68 bytes, well under limit

    # 1. Create a key
    echo -e "${INFO_EMOJI} Creating key for memory limit test"
    run_insic "api" "add" "$key_name"
    if [ $? -ne 0 ]; then echo "Failed to create API key"; return; fi
    api_key=$(echo "$CMD_OUTPUT" | grep "API Key:" | awk '{print $3}')
    expect_success "Create API key for memory limit test" 0 "$CMD_OUTPUT" "API Key:"

    sleep 5

    # 2. Set a low memory limit
    local mem_limit=200 # bytes. Set higher to accommodate overhead.
    echo -e "${INFO_EMOJI} Setting memory limit for '$api_key' to $mem_limit bytes"
    run_insic "api" "set-limits" "$api_key" "--mem" "$mem_limit"
    expect_success "Set memory limit to $mem_limit bytes" $? "$CMD_OUTPUT" "OK"

    sleep 10 # Allow time for limit to propagate

    # 3. Check initial usage and limit
    run_insic_with_key "$api_key" "" "api" "limits"
    expect_success "Get initial limits" $? "$CMD_OUTPUT" "Current Usage"
    expect_success "Check initial memory usage is zero" $? "$CMD_OUTPUT" "Bytes in Memory:   0"
    expect_success "Verify memory limit was set" $? "$CMD_OUTPUT" "Bytes in Memory:   $mem_limit"

    # 4. Try to set a cache value that is OVER the limit
    echo -e "${INFO_EMOJI} Attempting to set cache value with size ${#value_over} (should fail)"
    run_insic_with_key "$api_key" "" "cache" "set" "mem-key-1" "$value_over"
    expect_error "Cache set exceeds memory limit" $? "$CMD_OUTPUT" "memory limit exceeded"

    # 5. Check usage is still zero
    run_insic_with_key "$api_key" "" "api" "limits"
    expect_success "Get limits after failed cache set" $? "$CMD_OUTPUT" "Current Usage"
    expect_success "Check memory usage is still zero" $? "$CMD_OUTPUT" "Bytes in Memory:   0"

    # 6. Set a cache value that is UNDER the limit
    echo -e "${INFO_EMOJI} Attempting to set cache value with size ${#value_under} (should succeed)"
    run_insic_with_key "$api_key" "" "cache" "set" "mem-key-2" "$value_under"
    expect_success "Cache set under memory limit" $? "$CMD_OUTPUT" "OK"

    sleep 5

    # 7. Check that usage has been updated
    # Note: Server-side size calculation might differ slightly from bash string length
    # So we just check that it's non-zero. A more robust check would parse the exact value.
    run_insic_with_key "$api_key" "" "api" "limits"
    expect_success "Get limits after successful cache set" $? "$CMD_OUTPUT" "Current Usage"
    if [[ "$CMD_OUTPUT" == *"Bytes in Memory:   0 /"* ]]; then
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Memory usage did not increase after successful set.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    else
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: Memory usage increased after successful set.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    fi

    # 8. Attempt to set another value that would exceed the limit
    echo -e "${INFO_EMOJI} Attempting to set another cache value with size ${#value_under} (should fail)"
    run_insic_with_key "$api_key" "" "cache" "set" "mem-key-3" "$value_under"
    expect_error "Second cache set exceeds remaining memory" $? "$CMD_OUTPUT" "memory limit exceeded"

    # 9. Cleanup
    echo -e "${INFO_EMOJI} Cleanup for memory usage limit test"
    run_insic "api" "delete" "$api_key"
    expect_success "Delete API key for cleanup" $? "$CMD_OUTPUT" "OK"
}

test_event_rate_limit() {
    print_header "Test: Event Rate Limit"
    local key_name="eventlimit_key_$(date +%s)_$$"
    local api_key=""

    # 1. Create a key
    echo -e "${INFO_EMOJI} Creating key for event rate limit test"
    run_insic "api" "add" "$key_name"
    if [ $? -ne 0 ]; then echo "Failed to create API key"; return; fi
    api_key=$(echo "$CMD_OUTPUT" | grep "API Key:" | awk '{print $3}')
    expect_success "Create API key for event limit test" 0 "$CMD_OUTPUT" "API Key:"

    sleep 5

    # 2. Set a low event rate limit
    # The server uses a token bucket. We set a low rate and burst.
    local event_limit=3
    echo -e "${INFO_EMOJI} Setting event limit for '$api_key' to $event_limit events/sec"
    run_insic "api" "set-limits" "$api_key" "--events" "$event_limit"
    expect_success "Set event limit to $event_limit/sec" $? "$CMD_OUTPUT" "OK"

    sleep 10 # Allow time for limit to propagate

    # Verify limit was set
    run_insic_with_key "$api_key" "" "api" "limits"
    expect_success "Verify event limit was set" $? "$CMD_OUTPUT" "Events per Second: $event_limit"

    # 3. Burst events to exceed the limit
    echo -e "${INFO_EMOJI} Publishing events in a burst to test rate limit"
    local success_count=0
    local error_count=0
    for i in {1..10}; do
        run_insic_with_key "$api_key" "" "publish" "test-topic" "message-$i"
        if [ $? -eq 0 ]; then
            success_count=$((success_count + 1))
        else
            # Check for the specific rate limit error
            if [[ "$CMD_OUTPUT" == *"events limit exceeded"* ]] || [[ "$CMD_OUTPUT" == *"Too Many Requests"* ]]; then
                error_count=$((error_count + 1))
            fi
        fi
    done

    # 4. Verify that some succeeded (burst allowance) and some failed
    echo -e "${INFO_EMOJI} Burst test results: $success_count succeeded, $error_count failed with expected error."
    if [ "$success_count" -gt 0 ] && [ "$error_count" -gt 0 ]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: Event burst was correctly throttled (some succeeded, some failed).${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Event burst was not throttled as expected. Succeeded: $success_count, Failed: $error_count.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
    
    # 5. Cleanup
    echo -e "${INFO_EMOJI} Cleanup for event rate limit test"
    run_insic "api" "delete" "$api_key"
    expect_success "Delete API key for cleanup" $? "$CMD_OUTPUT" "OK"
}

test_subscriber_limit() {
    print_header "Test: Subscriber Limit"
    local key_name="sublimit_key_$(date +%s)_$$"
    local api_key=""
    local sub_pids=() # Array to store PIDs of background subscribers

    # Cleanup function to kill all background subscribers
    cleanup_subscribers() {
        echo -e "${INFO_EMOJI} Cleaning up subscriber processes..."
        for pid in "${sub_pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                echo "Killing subscriber process $pid"
                kill "$pid"
            fi
        done
        # Also clean up the API key
        if [ -n "$api_key" ]; then
            run_insic "api" "delete" "$api_key"
            expect_success "Delete API key for subscriber test cleanup" $? "$CMD_OUTPUT" "OK"
        fi
    }

    # Trap EXIT signal to ensure cleanup happens
    trap cleanup_subscribers EXIT

    # 1. Create a key
    echo -e "${INFO_EMOJI} Creating key for subscriber limit test"
    run_insic "api" "add" "$key_name"
    if [ $? -ne 0 ]; then echo "Failed to create API key"; return; fi
    api_key=$(echo "$CMD_OUTPUT" | grep "API Key:" | awk '{print $3}')
    expect_success "Create API key for subscriber limit test" 0 "$CMD_OUTPUT" "API Key:"

    sleep 5

    # 2. Set a low subscriber limit
    local sub_limit=3 # This is a GLOBAL limit for the key
    local nodes=("node0" "node1" "node2")
    echo -e "${INFO_EMOJI} Setting subscriber limit for '$api_key' to a total of $sub_limit"
    run_insic "api" "set-limits" "$api_key" "--subs" "$sub_limit"
    expect_success "Set subscriber limit to $sub_limit" $? "$CMD_OUTPUT" "OK"
    
    sleep 10 # Allow time for limit to propagate
    
    # Verify limit was set by checking against one node
    run_insic_with_key "$api_key" "node0" "api" "limits"
    expect_success "Verify subscriber limit was set" $? "$CMD_OUTPUT" "Subscribers:       $sub_limit"

    # 3. Launch one subscriber on each node, up to the global limit
    echo -e "${INFO_EMOJI} Launching ${#nodes[@]} subscribers, one on each node (should succeed)"
    local sub_count=0
    for node in "${nodes[@]}"; do
        sub_count=$((sub_count + 1))
        echo "Launching subscriber #$sub_count on $node..."
        local log_file="/tmp/sub_${sub_count}_log.txt"
        # We need to explicitly pass the target node to the command being run in the background
        INSI_API_KEY="$api_key" "$INSIC_PATH" --config "$DEFAULT_CONFIG_PATH" --target "$node" subscribe "test-topic" > "$log_file" 2>&1 &
        sub_pids+=($!)
    done

    # Give them a moment to connect
    sleep 10

    # Verify that the background subscribers connected successfully
    local connected_count=0
    for ((i=1; i<=${#nodes[@]}; i++)); do
        local log_file="/tmp/sub_${i}_log.txt"
        if [ -f "$log_file" ] && grep -q "Successfully connected" "$log_file"; then
            connected_count=$((connected_count + 1))
        fi
    done

    if [ "$connected_count" -eq "${#nodes[@]}" ]; then
         echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: All ${#nodes[@]} background subscribers connected successfully.${NC}"
         SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Expected ${#nodes[@]} subscribers to connect, but only $connected_count did.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi


    # 4. Attempt to launch one more subscriber on one of the nodes (should fail)
    echo -e "${INFO_EMOJI} Attempting to launch one more subscriber on node0 (should fail as global limit of $sub_limit is reached)"
    run_insic_with_key_timeout "10s" "$api_key" "node0" "subscribe" "test-topic"
    expect_error "Subscriber connection over limit on node0" $? "$CMD_OUTPUT" "subscriber limit exceeded"
    
    # 5. Cleanup is handled by the trap
    trap - EXIT # Disable the trap
    cleanup_subscribers
}

# --- Main Execution ---
main() {
    if [ -z "$1" ]; then
        echo -e "${FAILURE_EMOJI} ${RED}Usage: $0 <path_to_insic_executable>${NC}"
        echo -e "Example: $0 ../../bin/insic"
        exit 1
    fi
    INSIC_PATH="$1"

    if [ ! -f "$INSIC_PATH" ]; then
        echo -e "${FAILURE_EMOJI} ${RED}Error: insic executable not found at '$INSIC_PATH'${NC}"
        exit 1
    fi
    if [ ! -x "$INSIC_PATH" ]; then
        echo -e "${FAILURE_EMOJI} ${RED}Error: insic executable at '$INSIC_PATH' is not executable.${NC}"
        exit 1
    fi

    echo -e "${INFO_EMOJI} Using insic executable: ${INSIC_PATH}"
    echo -e "${INFO_EMOJI} Using config: ${DEFAULT_CONFIG_PATH}"

    # Pre-test: Check server status with ping
    print_header "Pre-Test: Server Ping Check"
    local ping_output
    local ping_exit_code
    local max_retries=15
    local retry_count=0
    local wait_time=2

    echo -e "${INFO_EMOJI} Attempting to ping server... (will retry up to ${max_retries} times)"
    while [ $retry_count -lt $max_retries ]; do
        run_insic "ping"
        ping_exit_code=$?
        ping_output=$CMD_OUTPUT
        if [ "$ping_exit_code" -eq 0 ]; then
            echo -e "${SUCCESS_EMOJI} Server is responsive."
            break # Success
        fi
        retry_count=$((retry_count + 1))
        echo -e "${WARNING_EMOJI} Ping failed (attempt ${retry_count}/${max_retries}). Retrying in ${wait_time}s..."
        sleep $wait_time
    done

    if [ "$ping_exit_code" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}CRITICAL: insic ping failed after ${max_retries} attempts. Server might be offline or unreachable.${NC}"
        echo -e "Ping output was:\n${ping_output}"
        echo -e "Exit code from ping: ${ping_exit_code}"
        echo -e "${WARNING_EMOJI} Aborting further tests due to ping failure."
        exit 1
    else
        expect_success "Server Ping" "$ping_exit_code" "$ping_output" "Ping Response:"
    fi

    echo -e "${INFO_EMOJI} Starting TKV Usage Limit operations tests..."

    test_subscriber_limit
    test_disk_usage_limit
    test_memory_usage_limit
    test_event_rate_limit
    
    echo -e "\n${GREEN}All TKV Usage Limit operations tests completed.${NC}"

    # --- Test Summary ---
    print_header "Test Summary"
    echo -e "${SUCCESS_EMOJI} Successful tests: ${SUCCESSFUL_TESTS_COUNT}"
    if [ "$FAILED_TESTS_COUNT" -gt 0 ]; then
        echo -e "${FAILURE_EMOJI} Failed tests:     ${FAILED_TESTS_COUNT}"
        echo -e "${RED}Some tests failed. Please review the output above.${NC}"
        exit 1 # Exit with error if any test failed
    else
        echo -e "${GREEN}All Usage Limit tests passed successfully!${NC}"
        exit 0
    fi
}

# Run main
main "$@"
