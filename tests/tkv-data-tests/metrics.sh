#!/bin/bash

# Script to test TKV Metrics operations of insic CLI

# --- Configuration ---
INSIC_PATH=""
DEFAULT_CONFIG_PATH="/tmp/insi-test-cluster/cluster.yaml"

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

# Global variable to hold command output
CMD_OUTPUT=""

# Function to print a section header
print_header() {
    echo -e "\n${BLUE}==== $1 ====${NC}"
}

# Function to run the insic command with --root
run_insic() {
    local main_command="$1"
    shift
    local sub_command_and_args=("$@")

    local full_command_array=("$INSIC_PATH" "--root" "--config" "$DEFAULT_CONFIG_PATH" "$main_command")
    if [ ${#sub_command_and_args[@]} -gt 0 ]; then
        full_command_array+=("${sub_command_and_args[@]}")
    fi

    echo -e "${INFO_EMOJI} Running: ${full_command_array[*]}" >&2
    CMD_OUTPUT=$("${full_command_array[@]}" 2>&1)
    local exit_code=$?
    echo -e "${CMD_OUTPUT}" >&2
    return ${exit_code}
}

# Function to run the insic command without --root
run_insic_no_root() {
    local main_command="$1"
    shift
    local sub_command_and_args=("$@")

    local full_command_array=("$INSIC_PATH" "--config" "$DEFAULT_CONFIG_PATH" "$main_command")
    if [ ${#sub_command_and_args[@]} -gt 0 ]; then
        full_command_array+=("${sub_command_and_args[@]}")
    fi

    echo -e "${INFO_EMOJI} Running (no root): ${full_command_array[*]}" >&2
    CMD_OUTPUT=$(INSI_API_KEY="dummy" "${full_command_array[@]}" 2>&1)
    local exit_code=$?
    echo -e "${CMD_OUTPUT}" >&2
    return ${exit_code}
}

# Function to assert successful execution
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

# Function to get a specific metric value from the output
get_metric_value() {
    local metric_name="$1" # e.g., "Value Store"
    local output="$2"
    # Use grep to find the line, awk to get the value, handle potential spaces
    echo "$output" | grep "${metric_name}:" | awk '{print $NF}'
}

test_metrics_access() {
    print_header "Test: Metrics Endpoint Access Control"

    # 1. Attempt to get metrics WITHOUT --root, should fail
    echo -e "${INFO_EMOJI} Attempting to get metrics without --root (should fail)"
    run_insic_no_root "admin" "ops"
    local exit_code_no_root=$?
    local output_no_root=$CMD_OUTPUT
    expect_error "Get metrics without --root" "$exit_code_no_root" "$output_no_root" "requires --root flag"

    # 2. Attempt to get metrics WITH --root, should succeed
    echo -e "${INFO_EMOJI} Attempting to get metrics with --root (should succeed)"
    run_insic "admin" "ops"
    local exit_code_with_root=$?
    local output_with_root=$CMD_OUTPUT
    expect_success "Get metrics with --root" "$exit_code_with_root" "$output_with_root" "Operations Per Second:"
    expect_success "Check for System metric" "$exit_code_with_root" "$output_with_root" "System:"
    expect_success "Check for Value Store metric" "$exit_code_with_root" "$output_with_root" "Value Store:"
}

test_metrics_tracking() {
    print_header "Test: Metrics Tracking"

    # Function to run ops in a loop
    run_ops_continuously() {
        local op_type=$1
        local key_prefix=$2
        local i=0
        echo "Starting to run '$op_type' operations continuously..."
        while true; do
            i=$((i + 1))
            case "$op_type" in
            "set")
                run_insic "set" "${key_prefix}_${i}" "v" >/dev/null 2>&1
                ;;
            "cache set")
                run_insic "cache" "set" "${key_prefix}_${i}" "v" >/dev/null 2>&1
                ;;
            esac
            # No sleep, run as fast as possible
        done
    }

    # Test Value Store
    echo -e "${INFO_EMOJI} Testing Value Store metrics..."
    # Run the load in the background
    run_ops_continuously "set" "vs_metric_key" &
    local vs_pid=$!
    
    # Let it run for a few seconds to generate a stable ops/sec rate
    sleep 3

    # Check the metrics while the load is running
    run_insic "admin" "ops"
    local exit_code_after_vs=$?
    local output_after_vs=$CMD_OUTPUT
    
    # Stop the background load
    kill $vs_pid
    wait $vs_pid 2>/dev/null
    echo "Stopped VS operations."


    expect_success "Get metrics during sustained VS operations" "$exit_code_after_vs" "$output_after_vs" "Operations Per Second:"

    local vs_ops=$(get_metric_value "Value Store" "$output_after_vs")
    echo -e "${INFO_EMOJI} Detected Value Store Ops/sec: $vs_ops"

    if (( $(echo "$vs_ops > 0.1" | bc -l) )); then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: Value Store ops/sec ($vs_ops) is greater than 0.1 after operations.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Value Store ops/sec ($vs_ops) was not greater than 0.1 after operations.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Give a moment for ops to drop to zero
    sleep 3

    # Test Cache Store
    echo -e "${INFO_EMOJI} Testing Cache Store metrics..."
    run_ops_continuously "cache set" "cache_metric_key" &
    local cache_pid=$!

    sleep 3

    run_insic "admin" "ops"
    local exit_code_after_cache=$?
    local output_after_cache=$CMD_OUTPUT
    
    kill $cache_pid
    wait $cache_pid 2>/dev/null
    echo "Stopped Cache operations."


    expect_success "Get metrics during sustained Cache operations" "$exit_code_after_cache" "$output_after_cache" "Operations Per Second:"

    local cache_ops=$(get_metric_value "Cache" "$output_after_cache")
    echo -e "${INFO_EMOJI} Detected Cache Ops/sec: $cache_ops"

    if (( $(echo "$cache_ops > 0.1" | bc -l) )); then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: Cache ops/sec ($cache_ops) is greater than 0.1 after operations.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Cache ops/sec ($cache_ops) was not greater than 0.1 after operations.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}

test_event_metrics_tracking() {
    print_header "Test: Event and Subscriber Metrics Tracking"
    local topic_prefix="metrics_test_topic_$(date +%s)_$$"

    # --- Test Subscribers metric ---
    echo -e "${INFO_EMOJI} Testing Subscribers metric..."

    # This helper function will create a sustained load of new subscriptions
    run_sub_ops_continuously() {
        local topic_prefix=$1
        local i=0
        echo "Starting to run 'subscribe' operations continuously..."
        while true; do
            i=$((i + 1))
            # Each subscribe runs in the background, we get its PID, and kill it shortly after.
            # This simulates a rapid churn of new subscribers.
            run_insic "subscribe" "${topic_prefix}_${i}" >/dev/null 2>&1 &
            local sub_pid=$!
            sleep 0.2
            kill $sub_pid
            wait $sub_pid 2>/dev/null
        done
    }
    
    run_sub_ops_continuously "$topic_prefix" &
    local sub_load_pid=$!

    # Let the load run for a few seconds to generate a stable ops/sec rate
    sleep 4

    run_insic "admin" "ops"
    local exit_code_after_sub=$?
    local output_after_sub=$CMD_OUTPUT
    
    # Clean up the load-generating process
    kill $sub_load_pid
    wait $sub_load_pid 2>/dev/null
    echo "Stopped subscriber load process."
    
    expect_success "Get metrics during sustained subscribe operations" "$exit_code_after_sub" "$output_after_sub" "Operations Per Second:"
    
    local sub_ops=$(get_metric_value "Subscribers" "$output_after_sub")
    echo -e "${INFO_EMOJI} Detected Subscribers Ops/sec: $sub_ops"

    if (( $(echo "$sub_ops > 0.1" | bc -l) )); then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: Subscribers ops/sec ($sub_ops) is greater than 0.1 after subscribing.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Subscribers ops/sec ($sub_ops) was not greater than 0.1 after subscribing.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Give a moment for ops to drop to zero before the next test
    sleep 3

    # --- Test Events metric ---
    echo -e "${INFO_EMOJI} Testing Events metric..."
    local topic_for_events="${topic_prefix}_events"

    # This helper function will create a sustained load of published events
    run_pub_ops_continuously() {
        local topic=$1
        local i=0
        echo "Starting to run 'publish' operations continuously..."
        while true; do
            i=$((i + 1))
            run_insic "publish" "${topic}" "data_${i}" >/dev/null 2>&1
        done
    }
    
    run_pub_ops_continuously "$topic_for_events" &
    local pub_pid=$!

    sleep 3

    run_insic "admin" "ops"
    local exit_code_after_pub=$?
    local output_after_pub=$CMD_OUTPUT
    
    kill $pub_pid
    wait $pub_pid 2>/dev/null
    echo "Stopped publish operations."

    expect_success "Get metrics during sustained publish operations" "$exit_code_after_pub" "$output_after_pub" "Operations Per Second:"

    local event_ops=$(get_metric_value "Events" "$output_after_pub")
    echo -e "${INFO_EMOJI} Detected Events Ops/sec: $event_ops"

    if (( $(echo "$event_ops > 0.1" | bc -l) )); then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: Events ops/sec ($event_ops) is greater than 0.1 during publishing.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Events ops/sec ($event_ops) was not greater than 0.1 during publishing.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}

# --- Main Execution ---
main() {
    if [ -z "$1" ]; then
        echo -e "${FAILURE_EMOJI} ${RED}Usage: $0 <path_to_insic_executable>${NC}"
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
    local max_retries=5
    local retry_count=0

    while [ $retry_count -lt $max_retries ]; do
        run_insic "ping"
        ping_exit_code=$?
        ping_output=$CMD_OUTPUT
        if [ "$ping_exit_code" -eq 0 ]; then
            break
        fi
        retry_count=$((retry_count + 1))
        sleep 2
    done

    if [ "$ping_exit_code" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}CRITICAL: insic ping failed after ${max_retries} attempts. Aborting tests.${NC}"
        exit 1
    fi
    expect_success "Server Ping" "$ping_exit_code" "$ping_output" "Ping Response:"

    echo -e "${INFO_EMOJI} Starting TKV Metrics operations tests..."

    test_metrics_access
    test_metrics_tracking
    test_event_metrics_tracking

    echo -e "\n${GREEN}All TKV Metrics operations tests completed.${NC}"

    # --- Test Summary ---
    print_header "Test Summary"
    echo -e "${SUCCESS_EMOJI} Successful tests: ${SUCCESSFUL_TESTS_COUNT}"
    if [ "$FAILED_TESTS_COUNT" -gt 0 ]; then
        echo -e "${FAILURE_EMOJI} Failed tests:     ${FAILED_TESTS_COUNT}"
        echo -e "${RED}Some tests failed. Please review the output above.${NC}"
        exit 1
    else
        echo -e "${GREEN}All Metrics tests passed successfully!${NC}"
        exit 0
    fi
}

# Run main
main "$@"
