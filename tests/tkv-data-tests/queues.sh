#!/bin/bash

# Script to test TKV queue operations of insic CLI

# --- Configuration ---
INSIC_PATH=""
DEFAULT_CONFIG_PATH="../../cluster.yaml" # Relative to this script's location

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

# Function to print a section header
print_header() {
    echo -e "\n${BLUE}==== $1 ====${NC}"
}

# Function to run the insic command
# $1: main command (e.g., "queue", "ping")
# $2+: arguments for the subcommand
run_insic() {
    local main_command="$1"
    shift
    local args=("$@")
    local cmd_output
    local exit_code

    echo -e "${INFO_EMOJI} Running: ${INSIC_PATH} --config ${DEFAULT_CONFIG_PATH} ${main_command} ${args[*]}" >&2
    cmd_output=$("${INSIC_PATH}" --root --config "${DEFAULT_CONFIG_PATH}" "${main_command}" "${args[@]}")
    exit_code=$? # Capture exit code immediately
    echo -e "${cmd_output}" # Print command output to stdout for capture
    echo -e "Exit code: ${exit_code}" >&2
    return ${exit_code}
}

# Function to assert successful execution
# $1: Command description
# $2: Exit code of the command
# $3: Output of the command (optional)
expect_success() {
    local description="$1"
    local exit_code="$2"
    local output_content="$3"

    if [ "$exit_code" -eq 0 ]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: ${description}${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
        if [ -n "$output_content" ]; then
            echo -e "   Output: ${output_content}"
        fi
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} (Exit code: $exit_code)${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        if [ -n "$output_content" ]; then
            echo -e "   Output: ${output_content}"
        fi
    fi
}

# Function to assert an expected error
# $1: Command description
# $2: Exit code of the command
# $3: Output of the command (optional)
expect_error() {
    local description="$1"
    local exit_code="$2"
    local output_content="$3"

    if [ "$exit_code" -ne 0 ]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS (Received Expected Error): ${description}${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
        if [ -n "$output_content" ]; then
            echo -e "   Output contains: ${output_content}"
        fi
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} - Expected an error, but command succeeded (Exit code: $exit_code)${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        if [ -n "$output_content" ]; then
            echo -e "   Output: ${output_content}"
        fi
    fi
}

# --- Test Functions ---

test_queue_new_delete() {
    print_header "Test: Queue New and Delete"
    local qkey="queue_nd_$(date +%s)"
    local output
    local exit_code

    # Queue New
    output=$(run_insic "queue" "new" "$qkey")
    exit_code=$?
    expect_success "Queue new for key '$qkey'" "$exit_code" "$output"

    # Queue New (again, should be idempotent)
    output=$(run_insic "queue" "new" "$qkey")
    exit_code=$?
    expect_success "Queue new (again) for key '$qkey' (idempotent)" "$exit_code" "$output"

    # Queue Delete
    output=$(run_insic "queue" "delete" "$qkey")
    exit_code=$?
    expect_success "Queue delete for key '$qkey'" "$exit_code" "$output"

    # Queue Delete (again, non-existent, should be idempotent)
    output=$(run_insic "queue" "delete" "$qkey")
    exit_code=$?
    expect_success "Queue delete (again) for non-existent key '$qkey' (idempotent)" "$exit_code" "$output"
}

test_queue_push_pop() {
    print_header "Test: Queue Push and Pop"
    local qkey="queue_pp_$(date +%s)"
    local val1="item1_$(date +%s)"
    local val2="item2_$(date +%s)"
    local output
    local exit_code

    # Create queue first
    run_insic "queue" "new" "$qkey" > /dev/null
    # Assume success for setup

    # Push item 1
    output=$(run_insic "queue" "push" "$qkey" "$val1")
    exit_code=$?
    expect_success "Queue push '$val1' to '$qkey'" "$exit_code" "$output"
    if [[ "$output" == *"New length: 1"* ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Correct new length (1) after pushing '$val1'${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Incorrect length after pushing '$val1'. Output: $output${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        # Adjust for prior expect_success if necessary, or just mark overall test as failed
    fi

    # Push item 2
    output=$(run_insic "queue" "push" "$qkey" "$val2")
    exit_code=$?
    expect_success "Queue push '$val2' to '$qkey'" "$exit_code" "$output"
    if [[ "$output" == *"New length: 2"* ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Correct new length (2) after pushing '$val2'${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Incorrect length after pushing '$val2'. Output: $output${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Pop item 1
    output=$(run_insic "queue" "pop" "$qkey")
    exit_code=$?
    expect_success "Queue pop from '$qkey' (expecting '$val1')" "$exit_code" # Output checked below
    local trimmed_output_pop1=$(echo -n "$output")
    if [[ "$trimmed_output_pop1" == "$val1" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Popped value matches '$val1'${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Popped value was '$trimmed_output_pop1', expected '$val1'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        # If expect_success was already counted, this is an additional failure point.
    fi

    # Pop item 2
    output=$(run_insic "queue" "pop" "$qkey")
    exit_code=$?
    expect_success "Queue pop from '$qkey' (expecting '$val2')" "$exit_code" # Output checked below
    local trimmed_output_pop2=$(echo -n "$output")
    if [[ "$trimmed_output_pop2" == "$val2" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Popped value matches '$val2'${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Popped value was '$trimmed_output_pop2', expected '$val2'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Pop from empty queue (should error)
    output=$(run_insic "queue" "pop" "$qkey")
    exit_code=$?
    expect_error "Queue pop from empty queue '$qkey'" "$exit_code" "$output"

    # Cleanup: Delete the queue
    run_insic "queue" "delete" "$qkey" > /dev/null
}

test_queue_ops_non_existent() {
    print_header "Test: Queue Operations on Non-Existent Queue"
    local qkey_ne="queue_ne_$(date +%s)"
    local output
    local exit_code

    # Push to non-existent queue (should error)
    output=$(run_insic "queue" "push" "$qkey_ne" "some_value")
    exit_code=$?
    expect_error "Queue push to non-existent queue '$qkey_ne'" "$exit_code" "$output"

    # Pop from non-existent queue (should error)
    output=$(run_insic "queue" "pop" "$qkey_ne")
    exit_code=$?
    expect_error "Queue pop from non-existent queue '$qkey_ne'" "$exit_code" "$output"

    # Delete non-existent queue (should be success/idempotent)
    output=$(run_insic "queue" "delete" "$qkey_ne")
    exit_code=$?
    expect_success "Queue delete on non-existent queue '$qkey_ne' (idempotent)" "$exit_code" "$output"
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

    echo -e "${INFO_EMOJI} Using config: ${DEFAULT_CONFIG_PATH}"

    # Pre-test: Check server status with ping
    print_header "Pre-Test: Server Ping Check"
    local ping_output
    local ping_exit_code
    ping_output=$(run_insic "ping")
    ping_exit_code=$?
    if [ "$ping_exit_code" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}CRITICAL: insic ping failed. Server might be offline or unreachable.${NC}"
        echo -e "Ping output was:\n${ping_output}"
        echo -e "Exit code from ping: ${ping_exit_code}"
        echo -e "${WARNING_EMOJI} Aborting further tests due to ping failure."
        exit 1
    else
        echo -e "${SUCCESS_EMOJI} ${GREEN}Server is online. Proceeding with tests.${NC}"
    fi

    echo -e "${INFO_EMOJI} Starting TKV queue operations tests with insic: ${INSIC_PATH}"

    test_queue_new_delete
    test_queue_push_pop
    test_queue_ops_non_existent

    echo -e "\n${GREEN}All TKV queue tests completed.${NC}"

    # --- Test Summary ---
    print_header "Test Summary"
    echo -e "${SUCCESS_EMOJI} Successful tests: ${SUCCESSFUL_TESTS_COUNT}"
    if [ "$FAILED_TESTS_COUNT" -gt 0 ]; then
        echo -e "${FAILURE_EMOJI} Failed tests:     ${FAILED_TESTS_COUNT}"
        echo -e "${RED}Some tests failed. Please review the output above.${NC}"
        exit 1
    else
        echo -e "${GREEN}All tests passed successfully!${NC}"
    fi
}

# Run main
main "$@"
