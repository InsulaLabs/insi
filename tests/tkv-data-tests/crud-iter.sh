#!/bin/bash

# Script to test TKV data operations of insic CLI

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

# Function to print a section header
print_header() {
    echo -e "\n${BLUE}==== $1 ====${NC}"
}

# Function to run the insic command
# $1: subcommand (e.g., "set", "get")
# $2+: arguments for the subcommand
run_insic() {
    local subcommand="$1"
    shift
    local args=("$@")
    local cmd_output
    local exit_code

    echo -e "${INFO_EMOJI} Running: ${INSIC_PATH} --config ${DEFAULT_CONFIG_PATH} ${subcommand} ${args[*]}" >&2
    cmd_output=$("${INSIC_PATH}" --root --config "${DEFAULT_CONFIG_PATH}" "${subcommand}" "${args[@]}" 2>&1)
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
    local output_content="$3" # Capture the output if passed

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
        # exit 1 # Optional: exit script on first failure
    fi
}

# Function to assert an expected error
# $1: Command description
# $2: Exit code of the command
# $3: Output of the command (optional)
expect_error() {
    local description="$1"
    local exit_code="$2"
    local output_content="$3" # Capture the output if passed

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
        # exit 1 # Optional: exit script on first failure
    fi
}

# --- Test Functions ---

test_set_get() {
    print_header "Test: Set and Get"
    local key="testkey_sg_$(date +%s)"
    local value="testvalue_sg_$(date +%s)"
    local output_set
    local exit_code_set
    local output_get
    local exit_code_get

    # Set
    output_set=$(run_insic "set" "$key" "$value")
    exit_code_set=$?
    expect_success "Set key '$key' to '$value'" "$exit_code_set" "$output_set"

    # Get
    output_get=$(run_insic "get" "$key")
    exit_code_get=$?
    expect_success "Get key '$key'" "$exit_code_get" "$output_get"
    if [[ "$output_get" != "$value" ]]; then
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Get key '$key' - Expected value '$value', got '$output_get'${NC}"
    else
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for '$key' matches!${NC}"
    fi
}

test_get_non_existent() {
    print_header "Test: Get Non-Existent Key"
    local key="nonexistentkey_$(date +%s)"
    local output
    local exit_code

    output=$(run_insic "get" "$key")
    exit_code=$?
    expect_error "Get non-existent key '$key'" "$exit_code" "$output"
    if [[ "$output" == *"Error: "* ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Correctly received an error message for non-existent key.${NC}"
    else
        echo -e "${WARNING_EMOJI} ${YELLOW}Warning: Error message for non-existent key did not contain 'Error: '. Output: $output${NC}"
    fi
}

test_delete() {
    print_header "Test: Set, Delete, and Get"
    local key="testkey_del_$(date +%s)"
    local value="testvalue_del_$(date +%s)"
    local output_set output_delete output_get
    local exit_code_set exit_code_delete exit_code_get

    # Set
    output_set=$(run_insic "set" "$key" "$value")
    exit_code_set=$?
    expect_success "Set key '$key' for deletion test" "$exit_code_set" "$output_set"

    # Delete
    output_delete=$(run_insic "delete" "$key")
    exit_code_delete=$?
    expect_success "Delete key '$key'" "$exit_code_delete" "$output_delete"

    # Get (should fail)
    output_get=$(run_insic "get" "$key")
    exit_code_get=$?
    expect_error "Get deleted key '$key'" "$exit_code_get" "$output_get"
}

test_iterate_prefix() {
    print_header "Test: Iterate by Prefix"
    local prefix="iter_prefix_$(date +%s)_"
    local key1="${prefix}key1"
    local value1="value1_iter" # Still needed for set
    local key2="${prefix}key2"
    local value2="value2_iter" # Still needed for set
    local key3="${prefix}key3"
    local value3="value3_iter" # Still needed for set
    local output_iterate
    local exit_code_iterate
    local exit_code_set # for set operations

    # Set keys
    run_insic "set" "$key1" "$value1" > /dev/null
    exit_code_set=$?
    expect_success "Set key '$key1' for iteration" "$exit_code_set"
    run_insic "set" "$key2" "$value2" > /dev/null
    exit_code_set=$?
    expect_success "Set key '$key2' for iteration" "$exit_code_set"
    run_insic "set" "$key3" "$value3" > /dev/null
    exit_code_set=$?
    expect_success "Set key '$key3' for iteration" "$exit_code_set"

    # Iterate
    # Note: Iterate command in insic now outputs KEYS.
    # For this test, we check if the keys are returned.
    output_iterate=$(run_insic "iterate" "prefix" "$prefix")
    exit_code_iterate=$?
    expect_success "Iterate by prefix '$prefix' (expecting keys)" "$exit_code_iterate" "$output_iterate"

    local found_key1=false
    local found_key2=false
    local found_key3=false
    local extra_items=false
    local count=0

    while IFS= read -r line; do
        count=$((count + 1))
        if [[ "$line" == "$key1" ]]; then
            found_key1=true
        elif [[ "$line" == "$key2" ]]; then
            found_key2=true
        elif [[ "$line" == "$key3" ]]; then
            found_key3=true
        else
            echo -e "${WARNING_EMOJI} ${YELLOW}Iterate by prefix '$prefix' returned unexpected item: $line${NC}"
            extra_items=true
        fi
    done <<< "$output_iterate"

    if [[ "$found_key1" == true && "$found_key2" == true && "$found_key3" == true && "$extra_items" == false && $count -eq 3 ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Iterate by prefix '$prefix' successfully retrieved all expected keys and no extras.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Iterate by prefix '$prefix' did not retrieve the expected set of keys.${NC}"
        echo -e "  Found key1 ($key1): $found_key1"
        echo -e "  Found key2 ($key2): $found_key2"
        echo -e "  Found key3 ($key3): $found_key3"
        echo -e "  Extra items found: $extra_items"
        echo -e "  Total items found: $count (Expected 3)"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Cleanup iteration keys
    run_insic "delete" "$key1" > /dev/null
    run_insic "delete" "$key2" > /dev/null
    run_insic "delete" "$key3" > /dev/null
}

test_bump() {
    print_header "Test: Bump Integer Value"
    local key="testkey_bump_$(date +%s)"
    local output_set output_get output_bump
    local exit_code_set exit_code_get exit_code_bump

    # Set initial value
    run_insic "set" "$key" "10" > /dev/null
    exit_code_set=$?
    expect_success "Set initial value for bump test ('$key' -> '10')" "$exit_code_set"

    # Bump up
    run_insic "bump" "$key" "5" > /dev/null
    exit_code_bump=$?
    expect_success "Bump key '$key' up by '5'" "$exit_code_bump"

    output_get=$(run_insic "get" "$key")
    exit_code_get=$?
    expect_success "Get key '$key' after bumping up" "$exit_code_get"
    if [[ "$output_get" == "15" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for '$key' is '15' as expected.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Value for '$key' - Expected '15', got '$output_get'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Bump down
    run_insic "bump" "$key" "-10" > /dev/null
    exit_code_bump=$?
    expect_success "Bump key '$key' down by '-10'" "$exit_code_bump"

    output_get=$(run_insic "get" "$key")
    exit_code_get=$?
    expect_success "Get key '$key' after bumping down" "$exit_code_get"
    if [[ "$output_get" == "5" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for '$key' is '5' as expected.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Value for '$key' - Expected '5', got '$output_get'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Bump below zero - This should result in 0
    echo -e "${INFO_EMOJI} Testing bumping below zero (expected to result in '0')..."
    run_insic "bump" "$key" "-10" > /dev/null
    exit_code_bump=$?
    expect_success "Bump key '$key' by '-10' which would go below zero" "$exit_code_bump"

    output_get=$(run_insic "get" "$key")
    exit_code_get=$?
    expect_success "Get key '$key' after bumping below zero" "$exit_code_get"
    if [[ "$output_get" == "0" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for '$key' is '0' as expected. Bumping does not go below zero.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Value for '$key' - Expected '0', got '$output_get'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Bump on a non-existent key
    local new_key="new_bump_key_$(date +%s)"
    run_insic "bump" "$new_key" "100" > /dev/null
    exit_code_bump=$?
    expect_success "Bump non-existent key '$new_key' by '100'" "$exit_code_bump"

    output_get=$(run_insic "get" "$new_key")
    exit_code_get=$?
    expect_success "Get key '$new_key' after initial bump" "$exit_code_get"
    if [[ "$output_get" == "100" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for new key '$new_key' is '100' as expected.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Value for new key '$new_key' - Expected '100', got '$output_get'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Test bump with non-integer value in command
    local bad_bump_val="not-a-number"
    output_bump=$(run_insic "bump" "$new_key" "$bad_bump_val")
    exit_code_bump=$?
    expect_error "Bump key '$new_key' with non-integer value '$bad_bump_val'" "$exit_code_bump" "$output_bump"

    # Test bump on a key that holds a non-integer value
    local non_int_key="non_int_key_$(date +%s)"
    run_insic "set" "$non_int_key" "i-am-string" > /dev/null
    output_bump=$(run_insic "bump" "$non_int_key" "5")
    exit_code_bump=$?
    expect_error "Bump key '$non_int_key' which holds a string value" "$exit_code_bump" "$output_bump"

    # Cleanup
    run_insic "delete" "$key" > /dev/null
    run_insic "delete" "$new_key" > /dev/null
    run_insic "delete" "$non_int_key" > /dev/null
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

    echo -e "${INFO_EMOJI} Starting TKV data tests with insic: ${INSIC_PATH}"
    echo -e "${INFO_EMOJI} Using config: ${DEFAULT_CONFIG_PATH}"

    test_set_get
    test_get_non_existent
    test_delete
    test_iterate_prefix
    test_bump

    echo -e "\n${GREEN}All TKV data tests completed.${NC}"

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
