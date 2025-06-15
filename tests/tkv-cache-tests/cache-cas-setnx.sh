#!/bin/bash

# Script to test the atomic TKV cache operations (setnx, cas) of the insic CLI

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

# Function to print a section header
print_header() {
    echo -e "\n${BLUE}==== $1 ====${NC}"
}

# Function to run the insic command
run_insic() {
    local subcommand="$1"
    shift
    local args=("$@")
    local cmd_output
    local exit_code

    echo -e "${INFO_EMOJI} Running: ${INSIC_PATH} --root --config ${DEFAULT_CONFIG_PATH} cache ${subcommand} ${args[*]}" >&2
    # Capture both stdout and stderr to check for "Conflict" message
    cmd_output=$("${INSIC_PATH}" --root --config "${DEFAULT_CONFIG_PATH}" cache "${subcommand}" "${args[@]}" 2>&1)
    exit_code=$?
    echo "${cmd_output}"
    # The line below is now just for debugging, as output is captured
    echo "Exit code: ${exit_code}" >&2
    return ${exit_code}
}

# Function to assert successful execution
expect_success() {
    local description="$1"
    local exit_code="$2"
    local output_content="$3"

    if [ "$exit_code" -eq 0 ]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: ${description}${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} (Exit code: $exit_code)${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}

# Function to assert a specific conflict error
expect_conflict() {
    local description="$1"
    local exit_code="$2"
    local output_content="$3"

    if [ "$exit_code" -ne 0 ]; then
        if echo "$output_content" | grep -q "Conflict\|Precondition"; then
            echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS (Received Expected Conflict/Precondition): ${description}${NC}"
            SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
        else
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} - Expected a Conflict/Precondition error, but got a different error (Exit code: $exit_code)${NC}"
            echo -e "   Output: ${output_content}"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        fi
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} - Expected an error, but command succeeded (Exit code: $exit_code)${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}


# --- Test Functions ---

test_cache_setnx() {
    print_header "Test: cache setnx"
    local key="test_cache_setnx_$(date +%s)"
    local value1="value1"
    local value2="value2"
    local output1 exit_code1 output_get1 output2 exit_code2 output_get2
    
    # 1. SetNX on a new key should succeed
    output1=$(run_insic "setnx" "$key" "$value1")
    exit_code1=$?
    expect_success "cache setnx on new key '$key'" "$exit_code1" "$output1"

    # 2. Verify the value was set
    output_get1=$(run_insic "get" "$key")
    if [[ "$output_get1" == "$value1" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for '$key' correctly set to '$value1'${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Get after setnx - Expected '$value1', got '$output_get1'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # 3. SetNX on the same key should fail with a conflict
    output2=$(run_insic "setnx" "$key" "$value2")
    exit_code2=$?
    expect_conflict "cache setnx on existing key '$key'" "$exit_code2" "$output2"

    # 4. Verify the value has not changed
    output_get2=$(run_insic "get" "$key")
    if [[ "$output_get2" == "$value1" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for '$key' remained '$value1' after failed setnx${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Get after failed setnx - Expected '$value1', got '$output_get2'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}

test_cache_cas() {
    print_header "Test: cache cas (Compare-And-Swap)"
    local key="test_cache_cas_$(date +%s)"
    local initial_value="initial_cas_value"
    local new_value="new_cas_value"
    local wrong_value="wrong_cas_value"
    local output_cas_nonexistent exit_code_cas_nonexistent
    local output_cas_wrong exit_code_cas_wrong
    local output_get_after_fail output_cas_correct exit_code_cas_correct
    local output_get_after_success

    # 1. Setup: set an initial value
    run_insic "set" "$key" "$initial_value" > /dev/null
    sleep 1
    
    # 2. CAS on a non-existent key should fail
    output_cas_nonexistent=$(run_insic "cas" "nonexistent_$key" "$initial_value" "$new_value")
    exit_code_cas_nonexistent=$?
    expect_conflict "cas on non-existent key" "$exit_code_cas_nonexistent" "$output_cas_nonexistent"

    # 3. CAS with incorrect old value should fail
    output_cas_wrong=$(run_insic "cas" "$key" "$wrong_value" "$new_value")
    exit_code_cas_wrong=$?
    expect_conflict "cas with incorrect old value for key '$key'" "$exit_code_cas_wrong" "$output_cas_wrong"

    # 4. Verify value did not change after failed CAS
    output_get_after_fail=$(run_insic "get" "$key")
    if [[ "$output_get_after_fail" == "$initial_value" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for '$key' remained '$initial_value' after failed cas${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Get after failed cas - Expected '$initial_value', got '$output_get_after_fail'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # 5. CAS with correct old value should succeed
    output_cas_correct=$(run_insic "cas" "$key" "$initial_value" "$new_value")
    exit_code_cas_correct=$?
    expect_success "cas with correct old value for key '$key'" "$exit_code_cas_correct" "$output_cas_correct"

    # 6. Verify value was updated after successful CAS
    output_get_after_success=$(run_insic "get" "$key")
    if [[ "$output_get_after_success" == "$new_value" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for '$key' correctly updated to '$new_value'${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Get after successful cas - Expected '$new_value', got '$output_get_after_success'${NC}"
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

    print_header "Pre-Test: Server Ping Check"
    # Note: ping does not have a 'cache' subcommand, so we call it directly.
    local ping_output=$("${INSIC_PATH}" --root --config "${DEFAULT_CONFIG_PATH}" ping 2>&1)
    local ping_exit_code=$?
    if [ "$ping_exit_code" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}CRITICAL: insic ping failed. Aborting tests.${NC}"
        exit 1
    else
        echo -e "${SUCCESS_EMOJI} ${GREEN}Server is online. Proceeding with atomic cache op tests.${NC}"
    fi

    test_cache_setnx
    test_cache_cas

    print_header "Test Summary"
    echo -e "${SUCCESS_EMOJI} Successful tests: ${SUCCESSFUL_TESTS_COUNT}"
    if [ "$FAILED_TESTS_COUNT" -gt 0 ]; then
        echo -e "${FAILURE_EMOJI} Failed tests:     ${FAILED_TESTS_COUNT}"
        echo -e "${RED}Some tests failed. Please review the output above.${NC}"
        exit 1
    else
        echo -e "${GREEN}All atomic cache op tests passed successfully!${NC}"
    fi
}

# Run main
main "$@" 