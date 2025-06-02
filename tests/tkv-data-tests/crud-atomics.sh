#!/bin/bash

# Script to test TKV atomic operations of insic CLI

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
# $1: subcommand (e.g., "atomic-new", "atomic-get")
# $2+: arguments for the subcommand
run_insic() {
    local main_command="$1"
    shift
    local sub_command_and_args=()
    if [[ "$main_command" == atomic-* ]]; then # Heuristic to catch old style for this fix
        # Convert atomic-new to atomic new, atomic-get to atomic get etc.
        sub_command_and_args=("${main_command#atomic-}" "$@")
        main_command="atomic"
    elif [[ "$main_command" == "atomic" ]]; then
        sub_command_and_args=("$@") # Already new style e.g. atomic new key
    else
        # For other commands like set, get, delete, ping, iterate
        # The first arg is the command itself, and the rest are its arguments.
        # No change needed for them, but we ensure the structure is general.
        sub_command_and_args=("$@")
        # For commands like set, get, delete, the first argument is already the command itself.
        # We reconstruct args to pass to insic correctly.
        # The run_insic function is called like: run_insic "set" "key" "value"
        # So, inside insic, it becomes: insic set key value
        # If command is atomic, it becomes: insic atomic new key
        # This logic seems a bit convoluted now. Let's simplify the run_insic call.
    fi

    # Simplified: The first arg to run_insic is the main command/subcommand part
    # and the rest are its direct arguments.
    # The script will now call: run_insic "atomic" "new" "key"
    # or run_insic "set" "key" "value"

    local insic_cmd_parts=("$main_command")
    insic_cmd_parts+=("$@") # Add all other arguments directly

    local cmd_output
    local exit_code

    # echo -e "${INFO_EMOJI} Running: ${INSIC_PATH} --config ${DEFAULT_CONFIG_PATH} ${main_command} ${sub_command_and_args[*]}" >&2
    # cmd_output=$("${INSIC_PATH}" --root --config "${DEFAULT_CONFIG_PATH}" "${main_command}" "${sub_command_and_args[@]}")

    echo -e "${INFO_EMOJI} Running: ${INSIC_PATH} --config ${DEFAULT_CONFIG_PATH} ${insic_cmd_parts[*]}" >&2
    cmd_output=$("${INSIC_PATH}" --root --config "${DEFAULT_CONFIG_PATH}" "${insic_cmd_parts[@]}")
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

test_atomic_new_get_delete() {
    print_header "Test: Atomic New, Get, and Delete"
    local key="atomic_ngd_$(date +%s)"
    local output_new output_get output_del
    local exit_code_new exit_code_get exit_code_del

    # Atomic New (default: overwrite=false)
    output_new=$(run_insic "atomic" "new" "$key")
    exit_code_new=$?
    expect_success "Atomic new for key '$key'" "$exit_code_new" "$output_new"

    # Atomic Get (should be 0)
    output_get=$(run_insic "atomic" "get" "$key")
    exit_code_get=$?
    expect_success "Atomic get for key '$key' (initial value)" "$exit_code_get" # Output checked below
    # Trim whitespace/newline from output_get for comparison
    local trimmed_output_get=$(echo -n "$output_get")
    if [[ "$trimmed_output_get" == "0" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Atomic get for '$key' returned initial value 0.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Atomic get for '$key' - Expected initial value 0, got '$trimmed_output_get'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT -1)) # Adjust for prior expect_success
    fi

    # Atomic Delete
    output_del=$(run_insic "atomic" "delete" "$key")
    exit_code_del=$?
    expect_success "Atomic delete for key '$key'" "$exit_code_del" "$output_del"

    # Atomic Get (should be 0 after delete, as per TKV spec for non-existent atomics)
    output_get_after_del=$(run_insic "atomic" "get" "$key")
    exit_code_get_after_del=$?
    expect_success "Atomic get for deleted key '$key' (should be 0)" "$exit_code_get_after_del"
    local trimmed_output_get_after_del=$(echo -n "$output_get_after_del")
    if [[ "$trimmed_output_get_after_del" == "0" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Atomic get for deleted key '$key' correctly returned 0.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Atomic get for deleted key '$key' - Expected 0, got '$trimmed_output_get_after_del'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT -1))
    fi
}

test_atomic_add() {
    print_header "Test: Atomic Add"
    local key="atomic_add_$(date +%s)"
    local output_new output_add1 output_add2 output_get_final output_del
    local exit_code_new exit_code_add1 exit_code_add2 exit_code_get_final exit_code_del

    # Atomic New
    output_new=$(run_insic "atomic" "new" "$key")
    exit_code_new=$?
    expect_success "Atomic new for key '$key' (for add test)" "$exit_code_new"

    # Atomic Add 5
    output_add1=$(run_insic "atomic" "add" "$key" "5")
    exit_code_add1=$?
    expect_success "Atomic add 5 to key '$key'" "$exit_code_add1"
    local trimmed_output_add1=$(echo -n "$output_add1")
    if [[ "$trimmed_output_add1" == "5" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Atomic add to '$key' returned new value 5.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Atomic add to '$key' - Expected new value 5, got '$trimmed_output_add1'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT -1))
    fi

    # Atomic Add -3
    output_add2=$(run_insic "atomic" "add" "$key" "-3")
    exit_code_add2=$?
    expect_success "Atomic add -3 to key '$key'" "$exit_code_add2"
    local trimmed_output_add2=$(echo -n "$output_add2")
    if [[ "$trimmed_output_add2" == "2" ]]; then # 5 + (-3) = 2
        echo -e "${SUCCESS_EMOJI} ${GREEN}Atomic add to '$key' returned new value 2.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Atomic add to '$key' - Expected new value 2, got '$trimmed_output_add2'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT -1))
    fi

    # Atomic Get (final value should be 2)
    output_get_final=$(run_insic "atomic" "get" "$key")
    exit_code_get_final=$?
    expect_success "Atomic get for key '$key' (final value)" "$exit_code_get_final"
    local trimmed_output_get_final=$(echo -n "$output_get_final")
    if [[ "$trimmed_output_get_final" == "2" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Atomic get for '$key' returned final value 2.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Atomic get for '$key' - Expected final value 2, got '$trimmed_output_get_final'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT -1))
    fi

    # Atomic Delete
    output_del=$(run_insic "atomic" "delete" "$key")
    exit_code_del=$?
    expect_success "Atomic delete for key '$key' (after add test)" "$exit_code_del"
}

test_atomic_new_overwrite() {
    print_header "Test: Atomic New with Overwrite"
    local key="atomic_ow_$(date +%s)"

    # 1. Create atomic, add to it (so value is not 0)
    run_insic "atomic" "new" "$key" > /dev/null
    run_insic "atomic" "add" "$key" "10" > /dev/null
    local output_get1
    local exit_code_get1
    output_get1=$(run_insic "atomic" "get" "$key")
    exit_code_get1=$?
    expect_success "Get before overwrite tests (setup)" "$exit_code_get1"
    local trimmed_output_get1=$(echo -n "$output_get1")
    if [[ "$trimmed_output_get1" == "10" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Pre-condition for overwrite test: value is 10.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Pre-condition for overwrite test failed. Expected 10, got '$trimmed_output_get1'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT -1))
        # Potentially skip further overwrite tests for this key if pre-condition fails badly
    fi


    # 2. Try 'atomic new' again (default overwrite=false, should fail with 409 from server, client translates to error)
    local output_new_no_ow
    local exit_code_new_no_ow
    output_new_no_ow=$(run_insic "atomic" "new" "$key") # Default is overwrite=false
    exit_code_new_no_ow=$?
    expect_error "Atomic new (overwrite=false) on existing key '$key' (expected error)" "$exit_code_new_no_ow" "$output_new_no_ow"


    local output_get_after_no_ow
    local exit_code_get_after_no_ow
    output_get_after_no_ow=$(run_insic "atomic" "get" "$key")
    exit_code_get_after_no_ow=$?
    expect_success "Atomic get after new (overwrite=false) for key '$key'" "$exit_code_get_after_no_ow"
    local trimmed_output_get_after_no_ow=$(echo -n "$output_get_after_no_ow")
    if [[ "$trimmed_output_get_after_no_ow" == "10" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for '$key' correctly remained 10 after 'atomic new' (overwrite=false).${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Value for '$key' DID NOT remain 10 after 'atomic new' (overwrite=false). Expected 10, got '$trimmed_output_get_after_no_ow'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT -1))
    fi

    # 3. Try 'atomic new' with overwrite=true
    local output_new_ow
    local exit_code_new_ow
    output_new_ow=$(run_insic "atomic" "new" "$key" "true") # Pass "true" as the overwrite argument
    exit_code_new_ow=$?
    expect_success "Atomic new (overwrite=true) on existing key '$key'" "$exit_code_new_ow" "$output_new_ow"

    local output_get_after_ow
    local exit_code_get_after_ow
    output_get_after_ow=$(run_insic "atomic" "get" "$key")
    exit_code_get_after_ow=$?
    expect_success "Atomic get after new (overwrite=true) for key '$key'" "$exit_code_get_after_ow"
    local trimmed_output_get_after_ow=$(echo -n "$output_get_after_ow")
    if [[ "$trimmed_output_get_after_ow" == "0" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Value for '$key' correctly reset to 0 after 'atomic new' (overwrite=true).${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Value for '$key' not reset after 'atomic new' (overwrite=true). Expected 0, got '$trimmed_output_get_after_ow'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT -1))
    fi

    # Cleanup
    run_insic "atomic" "delete" "$key" > /dev/null
}

test_atomic_non_existent() {
    print_header "Test: Operations on Non-Existent Atomic Counters"
    local key_ne="atomic_ne_$(date +%s)"

    # Atomic Get on non-existent (should return 0 and succeed)
    local output_get_ne
    local exit_code_get_ne
    output_get_ne=$(run_insic "atomic" "get" "$key_ne")
    exit_code_get_ne=$?
    expect_success "Atomic get on non-existent key '$key_ne' (should be 0)" "$exit_code_get_ne"
    local trimmed_output_get_ne=$(echo -n "$output_get_ne")
    if [[ "$trimmed_output_get_ne" == "0" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Atomic get on non-existent key '$key_ne' correctly returned 0.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Atomic get on non-existent key '$key_ne' - Expected 0, got '$trimmed_output_get_ne'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT -1))
    fi

    # Atomic Add on non-existent (should create it with delta value and succeed)
    local output_add_ne
    local exit_code_add_ne
    output_add_ne=$(run_insic "atomic" "add" "$key_ne" "5")
    exit_code_add_ne=$?
    expect_success "Atomic add on non-existent key '$key_ne' (should create with value 5)" "$exit_code_add_ne"
    local trimmed_output_add_ne=$(echo -n "$output_add_ne")
    if [[ "$trimmed_output_add_ne" == "5" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Atomic add on non-existent key '$key_ne' correctly returned 5.${NC}"
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Atomic add on non-existent key '$key_ne' - Expected 5, got '$trimmed_output_add_ne'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT -1))
    fi

    # Atomic Delete on non-existent (should be success/idempotent)
    local output_del_ne
    local exit_code_del_ne
    output_del_ne=$(run_insic "atomic" "delete" "$key_ne") # This key was just created by atomic add
    exit_code_del_ne=$?
    expect_success "Atomic delete on recently created key '$key_ne' (idempotent)" "$exit_code_del_ne" "$output_del_ne"

    # Atomic Delete again on now non-existent key
    local output_del_ne_again
    local exit_code_del_ne_again
    output_del_ne_again=$(run_insic "atomic" "delete" "$key_ne")
    exit_code_del_ne_again=$?
    expect_success "Atomic delete on non-existent key '$key_ne' (idempotent, second attempt)" "$exit_code_del_ne_again" "$output_del_ne_again"

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

    echo -e "${INFO_EMOJI} Starting TKV atomic operations tests with insic: ${INSIC_PATH}"

    test_atomic_new_get_delete
    test_atomic_add
    test_atomic_new_overwrite
    test_atomic_non_existent


    echo -e "\n${GREEN}All TKV atomic operations tests completed.${NC}"

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
