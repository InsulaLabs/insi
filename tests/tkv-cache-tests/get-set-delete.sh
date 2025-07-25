#!/bin/bash

# Script to test TKV cache operations (set, get, delete) of insic CLI

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

# Global variable to store the last raw command output (stdout + stderr)
LAST_RAW_CMD_OUTPUT=""

# --- Helper Functions ---

# Function to print a section header
print_header() {
    echo -e "\n${BLUE}==== $1 ====${NC}"
}

# Function to run the insic command
# $1: subcommand (e.g., "cache", "ping")
# $2+: arguments for the subcommand
run_insic() {
    local subcommand="$1"
    shift
    local args=("$@")
    local captured_output_combined # Stores combined stdout and stderr
    local exit_code

    echo -e "${INFO_EMOJI} Running: ${INSIC_PATH} --config ${DEFAULT_CONFIG_PATH} ${subcommand} ${args[*]}" >&2
    # Capture combined stdout and stderr
    captured_output_combined=$("${INSIC_PATH}" --root --config "${DEFAULT_CONFIG_PATH}" "${subcommand}" "${args[@]}" 2>&1)
    exit_code=$?
    LAST_RAW_CMD_OUTPUT="${captured_output_combined}" # Store for expect_error and expect_success

    # Filter out default slog lines to try and get "clean" stdout
    # Default slog TextHandler format: time=YYYY-MM-DDTHH:MM:SS.sssZ level=LEVEL msg="message" key=value
    # This regex aims to catch these log lines.
    # Note: This assumes the default slog text handler format.
    local clean_stdout=$(echo "${captured_output_combined}" | grep -v -E '^time=[0-9.:T-]+Z? level=(INFO|DEBUG|WARN|ERROR) msg="')

    echo -e "${clean_stdout}" # This is what gets captured by `var=$(run_insic ...)`
    echo -e "Exit code: ${exit_code}" >&2 # Debug info, not captured by var=
    return ${exit_code}
}

# Function to assert successful execution
# $1: Command description
# $2: Exit code of the command
# $3: Output of the command (this will be the "clean_stdout" from run_insic)
expect_success() {
    local description="$1"
    local exit_code="$2"
    local clean_stdout_arg="$3"

    if [ "$exit_code" -eq 0 ]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: ${description}${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
        if [ -n "$clean_stdout_arg" ]; then
            echo -e "   Output (Cleaned Stdout): 
${clean_stdout_arg}"
        fi
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} (Exit code: $exit_code)${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        echo -e "   Full Raw Output on failure (stdout + stderr):
--START RAW OUTPUT--
${LAST_RAW_CMD_OUTPUT}
--END RAW OUTPUT--"
    fi
}

# Function to assert an expected error
# $1: Command description
# $2: Exit code of the command
# $3: Cleaned stdout from run_insic (optional, for context if needed for debugging user, but not for validation)
expect_error() {
    local description="$1"
    local exit_code="$2"
    local clean_stdout_arg="$3" # Kept for potential debugging, but not used for pass/fail logic

    if [ "$exit_code" -ne 0 ]; then # Command failed, as expected.
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS (Received Expected Error): ${description}${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else # Command succeeded, but an error was expected. This is a test failure.
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} - Expected an error, but command succeeded (Exit code: $exit_code)${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        echo -e "   Full Raw Output (from unexpected success, stdout + stderr):
--START RAW OUTPUT--
${LAST_RAW_CMD_OUTPUT}
--END RAW OUTPUT--"
    fi
}

# --- Test Functions ---

test_cache_set_get_basic() {
    print_header "Test: Cache Set and Get Basic"
    local key="cachekey_sg_$(date +%s)"
    local value="cachevalue_sg_$(date +%s)"
    local output_set output_get
    local exit_code_set exit_code_get

    # Set
    output_set=$(run_insic "cache" "set" "$key" "$value")
    exit_code_set=$?
    if [ "$exit_code_set" -eq 0 ] && [[ "$output_set" == "OK" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: Cache set key '$key' to '$value', received 'OK'.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Cache set key '$key'. Expected exit 0 and 'OK'. Got exit $exit_code_set, output: '$output_set'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        echo -e "   Full Raw Output:
--START RAW OUTPUT--
${LAST_RAW_CMD_OUTPUT}
--END RAW OUTPUT--"
        # If set fails, no point in trying to get
        return
    fi

    echo -e "${INFO_EMOJI} Brief pause after set..."
    sleep 0.2 # Diagnostic delay

    # Get
    output_get=$(run_insic "cache" "get" "$key")
    exit_code_get=$?
    if [ "$exit_code_get" -eq 0 ] && [[ "$output_get" == "$value" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: Cache get key '$key' returned correct value.${NC}"
        # No need to print value here as it's confirmed to match
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    elif [ "$exit_code_get" -eq 0 ] && [[ "$output_get" != "$value" ]]; then
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Cache get key '$key' - Value mismatch. Expected '$value', got '$output_get'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        echo -e "   Full Raw Output:
--START RAW OUTPUT--
${LAST_RAW_CMD_OUTPUT}
--END RAW OUTPUT--"
    else # exit_code_get != 0
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Cache get key '$key' - Command failed. Exit code: $exit_code_get${NC}"
        echo -e "   Cleaned Stdout (may contain error from CLI): '$output_get'"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        echo -e "   Full Raw Output:
--START RAW OUTPUT--
${LAST_RAW_CMD_OUTPUT}
--END RAW OUTPUT--"
    fi
}

test_cache_delete() {
    print_header "Test: Cache Delete"
    local key="cachekey_del_$(date +%s)"
    local value="cachevalue_del_$(date +%s)"
    local output_set output_delete output_get
    local exit_code_set exit_code_delete exit_code_get

    # Set
    output_set=$(run_insic "cache" "set" "$key" "$value")
    exit_code_set=$?
    if [ "$exit_code_set" -eq 0 ] && [[ "$output_set" == "OK" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: Cache set key '$key' for delete test, received 'OK'.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Cache set key '$key' for delete test. Expected exit 0 and 'OK'. Got exit $exit_code_set, output: '$output_set'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        echo -e "   Full Raw Output:
--START RAW OUTPUT--
${LAST_RAW_CMD_OUTPUT}
--END RAW OUTPUT--"
        return # If set fails, no point in proceeding
    fi

    # Delete
    output_delete=$(run_insic "cache" "delete" "$key")
    exit_code_delete=$?
    if [ "$exit_code_delete" -eq 0 ] && [[ "$output_delete" == "OK" ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: Cache delete key '$key', received 'OK'.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Cache delete key '$key'. Expected exit 0 and 'OK'. Got exit $exit_code_delete, output: '$output_delete'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        echo -e "   Full Raw Output:
--START RAW OUTPUT--
${LAST_RAW_CMD_OUTPUT}
--END RAW OUTPUT--"
        # Even if delete fails, we should still try to get to see if it's gone
    fi

    echo -e "${INFO_EMOJI} Brief pause after delete..."
    sleep 0.2 # Diagnostic delay

    # Get (should now correctly fail with an error, e.g., 404)
    output_get=$(run_insic "cache" "get" "$key")
    exit_code_get=$?
    expect_error "Cache get deleted key '$key' (should fail after delete)" "$exit_code_get" "$output_get"
}

test_cache_get_non_existent() {
    print_header "Test: Cache Get Non-Existent Key"
    local key="nonexistentcachekey_$(date +%s)"
    local output_get
    local exit_code_get

    # No need for a pause here as the key never existed
    output_get=$(run_insic "cache" "get" "$key")
    exit_code_get=$?
    expect_error "Cache get non-existent key '$key'" "$exit_code_get" "$output_get"
}

test_cache_cli_arg_validation() {
    print_header "Test: Cache CLI Argument Validation"
    local key="argtest_key_$(date +%s)"
    local val="argtest_val"
    local output exit_code

    # cache set
    output=$(run_insic "cache" "set" "$key") # Missing value
    exit_code=$?
    expect_error "cache set: missing value argument" "$exit_code" "$output"

    output=$(run_insic "cache" "set" "$key" "$val" "extra_arg") # Too many args
    exit_code=$?
    expect_error "cache set: too many arguments" "$exit_code" "$output"

    # cache get
    output=$(run_insic "cache" "get") # Missing key
    exit_code=$?
    expect_error "cache get: missing key argument" "$exit_code" "$output"

    output=$(run_insic "cache" "get" "$key" "extra_arg") # Too many args
    exit_code=$?
    expect_error "cache get: too many arguments" "$exit_code" "$output"

    # cache delete
    output=$(run_insic "cache" "delete") # Missing key
    exit_code=$?
    expect_error "cache delete: missing key argument" "$exit_code" "$output"

    output=$(run_insic "cache" "delete" "$key" "extra_arg") # Too many args
    exit_code=$?
    expect_error "cache delete: too many arguments" "$exit_code" "$output"
}

test_cache_iterate_prefix() {
    print_header "Test: Cache Iterate by Prefix"
    local prefix="c_iter_prefix_$(date +%s)_"
    local key1="${prefix}key1"
    local value1="c_value1_iter"
    local key2="${prefix}key2"
    local value2="c_value2_iter"
    local key3="${prefix}key3"
    local value3="c_value3_iter"
    local output_iterate
    local exit_code_iterate
    local exit_code_set

    # Set keys
    run_insic "cache" "set" "$key1" "$value1" > /dev/null
    exit_code_set=$?
    expect_success "Cache set key '$key1' for iteration" "$exit_code_set"
    run_insic "cache" "set" "$key2" "$value2" > /dev/null
    exit_code_set=$?
    expect_success "Cache set key '$key2' for iteration" "$exit_code_set"
    run_insic "cache" "set" "$key3" "$value3" > /dev/null
    exit_code_set=$?
    expect_success "Cache set key '$key3' for iteration" "$exit_code_set"

    sleep 0.5 # Give a moment for writes to settle across the cluster if applicable

    # Iterate
    output_iterate=$(run_insic "cache" "iterate" "prefix" "$prefix")
    exit_code_iterate=$?
    expect_success "Cache iterate by prefix '$prefix'" "$exit_code_iterate" # Don't print output here, we'll validate it below

    local found_key1=false
    local found_key2=false
    local found_key3=false
    local extra_items=false
    local count=0

    # Read the output line by line to handle multi-line results
    while IFS= read -r line; do
        # Ignore empty lines which might result from the command output parsing
        if [ -z "$line" ]; then
            continue
        fi
        count=$((count + 1))
        if [[ "$line" == "$key1" ]]; then
            found_key1=true
        elif [[ "$line" == "$key2" ]]; then
            found_key2=true
        elif [[ "$line" == "$key3" ]]; then
            found_key3=true
        else
            # This logic assumes the only output is the list of keys.
            # We filter slog lines in run_insic, but if other text appears, it will be flagged.
            echo -e "${WARNING_EMOJI} ${YELLOW}Iterate by prefix '$prefix' returned unexpected item: $line${NC}"
            extra_items=true
        fi
    done <<< "$(echo -e "${output_iterate}")" # Use echo -e to handle potential newlines correctly

    if [[ "$found_key1" == true && "$found_key2" == true && "$found_key3" == true && "$extra_items" == false && $count -eq 3 ]]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Cache iterate by prefix '$prefix' successfully retrieved all expected keys and no extras.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Cache iterate by prefix '$prefix' did not retrieve the expected set of keys.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        echo -e "  Found key1 ($key1): $found_key1"
        echo -e "  Found key2 ($key2): $found_key2"
        echo -e "  Found key3 ($key3): $found_key3"
        echo -e "  Extra items found: $extra_items"
        echo -e "  Total items found: $count (Expected 3)"
    fi

    # Cleanup iteration keys
    run_insic "cache" "delete" "$key1" > /dev/null
    run_insic "cache" "delete" "$key2" > /dev/null
    run_insic "cache" "delete" "$key3" > /dev/null
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

    if [ ! -f "$DEFAULT_CONFIG_PATH" ]; then
        echo -e "${WARNING_EMOJI} ${YELLOW}Warning: Default config file not found at '$DEFAULT_CONFIG_PATH'. Some operations might fail if server requires specific config.${NC}"
    else
         echo -e "${INFO_EMOJI} Using config: ${DEFAULT_CONFIG_PATH}"
    fi

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

    echo -e "${INFO_EMOJI} Starting TKV cache tests with insic: ${INSIC_PATH}"

    test_cache_set_get_basic
    test_cache_delete
    test_cache_get_non_existent
    test_cache_cli_arg_validation
    test_cache_iterate_prefix

    echo -e "\n${GREEN}All TKV cache tests completed.${NC}"

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
