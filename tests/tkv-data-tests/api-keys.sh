#!/bin/bash

# Script to test TKV API Key operations of insic CLI

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
    local cmd_output
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
    cmd_output=$("${full_command_array[@]}")
    exit_code=$? # Capture exit code immediately
    echo -e "${cmd_output}" # Print command output to stdout for capture
    echo -e "Exit code: ${exit_code}" >&2
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

test_api_key_lifecycle() {
    print_header "Test: API Key Lifecycle (Add, Verify, Delete)"
    local key_name="testapikey_$(date +%s)_$$"
    local generated_key=""
    local output_add output_verify_ok output_delete output_verify_deleted output_verify_random
    local exit_code_add exit_code_verify_ok exit_code_delete exit_code_verify_deleted exit_code_verify_random

    # 1. API Add Key
    echo -e "${INFO_EMOJI} Attempting to add API key: $key_name"
    output_add=$(run_insic "api" "add" "$key_name")
    exit_code_add=$?
    expect_success "API add key '$key_name'" "$exit_code_add" "$output_add" "API Key:"
    if [ "$exit_code_add" -eq 0 ]; then
        generated_key=$(echo "$output_add" | grep "API Key:" | awk '{print $3}')
        if [ -z "$generated_key" ]; then
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Could not parse generated API key from output.${NC}"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1)) # Already counted by expect_success failure if output mismatch
            SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT - 1)) # Decrement if expect_success passed based on exit code but key parsing failed
            return # Cannot proceed without the key
        else
            echo -e "${INFO_EMOJI} Parsed generated key: $generated_key"
        fi
    else
        echo -e "${FAILURE_EMOJI} ${RED}API key add failed, cannot proceed with test.${NC}"
        return # Cannot proceed
    fi

    sleep 2

    # 2. API Verify (Newly Created Key)
    echo -e "${INFO_EMOJI} Attempting to verify newly created API key: $generated_key"
    output_verify_ok=$(run_insic "api" "verify" "$generated_key")
    exit_code_verify_ok=$?
    expect_success "API verify for new key '$generated_key'" "$exit_code_verify_ok" "$output_verify_ok" "API Key Verified Successfully!"

    # 3. API Delete Key
    echo -e "${INFO_EMOJI} Attempting to delete API key: $generated_key"
    output_delete=$(run_insic "api" "delete" "$generated_key")
    exit_code_delete=$?
    expect_success "API delete for key '$generated_key'" "$exit_code_delete" "$output_delete" "OK"

    sleep 12 # Added sleep after key deletion

    # 4. API Verify (Deleted Key)
    echo -e "${INFO_EMOJI} Attempting to verify deleted API key: $generated_key"
    output_verify_deleted=$(run_insic "api" "verify" "$generated_key")
    exit_code_verify_deleted=$?
    expect_error "API verify for deleted key '$generated_key'" "$exit_code_verify_deleted" "$output_verify_deleted" "Verification FAILED"

    # 5. API Verify (Random/Invalid Key)
    local random_key="invalidkey_$(date +%s)_$$_random"
    echo -e "${INFO_EMOJI} Attempting to verify random invalid API key: $random_key"
    output_verify_random=$(run_insic "api" "verify" "$random_key")
    exit_code_verify_random=$?
    expect_error "API verify for random key '$random_key'" "$exit_code_verify_random" "$output_verify_random" "Verification FAILED"
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
    ping_output=$(run_insic "ping") # Ping now also uses --root via updated run_insic
    ping_exit_code=$?
    if [ "$ping_exit_code" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}CRITICAL: insic ping failed. Server might be offline or unreachable.${NC}"
        echo -e "Ping output was:\n${ping_output}"
        echo -e "Exit code from ping: ${ping_exit_code}"
        echo -e "${WARNING_EMOJI} Aborting further tests due to ping failure."
        exit 1
    else
        expect_success "Server Ping" "$ping_exit_code" "$ping_output" "Ping Response:"
    fi

    echo -e "${INFO_EMOJI} Starting TKV API Key operations tests..."

    test_api_key_lifecycle

    echo -e "\n${GREEN}All TKV API Key operations tests completed.${NC}"

    # --- Test Summary ---
    print_header "Test Summary"
    echo -e "${SUCCESS_EMOJI} Successful tests: ${SUCCESSFUL_TESTS_COUNT}"
    if [ "$FAILED_TESTS_COUNT" -gt 0 ]; then
        echo -e "${FAILURE_EMOJI} Failed tests:     ${FAILED_TESTS_COUNT}"
        echo -e "${RED}Some tests failed. Please review the output above.${NC}"
        exit 1 # Exit with error if any test failed
    else
        echo -e "${GREEN}All API Key tests passed successfully!${NC}"
        exit 0
    fi
}

# Run main
main "$@"