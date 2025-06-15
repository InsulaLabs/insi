#!/bin/bash

# Script to test TKV API Key operations of insic CLI

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
    cmd_output=$("${full_command_array[@]}" 2>&1)
    exit_code=$? # Capture exit code immediately
    echo -e "${cmd_output}" # Print command output to stdout for capture
    echo -e "Exit code: ${exit_code}" >&2
    return ${exit_code}
}

# Function to run the insic command without the --root flag
# $1: main command (e.g., "api")
# $2+: subcommand and its arguments
run_insic_no_root() {
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
    # This version does NOT add --root
    full_command_array+=("$config_arg" "$config_file")
    full_command_array+=("$main_command")
    if [ ${#sub_command_and_args[@]} -gt 0 ]; then
        full_command_array+=("${sub_command_and_args[@]}")
    fi

    echo -e "${INFO_EMOJI} Running (no root, with dummy key): ${full_command_array[*]}" >&2
    # Setting a dummy key so the client doesn't complain about a missing key,
    # allowing the --root flag check inside the CLI to be the point of failure.
    cmd_output=$(INSI_API_KEY="dummy" "${full_command_array[@]}" 2>&1)
    exit_code=$? # Capture exit code immediately
    echo -e "${cmd_output}" # Print command output to stdout for capture
    echo -e "Exit code: ${exit_code}" >&2
    return ${exit_code}
}

# Function to run the insic command with a specific API key via environment variable
# $1: API Key
# $2: main command (e.g., "api", "ping")
# $3+: subcommand and its arguments (e.g., "limits")
run_insic_with_key() {
    local api_key="$1"
    shift
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
    # This version does NOT add --root
    full_command_array+=("$config_arg" "$config_file")
    full_command_array+=("$main_command")
    if [ ${#sub_command_and_args[@]} -gt 0 ]; then
        full_command_array+=("${sub_command_and_args[@]}")
    fi

    echo -e "${INFO_EMOJI} Running (with INSI_API_KEY set): ${full_command_array[*]}" >&2
    cmd_output=$(INSI_API_KEY="$api_key" "${full_command_array[@]}" 2>&1)
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

    sleep 5

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

test_api_key_limits() {
    print_header "Test: API Key Limits (Get, Set)"
    local key_name="testlimitskey_$(date +%s)_$$"
    local generated_key=""
    local output_add output_get_initial output_set_limits output_get_updated output_delete
    local exit_code_add exit_code_get_initial exit_code_set_limits exit_code_get_updated exit_code_delete

    # 1. API Add Key
    echo -e "${INFO_EMOJI} Attempting to add API key for limits test: $key_name"
    output_add=$(run_insic "api" "add" "$key_name")
    exit_code_add=$?
    expect_success "API add key '$key_name' for limits test" "$exit_code_add" "$output_add" "API Key:"
    if [ "$exit_code_add" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}API key add failed, cannot proceed with limits test.${NC}"
        return
    fi
    generated_key=$(echo "$output_add" | grep "API Key:" | awk '{print $3}')
    echo -e "${INFO_EMOJI} Parsed generated key for limits test: $generated_key"

    sleep 5

    # 2. Get initial limits for the new key
    echo -e "${INFO_EMOJI} Attempting to get initial limits for key: $generated_key"
    output_get_initial=$(run_insic_with_key "$generated_key" "api" "limits")
    exit_code_get_initial=$?
    expect_success "Get initial limits for new key" "$exit_code_get_initial" "$output_get_initial" "Maximum Limits"
    expect_success "Check initial current usage is zero" "$exit_code_get_initial" "$output_get_initial" "Bytes on Disk:     0"

    # 3. Set new limits for the key (requires --root)
    local new_disk=123456789
    local new_mem=987654321
    local new_events=500
    local new_subs=50
    echo -e "${INFO_EMOJI} Attempting to set limits for key: $generated_key"
    output_set_limits=$(run_insic "api" "set-limits" "$generated_key" "--disk" "$new_disk" "--mem" "$new_mem" "--events" "$new_events" "--subs" "$new_subs")
    exit_code_set_limits=$?
    expect_success "Set new limits for key '$generated_key'" "$exit_code_set_limits" "$output_set_limits" "OK"

    sleep 5

    # 4. Get updated limits and verify they were set correctly
    echo -e "${INFO_EMOJI} Attempting to get updated limits for key: $generated_key"
    output_get_updated=$(run_insic_with_key "$generated_key" "api" "limits")
    exit_code_get_updated=$?
    expect_success "Get updated limits for key '$generated_key'" "$exit_code_get_updated" "$output_get_updated" "Maximum Limits"
    # Check each value individually
    expect_success "Verify updated disk limit" "$exit_code_get_updated" "$output_get_updated" "Bytes on Disk:     $new_disk"
    expect_success "Verify updated memory limit" "$exit_code_get_updated" "$output_get_updated" "Bytes in Memory:   $new_mem"
    expect_success "Verify updated events limit" "$exit_code_get_updated" "$output_get_updated" "Events per Second: $new_events"
    expect_success "Verify updated subscribers limit" "$exit_code_get_updated" "$output_get_updated" "Subscribers:       $new_subs"

    # 5. Delete the key
    echo -e "${INFO_EMOJI} Attempting to delete API key used for limits test: $generated_key"
    output_delete=$(run_insic "api" "delete" "$generated_key")
    exit_code_delete=$?
    expect_success "API delete for key '$generated_key' after limits test" "$exit_code_delete" "$output_delete" "OK"
}

test_api_get_limits_for_other_key() {
    print_header "Test: API Get Another Key's Limits (as root)"
    local key_name="testgetlimits_$(date +%s)_$$"
    local generated_key=""
    local output_add output_set_limits output_get_specific output_delete
    local exit_code_add exit_code_set_limits exit_code_get_specific exit_code_delete

    # 1. API Add Key
    echo -e "${INFO_EMOJI} Attempting to add API key for get-limits test: $key_name"
    output_add=$(run_insic "api" "add" "$key_name")
    exit_code_add=$?
    expect_success "API add key '$key_name' for get-limits test" "$exit_code_add" "$output_add" "API Key:"
    if [ "$exit_code_add" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}API key add failed, cannot proceed with get-limits test.${NC}"
        return
    fi
    generated_key=$(echo "$output_add" | grep "API Key:" | awk '{print $3}')
    echo -e "${INFO_EMOJI} Parsed generated key for get-limits test: $generated_key"

    sleep 5

    # 2. Set new limits for the key (requires --root)
    local new_disk=555666777
    local new_mem=111222333
    local new_events=250
    local new_subs=25
    echo -e "${INFO_EMOJI} Attempting to set limits for key: $generated_key"
    output_set_limits=$(run_insic "api" "set-limits" "$generated_key" "--disk" "$new_disk" "--mem" "$new_mem" "--events" "$new_events" "--subs" "$new_subs")
    exit_code_set_limits=$?
    expect_success "Set new limits for key '$generated_key'" "$exit_code_set_limits" "$output_set_limits" "OK"

    sleep 5

    # 3. Get limits for that specific key using the root key and limits command
    echo -e "${INFO_EMOJI} Attempting to get specific limits for key '$generated_key' using root"
    output_get_specific=$(run_insic "api" "limits" "$generated_key")
    exit_code_get_specific=$?
    expect_success "Get specific limits for key '$generated_key'" "$exit_code_get_specific" "$output_get_specific" "Maximum Limits"
    # Check each value individually
    expect_success "Verify specific disk limit" "$exit_code_get_specific" "$output_get_specific" "Bytes on Disk:     $new_disk"
    expect_success "Verify specific memory limit" "$exit_code_get_specific" "$output_get_specific" "Bytes in Memory:   $new_mem"
    expect_success "Verify specific events limit" "$exit_code_get_specific" "$output_get_specific" "Events per Second: $new_events"
    expect_success "Verify specific subscribers limit" "$exit_code_get_specific" "$output_get_specific" "Subscribers:       $new_subs"

    # 4. Try to get limits for that specific key without root. This should fail.
    echo -e "${INFO_EMOJI} Attempting to get specific limits for key '$generated_key' WITHOUT root"
    local output_get_specific_no_root
    local exit_code_get_specific_no_root
    output_get_specific_no_root=$(run_insic_no_root "api" "limits" "$generated_key")
    exit_code_get_specific_no_root=$?
    expect_error "Get specific limits without root" "$exit_code_get_specific_no_root" "$output_get_specific_no_root" "requires --root flag"

    # 5. Delete the key
    echo -e "${INFO_EMOJI} Attempting to delete API key used for get-limits test: $generated_key"
    output_delete=$(run_insic "api" "delete" "$generated_key")
    exit_code_delete=$?
    expect_success "API delete for key '$generated_key' after get-limits test" "$exit_code_delete" "$output_delete" "OK"
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
        ping_output=$(run_insic "ping") # Ping now also uses --root via updated run_insic
        ping_exit_code=$?
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

    echo -e "${INFO_EMOJI} Starting TKV API Key operations tests..."

    test_api_key_lifecycle
    test_api_key_limits
    test_api_get_limits_for_other_key

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