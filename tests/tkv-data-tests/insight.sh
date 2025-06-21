#!/bin/bash

# Script to test TKV Insight operations of insic CLI

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

# Function to run the insic command with a specific API key via environment variable
run_insic_with_key() {
    local api_key="$1"
    shift
    local main_command="$1"
    shift
    local sub_command_and_args=("$@")

    local full_command_array=("$INSIC_PATH" "--config" "$DEFAULT_CONFIG_PATH" "$main_command")
    if [ ${#sub_command_and_args[@]} -gt 0 ]; then
        full_command_array+=("${sub_command_and_args[@]}")
    fi

    echo -e "${INFO_EMOJI} Running (with INSI_API_KEY set): ${full_command_array[*]}" >&2
    CMD_OUTPUT=$(INSI_API_KEY="$api_key" "${full_command_array[@]}" 2>&1)
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


# --- Test Functions ---

test_insight_retrieval() {
    print_header "Test: Insight Data Creation and Retrieval"

    declare -A primary_keys
    declare -A primary_to_aliases

    # 1. Create 3 primary keys
    echo -e "${INFO_EMOJI} Creating 3 primary keys..."
    for i in {1..3}; do
        local key_name="insight_test_key_${i}_$(date +%s)_$$"
        run_insic "api" "add" "$key_name"
        local exit_code_add=$?
        local output_add=$CMD_OUTPUT
        expect_success "Create primary key #${i}" "$exit_code_add" "$output_add" "API Key:"
        if [ "$exit_code_add" -ne 0 ]; then
            echo -e "${FAILURE_EMOJI} ${RED}Failed to create primary key #${i}, aborting test.${NC}"
            return 1
        fi
        local new_key=$(echo "$output_add" | grep "API Key:" | awk '{print $3}')
        primary_keys["$key_name"]="$new_key"
    done

    echo -e "${INFO_EMOJI} Primary keys created:"
    for name in "${!primary_keys[@]}"; do echo " - $name: ${primary_keys[$name]}"; done

    sleep 5 # Allow time for propagation

    # 2. Create 2 aliases for each primary key
    echo -e "${INFO_EMOJI} Creating 2 aliases for each primary key..."
    for name in "${!primary_keys[@]}"; do
        local primary_key_value=${primary_keys[$name]}
        local alias_list=""
        for j in {1..2}; do
            run_insic_with_key "$primary_key_value" "alias" "add"
            local exit_code_add_alias=$?
            local output_add_alias=$CMD_OUTPUT
            expect_success "Create alias #${j} for key '$name'" "$exit_code_add_alias" "$output_add_alias" "Alias Key:"
            if [ "$exit_code_add_alias" -ne 0 ]; then
                echo -e "${FAILURE_EMOJI} ${RED}Failed to create alias for key '$name', aborting further alias creation for this key.${NC}"
                continue
            fi
            local new_alias=$(echo "$output_add_alias" | grep "Alias Key:" | awk '{print $3}')
            alias_list+="$new_alias "
        done
        primary_to_aliases["$primary_key_value"]=$(echo "$alias_list" | xargs) # trim whitespace
    done

    echo -e "${INFO_EMOJI} Aliases created:"
    for p_key in "${!primary_to_aliases[@]}"; do echo " - For ${p_key:0:12}... -> ${primary_to_aliases[$p_key]}"; done

    sleep 5 # Allow time for propagation

    # 3. Test 'admin insight entities'
    print_header "Testing 'admin insight entities'"
    run_insic "admin" "insight" "entities" "0" "20"
    local exit_code_entities=$?
    local output_entities=$CMD_OUTPUT
    expect_success "Get list of entities" "$exit_code_entities" "$output_entities" "Entity Details"

    for name in "${!primary_keys[@]}"; do
        local p_key_val=${primary_keys[$name]}
        expect_success "Check for primary key '$name' in entities list" "$exit_code_entities" "$output_entities" "$p_key_val"
    done


    # 4. Test 'admin insight entity' for each primary key
    print_header "Testing 'admin insight entity' for each primary key"
    for name in "${!primary_keys[@]}"; do
        local p_key_val=${primary_keys[$name]}
        echo -e "${INFO_EMOJI} Checking entity details for primary key '$name'"
        run_insic "admin" "insight" "entity" "$p_key_val"
        local exit_code_entity=$?
        local output_entity=$CMD_OUTPUT

        expect_success "Get entity details for key '$name'" "$exit_code_entity" "$output_entity" "Entity Details"
        expect_success "  - Verify RootApiKey matches" "$exit_code_entity" "$output_entity" "Root API Key:    $p_key_val"

        # Check that its aliases are listed
        local expected_aliases=${primary_to_aliases[$p_key_val]}
        for alias in $expected_aliases; do
             expect_success "  - Verify alias '$alias' is listed" "$exit_code_entity" "$output_entity" "$alias"
        done
    done


    # 5. Test 'admin insight entity-by-alias' for each alias
    print_header "Testing 'admin insight entity-by-alias' for each alias"
    for p_key_val in "${!primary_to_aliases[@]}"; do
        local aliases_to_check=${primary_to_aliases[$p_key_val]}
        for alias in $aliases_to_check; do
            echo -e "${INFO_EMOJI} Checking entity details for alias '${alias:0:12}...' (should map to '${p_key_val:0:12}...')"
            run_insic "admin" "insight" "entity-by-alias" "$alias"
            local exit_code_alias_lookup=$?
            local output_alias_lookup=$CMD_OUTPUT

            expect_success "Get entity details for alias '${alias:0:12}'" "$exit_code_alias_lookup" "$output_alias_lookup" "Entity Details"
            expect_success "  - Verify RootApiKey matches parent" "$exit_code_alias_lookup" "$output_alias_lookup" "Root API Key:    $p_key_val"
        done
    done

    # 6. Cleanup
    print_header "Cleaning up created keys"
    for name in "${!primary_keys[@]}"; do
        local p_key_val=${primary_keys[$name]}
        echo -e "${INFO_EMOJI} Deleting primary key '$name' ($p_key_val)"
        run_insic "api" "delete" "$p_key_val"
        local exit_code_delete=$?
        expect_success "Delete primary key '$name'" "$exit_code_delete" "$CMD_OUTPUT"
    done
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
    run_insic "ping"
    local ping_exit_code=$?
    if [ "$ping_exit_code" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}CRITICAL: insic ping failed. Aborting tests.${NC}"
        exit 1
    fi
    expect_success "Server Ping" "$ping_exit_code" "$CMD_OUTPUT" "Ping Response:"

    echo -e "${INFO_EMOJI} Starting TKV Insight operations tests..."

    test_insight_retrieval

    echo -e "\n${GREEN}All TKV Insight operations tests completed.${NC}"

    # --- Test Summary ---
    print_header "Test Summary"
    echo -e "${SUCCESS_EMOJI} Successful tests: ${SUCCESSFUL_TESTS_COUNT}"
    if [ "$FAILED_TESTS_COUNT" -gt 0 ]; then
        echo -e "${FAILURE_EMOJI} Failed tests:     ${FAILED_TESTS_COUNT}"
        echo -e "${RED}Some tests failed. Please review the output above.${NC}"
        exit 1
    else
        echo -e "${GREEN}All Insight tests passed successfully!${NC}"
        exit 0
    fi
}

# Run main
main "$@"
