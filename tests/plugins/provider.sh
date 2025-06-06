#!/bin/bash

# Script to test 'insic provider' commands

# Exit on any error
set -e

# --- Configuration ---
INSIC_PATH="$1"
CONFIG_PATH="/tmp/insi-test-cluster/cluster.yaml"
TEST_DIR=$(mktemp -d -t insic_providers_test_XXXXXX)
UNIQUE_ID=$(date +%s)

# --- Colors and Emojis ---
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
BLUE="\033[0;34m"
NC="\033[0m"

SUCCESS_EMOJI="✅"
FAILURE_EMOJI="❌"
INFO_EMOJI="ℹ️"

# --- Counters ---
SUCCESSFUL_TESTS_COUNT=0
FAILED_TESTS_COUNT=0

# --- Global Vars ---
PROVIDER_UUID=""
PROVIDER_NAME="test-provider-${UNIQUE_ID}"
PROVIDER_TYPE="openai"
PROVIDER_API_KEY="sk-test-${UNIQUE_ID}"
PROVIDER_BASE_URL="https://api.test.com/v1"

# --- Helper Functions ---
cleanup() {
    echo -e "${INFO_EMOJI} Cleaning up test directory: $TEST_DIR"
    # Attempt to delete the provider if its UUID is known, to clean up the server state
    if [ -n "$PROVIDER_UUID" ]; then
        echo -e "${INFO_EMOJI} Attempting cleanup of provider $PROVIDER_UUID"
        run_insic "provider" "delete" "$PROVIDER_UUID" >/dev/null 2>&1 || echo "Provider cleanup failed, may have already been deleted."
    fi
    rm -rf "$TEST_DIR"
}

trap cleanup EXIT SIGINT SIGTERM

print_header() {
    echo -e "\n${BLUE}==== $1 ====${NC}"
}

run_insic() {
    local subcommand="$1"
    shift
    local args=("$@")
    echo -e "${INFO_EMOJI} Running: ${INSIC_PATH} --config ${CONFIG_PATH} --root ${subcommand} ${args[*]}" >&2
    "${INSIC_PATH}" --config "${CONFIG_PATH}" --root "${subcommand}" "${args[@]}"
    return $?
}

expect_success() {
    local description="$1"
    local exit_code="$2"
    if [ "$exit_code" -eq 0 ]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: ${description}${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} (Exit code: $exit_code)${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}

expect_error() {
    local description="$1"
    local exit_code="$2"
    if [ "$exit_code" -ne 0 ]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS (Received Expected Error): ${description}${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} - Expected an error, but command succeeded${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}

# --- Test Functions ---

test_create_and_list_provider() {
    print_header "Test: provider new and list"

    local output
    output=$(run_insic "provider" "new" "$PROVIDER_NAME" "$PROVIDER_TYPE" "$PROVIDER_API_KEY" "$PROVIDER_BASE_URL")
    local exit_code=$?
    expect_success "Creating a new provider" "$exit_code"
    if [ "$exit_code" -eq 0 ]; then
        PROVIDER_UUID=$(echo "$output" | grep 'UUID:' | awk '{print $2}')
        if [ -z "$PROVIDER_UUID" ]; then
            echo -e "${FAILURE_EMOJI} ${RED}CRITICAL: Could not parse Provider UUID from output. Cannot proceed.${NC}"
            exit 1
        fi
        echo -e "${SUCCESS_EMOJI} ${GREEN}Created provider with UUID: $PROVIDER_UUID${NC}"
    else
        # If creation fails, we can't continue
        exit 1
    fi

    sleep 2 # Allow for raft propagation

    print_header "Test: provider list"
    output=$(run_insic "provider" "list")
    exit_code=$?
    expect_success "Listing providers" "$exit_code"
    if [ "$exit_code" -eq 0 ]; then
        if echo "$output" | grep -q "$PROVIDER_UUID"; then
            echo -e "${SUCCESS_EMOJI} ${GREEN}Found created provider in the list.${NC}"
        else
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Did not find created provider UUID in list output.${NC}"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        fi
    fi
}

test_update_provider() {
    print_header "Test: provider update commands"
    local new_name="new-provider-name-${UNIQUE_ID}"
    local new_api_key="sk-new-key-${UNIQUE_ID}"
    local new_base_url="https://api.new-url.com/v1"
    local output
    local exit_code

    # Update Display Name
    output=$(run_insic "provider" "update-display-name" "$PROVIDER_UUID" "$new_name")
    exit_code=$?
    expect_success "Updating provider display name" "$exit_code"
    if [ "$exit_code" -eq 0 ] && ! echo "$output" | grep -q "OK"; then
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Expected 'OK' on update-display-name, got: $output ${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Update API Key
    output=$(run_insic "provider" "update-api-key" "$PROVIDER_UUID" "$new_api_key")
    exit_code=$?
    expect_success "Updating provider API key" "$exit_code"
    if [ "$exit_code" -eq 0 ] && ! echo "$output" | grep -q "OK"; then
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Expected 'OK' on update-api-key, got: $output ${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Update Base URL
    output=$(run_insic "provider" "update-base-url" "$PROVIDER_UUID" "$new_base_url")
    exit_code=$?
    expect_success "Updating provider base URL" "$exit_code"
    if [ "$exit_code" -eq 0 ] && ! echo "$output" | grep -q "OK"; then
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Expected 'OK' on update-base-url, got: $output ${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}

test_delete_provider() {
    print_header "Test: provider delete"
    run_insic "provider" "delete" "$PROVIDER_UUID"
    local exit_code=$?
    expect_success "Deleting the provider" "$exit_code"

    sleep 2

    # Verify it's gone
    local list_output
    list_output=$(run_insic "provider" "list")
    local list_exit_code=$?
    expect_success "Listing providers after deletion" "$list_exit_code"
    if [ "$list_exit_code" -eq 0 ]; then
        if echo "$list_output" | grep -q "$PROVIDER_UUID"; then
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Found deleted provider in the list.${NC}"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        else
            echo -e "${SUCCESS_EMOJI} ${GREEN}Provider successfully removed from list.${NC}"
        fi
    fi

    # Set PROVIDER_UUID to empty so cleanup trap doesn't try to delete it again
    PROVIDER_UUID=""
}

test_arg_validation() {
    print_header "Test: Argument Validation"
    local exit_code

    set +e # allow commands to fail without exiting the script

    run_insic "provider" "new" "test-name" "openai"
    exit_code=$?
    expect_error "provider new with missing api-key" "$exit_code"

    run_insic "provider" "delete"
    exit_code=$?
    expect_error "provider delete with no uuid" "$exit_code"

    run_insic "provider" "update-display-name" "some-uuid"
    exit_code=$?
    expect_error "provider update-display-name with no name" "$exit_code"

    set -e # re-enable exit on error
}

# --- Main Execution ---
main() {
    echo -e "${INFO_EMOJI} Starting 'provider' command tests..."

    test_create_and_list_provider
    test_update_provider
    test_delete_provider
    test_arg_validation

    print_header "Test Summary"
    echo -e "${SUCCESS_EMOJI} Successful tests: ${SUCCESSFUL_TESTS_COUNT}"
    if [ "$FAILED_TESTS_COUNT" -gt 0 ]; then
        echo -e "${FAILURE_EMOJI} Failed tests:     ${FAILED_TESTS_COUNT}"
        echo -e "${RED}Provider tests failed. Please review the output above.${NC}"
        exit 1
    else
        echo -e "${GREEN}All provider tests passed successfully!${NC}"
    fi
}

main
