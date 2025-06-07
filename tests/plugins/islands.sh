#!/bin/bash

# Script to test 'insic island' commands

# Exit on any error
set -e

# --- Configuration ---
INSIC_PATH="$1"
CONFIG_PATH="/tmp/insi-test-cluster/cluster.yaml"
TEST_DIR=$(mktemp -d -t insic_islands_test_XXXXXX)
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
ISLAND_UUID=""
ISLAND_NAME="test-island-${UNIQUE_ID}"
ISLAND_SLUG="test-island-slug-${UNIQUE_ID}"
ISLAND_DESC="A test island for insic"

OBJECT_UUID=""

# --- Helper Functions ---
cleanup() {
    echo -e "${INFO_EMOJI} Cleaning up test directory: $TEST_DIR"
    # Attempt to delete the island if its UUID is known, to clean up the server state
    if [ -n "$ISLAND_UUID" ]; then
        echo -e "${INFO_EMOJI} Attempting cleanup of island $ISLAND_UUID"
        run_insic "island" "delete" "$ISLAND_UUID" >/dev/null 2>&1 || echo "Island cleanup failed, may have already been deleted."
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

test_create_island() {
    print_header "Test: island new"
    local output
    output=$(run_insic "island" "new" "$ISLAND_NAME" "$ISLAND_SLUG" "$ISLAND_DESC")
    local exit_code=$?
    expect_success "Creating a new island" "$exit_code"
    if [ "$exit_code" -eq 0 ]; then
        ISLAND_UUID=$(echo "$output" | grep 'UUID:' | awk '{print $2}')
        if [ -z "$ISLAND_UUID" ]; then
            echo -e "${FAILURE_EMOJI} ${RED}CRITICAL: Could not parse Island UUID from output. Cannot proceed.${NC}"
            exit 1
        fi
        echo -e "${SUCCESS_EMOJI} ${GREEN}Created island with UUID: $ISLAND_UUID${NC}"
    fi
}

test_list_islands() {
    print_header "Test: island list"
    local output
    output=$(run_insic "island" "list")
    local exit_code=$?
    expect_success "Listing islands" "$exit_code"
    if [ "$exit_code" -eq 0 ]; then
        if echo "$output" | grep -q "$ISLAND_UUID"; then
            echo -e "${SUCCESS_EMOJI} ${GREEN}Found created island in the list.${NC}"
        else
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Did not find created island UUID in list output.${NC}"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        fi
    fi
}

test_update_island() {
    print_header "Test: island update commands"
    local new_name="new-name-${UNIQUE_ID}"
    local new_desc="new-description-${UNIQUE_ID}"
    local new_slug="new-slug-${UNIQUE_ID}"
    local output
    local exit_code

    # Update Name
    output=$(run_insic "island" "update-name" "$ISLAND_UUID" "$new_name")
    exit_code=$?
    expect_success "Updating island name" "$exit_code"
    if [ "$exit_code" -eq 0 ] && ! echo "$output" | grep -q "OK"; then
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Expected 'OK' on update-name, got: $output ${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
    # Update Description
    output=$(run_insic "island" "update-description" "$ISLAND_UUID" "$new_desc")
    exit_code=$?
    expect_success "Updating island description" "$exit_code"
     if [ "$exit_code" -eq 0 ] && ! echo "$output" | grep -q "$new_desc"; then
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Did not find new description in update output.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # Update Slug
    output=$(run_insic "island" "update-slug" "$ISLAND_UUID" "$new_slug")
    exit_code=$?
    expect_success "Updating island slug" "$exit_code"
    if [ "$exit_code" -eq 0 ] && ! echo "$output" | grep -q "$new_slug"; then
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Did not find new slug in update output.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}

test_resource_management() {
    print_header "Test: island resource management"
    # Step 1: Create an object to use as a resource
    local test_file="$TEST_DIR/resource_file.txt"
    echo "resource data" > "$test_file"
    local upload_output
    upload_output=$(run_insic "object" "upload" "$test_file")
    local upload_exit_code=$?
    expect_success "Uploading a file to be used as a resource" "$upload_exit_code"
    if [ "$upload_exit_code" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}CRITICAL: Cannot create resource object. Aborting resource tests.${NC}"
        exit 1
    fi
    OBJECT_UUID=$(echo "$upload_output" | grep 'ObjectID:' | awk '{print $2}')
    echo -e "${SUCCESS_EMOJI} ${GREEN}Created object resource with UUID: $OBJECT_UUID${NC}"

    # Step 2: Add resource
    run_insic "island" "add-resource" "$ISLAND_UUID" "$OBJECT_UUID" "text_file" "A test resource"
    expect_success "Adding a resource to the island" "$?"

    sleep 5

    # Step 3: List resources and verify
    local list_output
    list_output=$(run_insic "island" "list-resources" "$ISLAND_UUID")
    local list_exit_code=$?
    expect_success "Listing resources for the island" "$list_exit_code"
    if [ "$list_exit_code" -eq 0 ]; then
        if echo "$list_output" | grep -q "$OBJECT_UUID"; then
            echo -e "${SUCCESS_EMOJI} ${GREEN}Found added resource in list.${NC}"
        else
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Did not find added resource in list output.${NC}"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        fi
    fi

    # Step 4: Remove resource
    run_insic "island" "remove-resources" "$ISLAND_UUID" "$OBJECT_UUID"
    expect_success "Removing a resource from the island" "$?"

    sleep 5

    # Step 5: List resources again and verify it's empty
    list_output=$(run_insic "island" "list-resources" "$ISLAND_UUID")
    list_exit_code=$?
    expect_success "Listing resources after removal" "$list_exit_code"
    if [ "$list_exit_code" -eq 0 ]; then
        if echo "$list_output" | grep -q "No resources found"; then
            echo -e "${SUCCESS_EMOJI} ${GREEN}Resource list is correctly empty after removal.${NC}"
        else
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Resource list was not empty after removal.${NC}"
            echo "Output was: $list_output"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        fi
    fi
}

test_delete_island() {
    print_header "Test: island delete"
    run_insic "island" "delete" "$ISLAND_UUID"
    local exit_code=$?
    expect_success "Deleting the island" "$exit_code"

    sleep 5
    
    # Verify it's gone
    local list_output
    list_output=$(run_insic "island" "list")
    local list_exit_code=$?
    expect_success "Listing islands after deletion" "$list_exit_code"
    if [ "$list_exit_code" -eq 0 ]; then
        if echo "$list_output" | grep -q "$ISLAND_UUID"; then
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Found deleted island in the list.${NC}"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        else
            echo -e "${SUCCESS_EMOJI} ${GREEN}Island successfully removed from list.${NC}"
        fi
    fi

    # Set ISLAND_UUID to empty so cleanup trap doesn't try to delete it again
    ISLAND_UUID=""
}

# --- Main Execution ---
main() {
    echo -e "${INFO_EMOJI} Starting 'island' command tests..."

    test_create_island
    test_list_islands
    test_update_island
    test_resource_management
    test_delete_island

    print_header "Test Summary"
    echo -e "${SUCCESS_EMOJI} Successful tests: ${SUCCESSFUL_TESTS_COUNT}"
    if [ "$FAILED_TESTS_COUNT" -gt 0 ]; then
        echo -e "${FAILURE_EMOJI} Failed tests:     ${FAILED_TESTS_COUNT}"
        echo -e "${RED}Island tests failed. Please review the output above.${NC}"
        exit 1
    else
        echo -e "${GREEN}All island tests passed successfully!${NC}"
    fi
}

main
