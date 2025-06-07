#!/bin/bash

# Script to test 'insic object' commands

# Exit on any error
set -e

# --- Configuration ---
# The insic executable path is passed as the first argument ($1)
INSIC_PATH="$1"
CONFIG_PATH="/tmp/insi-test-cluster/cluster.yaml"
TEST_DIR=$(mktemp -d -t insic_objects_test_XXXXXX)

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
UPLOADED_OBJECT_UUID=""
SOURCE_FILE_PATH=""
SOURCE_FILE_HASH=""

# --- Helper Functions ---
cleanup() {
    echo -e "${INFO_EMOJI} Cleaning up test directory: $TEST_DIR"
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
    local cmd_output
    local exit_code

    echo -e "${INFO_EMOJI} Running: ${INSIC_PATH} --config ${CONFIG_PATH} --root ${subcommand} ${args[*]}" >&2
    cmd_output=$("${INSIC_PATH}" --config "${CONFIG_PATH}" --root "${subcommand}" "${args[@]}")
    exit_code=$?
    echo -e "${cmd_output}"
    return ${exit_code}
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

setup_test_file() {
    print_header "Setup: Creating a test file"
    SOURCE_FILE_PATH="$TEST_DIR/source.txt"
    echo "This is a test file for insic object storage. Timestamp: $(date +%s%N)" > "$SOURCE_FILE_PATH"
    SOURCE_FILE_HASH=$(sha256sum "$SOURCE_FILE_PATH" | awk '{print $1}')
    echo -e "${INFO_EMOJI} Created test file at $SOURCE_FILE_PATH"
    echo -e "${INFO_EMOJI} SHA256 hash: $SOURCE_FILE_HASH"
}

test_object_upload() {
    print_header "Test: object upload"
    local output
    local exit_code
    output=$(run_insic "object" "upload" "$SOURCE_FILE_PATH")
    exit_code=$?
    expect_success "Uploading file '$SOURCE_FILE_PATH'" "$exit_code"
    if [ "$exit_code" -eq 0 ]; then
        UPLOADED_OBJECT_UUID=$(echo "$output" | grep 'ObjectID:' | awk '{print $2}')
        if [ -z "$UPLOADED_OBJECT_UUID" ]; then
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Could not parse ObjectID from upload output.${NC}"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
            # Critical failure, cannot proceed
            exit 1
        else
            echo -e "${SUCCESS_EMOJI} ${GREEN}Successfully parsed ObjectID: $UPLOADED_OBJECT_UUID${NC}"
        fi
    fi
}

test_object_hash() {
    print_header "Test: object hash"
    if [ -z "$UPLOADED_OBJECT_UUID" ]; then
        echo -e "${FAILURE_EMOJI} ${RED}Skipping hash test: No ObjectID available.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        return
    fi
    local output
    local exit_code
    output=$(run_insic "object" "hash" "$UPLOADED_OBJECT_UUID")
    exit_code=$?
    expect_success "Getting hash for ObjectID '$UPLOADED_OBJECT_UUID'" "$exit_code"
    if [ "$exit_code" -eq 0 ]; then
        local returned_hash=$(echo "$output" | grep 'SHA256:' | awk '{print $2}')
        if [ "$returned_hash" == "$SOURCE_FILE_HASH" ]; then
            echo -e "${SUCCESS_EMOJI} ${GREEN}Returned hash matches source file hash.${NC}"
        else
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Hash mismatch!${NC}"
            echo -e "  Expected: $SOURCE_FILE_HASH"
            echo -e "  Returned: $returned_hash"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        fi
    fi
}

test_object_download() {
    print_header "Test: object download"
    if [ -z "$UPLOADED_OBJECT_UUID" ]; then
        echo -e "${FAILURE_EMOJI} ${RED}Skipping download test: No ObjectID available.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        return
    fi
    local download_path="$TEST_DIR/downloaded.txt"
    local exit_code
    run_insic "object" "download" "$UPLOADED_OBJECT_UUID" "$download_path" >/dev/null # Suppress progress bar output for cleaner logs
    exit_code=$?
    expect_success "Downloading ObjectID '$UPLOADED_OBJECT_UUID' to '$download_path'" "$exit_code"
    if [ "$exit_code" -eq 0 ]; then
        local downloaded_hash=$(sha256sum "$download_path" | awk '{print $1}')
        if [ "$downloaded_hash" == "$SOURCE_FILE_HASH" ]; then
            echo -e "${SUCCESS_EMOJI} ${GREEN}Downloaded file content matches source file content.${NC}"
        else
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Downloaded file content mismatch!${NC}"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        fi
    fi
}

test_arg_validation() {
    print_header "Test: Argument Validation"
    local exit_code

    set +e
    run_insic "object" "upload"
    exit_code=$?
    expect_error "object upload with no filepath" "$exit_code"

    run_insic "object" "download" "some-uuid"
    exit_code=$?
    expect_error "object download with no destination path" "$exit_code"

    run_insic "object" "hash"
    exit_code=$?
    expect_error "object hash with no uuid" "$exit_code"
    set -e
}

# --- Main Execution ---
main() {
    echo -e "${INFO_EMOJI} Starting 'object' command tests..."

    setup_test_file
    test_object_upload
    test_object_hash
    test_object_download
    test_arg_validation

    print_header "Test Summary"
    echo -e "${SUCCESS_EMOJI} Successful tests: ${SUCCESSFUL_TESTS_COUNT}"
    if [ "$FAILED_TESTS_COUNT" -gt 0 ]; then
        echo -e "${FAILURE_EMOJI} Failed tests:     ${FAILED_TESTS_COUNT}"
        echo -e "${RED}Object tests failed. Please review the output above.${NC}"
        exit 1
    else
        echo -e "${GREEN}All object tests passed successfully!${NC}"
    fi
}

main
