#!/bin/bash

# Script to test the blob operations of the insic CLI

# --- Configuration ---
INSIC_PATH=""
DEFAULT_CONFIG_PATH="/tmp/insi-test-cluster/cluster.yaml"
TEST_DIR="/tmp/insic-blob-tests"

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

# Function to get node home directories and IDs from cluster.yaml
get_node_details() {
    local config_path="$1"
    # The 'nodes' block is a map, not an array. We need to get the keys (node IDs)
    # and combine them with the global 'insiHome' value.
    local insi_home
    insi_home=$(yq -r '.insiHome' "${config_path}")

    yq -r '.nodes | keys | .[]' "${config_path}" | while read -r node_id; do
        echo "${node_id} ${insi_home}"
    done
}

# Function to run the insic command
run_insic() {
    local subcommand="$1"
    shift
    local args=("$@")
    local cmd_output
    local exit_code

    echo -e "${INFO_EMOJI} Running: ${INSIC_PATH} --root --config ${DEFAULT_CONFIG_PATH} ${subcommand} ${args[*]}" >&2
    cmd_output=$("${INSIC_PATH}" --root --config "${DEFAULT_CONFIG_PATH}" "${subcommand}" "${args[@]}" 2>&1)
    exit_code=$?
    echo "${cmd_output}"
    # The line below is for debugging
    # echo "Exit code: ${exit_code}" >&2
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
        echo -e "   Output: ${output_content}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}

# Function to assert a failure
expect_failure() {
    local description="$1"
    local exit_code="$2"
    local output_content="$3"
    local grep_text="$4"

    if [ "$exit_code" -ne 0 ]; then
        if [ -n "$grep_text" ]; then
            if echo "$output_content" | grep -q "$grep_text"; then
                echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS (Received Expected Failure): ${description}${NC}"
                SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
            else
                echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} - Command failed but did not contain expected text '${grep_text}' (Exit code: $exit_code)${NC}"
                echo -e "   Output: ${output_content}"
                FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
            fi
        else
            echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS (Received Expected Failure): ${description}${NC}"
            SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
        fi
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} - Expected a failure, but command succeeded (Exit code: $exit_code)${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
}

# --- Test Functions ---

test_blob_lifecycle() {
    print_header "Test: Blob Lifecycle"
    local key_prefix="test_blob_$(date +%s)"
    local key="${key_prefix}_file1.txt"
    
    local upload_file="${TEST_DIR}/upload.txt"
    local download_file="${TEST_DIR}/download.txt"
    
    # 1. Setup: Create a test file to upload
    echo "This is a test blob for insic." > "${upload_file}"
    local original_hash=$(sha256sum "${upload_file}" | awk '{print $1}')
    echo -e "${INFO_EMOJI} Created test file '${upload_file}' with SHA256 hash: ${original_hash}"

    # 2. Upload the blob
    local upload_output=$(run_insic "blob" "upload" "$key" "${upload_file}")
    local upload_exit_code=$?
    expect_success "Upload blob with key '$key'" "$upload_exit_code" "$upload_output"

    # 3. Iterate to verify blob key exists
    local iterate_output=$(run_insic "blob" "iterate" "${key_prefix}")
    local iterate_exit_code=$?
    if [ "$iterate_exit_code" -eq 0 ] && echo "$iterate_output" | grep -q "^${key}$"; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Iterate found blob key '$key'${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Iterate did not find blob key '$key'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # 4. Download the blob
    local download_output=$(run_insic "blob" "download" "$key" "${download_file}")
    local download_exit_code=$?
    expect_success "Download blob with key '$key'" "$download_exit_code" "$download_output"

    # 5. Verify downloaded content
    if [ -f "${download_file}" ]; then
        local downloaded_hash=$(sha256sum "${download_file}" | awk '{print $1}')
        echo -e "${INFO_EMOJI} Downloaded file hash: ${downloaded_hash}"
        if [ "${original_hash}" == "${downloaded_hash}" ]; then
            echo -e "${SUCCESS_EMOJI} ${GREEN}Downloaded file content matches original${NC}"
            SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
        else
            echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Hash mismatch between uploaded and downloaded file${NC}"
            FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        fi
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Downloaded file does not exist at '${download_file}'${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi
    
    # 5b. Verify physical existence on ALL nodes
    echo -e "\n${INFO_EMOJI}${YELLOW} Verifying physical blob existence on ALL nodes...${NC}"
    local key_hash
    key_hash=$(echo -n "$key" | sha256sum | awk '{print $1}')
    local data_scope="test-cluster-prefix" # For --root, defined in client/client.go
    echo -e "${INFO_EMOJI}${YELLOW} > Key: '${key}', Hash: '${key_hash}', Scope: '${data_scope}'${NC}"

    echo -e "${INFO_EMOJI}${YELLOW} > Waiting up to 10s for replication to all nodes...${NC}"

    local insi_home
    insi_home=$(yq -r '.insiHome' "${DEFAULT_CONFIG_PATH}")
    local node_ids
    node_ids=($(yq -r '.nodes | keys | .[]' "${DEFAULT_CONFIG_PATH}"))
    local total_nodes=${#node_ids[@]}
    local found_on_nodes=0
    
    local checks=0
    local max_checks=10
    local all_found=false

    while [ "$checks" -lt "$max_checks" ]; do
        found_on_nodes=0
        for node_id in "${node_ids[@]}"; do
            local blob_path="${insi_home}/${node_id}/blobs/${data_scope}/${key_hash}"
            if [ -f "${blob_path}" ]; then
                found_on_nodes=$((found_on_nodes + 1))
            fi
        done

        if [ "$found_on_nodes" -eq "$total_nodes" ]; then
            all_found=true
            break
        fi

        checks=$((checks + 1))
        echo -e "${WARNING_EMOJI} Found on ${found_on_nodes}/${total_nodes} nodes. Retrying in 1s...${NC}" >&2
        sleep 1
    done

    if [ "$all_found" = true ]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Verified blob file exists on all ${total_nodes} nodes.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Blob not found on all nodes after 10s. Found on ${found_on_nodes}/${total_nodes}.${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        echo -e "${INFO_EMOJI}${YELLOW} > Final check state: running tree on '${insi_home}'...${NC}"
        tree "${insi_home}"
    fi

    # 6. Delete the blob
    local delete_output
    delete_output=$(run_insic "blob" "delete" "$key")
    local delete_exit_code=$?
    expect_success "Delete blob with key '$key'" "$delete_exit_code" "$delete_output"

    # 7. Attempt to download the deleted blob. Poll until it fails, as the tombstone
    #    application via Raft is asynchronous.
    local download_after_delete_exit_code=0
    local attempts=0
    local max_attempts=10
    local download_after_delete_output=""
    while [ "$attempts" -lt "$max_attempts" ]; do
        download_after_delete_output=$(run_insic "blob" "download" "$key" "${download_file}.after_delete" 2>&1)
        download_after_delete_exit_code=$?
        if [ "$download_after_delete_exit_code" -ne 0 ]; then
            break # Success! The command failed as expected.
        fi
        attempts=$((attempts + 1))
        echo -e "${WARNING_EMOJI} Download of deleted blob succeeded (attempt ${attempts}/${max_attempts}). Retrying in 1 second...${NC}" >&2
        sleep 1
    done
    expect_failure "Attempt to download deleted blob '$key'" "$download_after_delete_exit_code" "$download_after_delete_output" "not found"
    
    # 8. Iterate again to verify blob key is gone from the API listing
    echo -e "\n${INFO_EMOJI}${YELLOW} Verifying API no longer lists the deleted blob...${NC}"
    local iterate_after_delete_output
    iterate_after_delete_output=$(run_insic "blob" "iterate" "${key_prefix}")
    local iterate_after_delete_exit_code=$?
    if [ "$iterate_after_delete_exit_code" -eq 0 ] && ! echo "$iterate_after_delete_output" | grep -q "^${key}$"; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Iterate no longer finds deleted blob key '$key'${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Iterate still finds deleted blob key '$key'${NC}"
        echo -e "   Output: ${iterate_after_delete_output}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
    fi

    # 9. Verify physical deletion on ALL nodes
    echo -e "\n${INFO_EMOJI}${YELLOW} Verifying physical blob deletion on ALL nodes...${NC}"
    echo -e "${WARNING_EMOJI} The application's blob garbage collector runs every 30 seconds."
    echo -e "${INFO_EMOJI}${YELLOW} > WAITING 35 SECONDS for the garbage collector and replication...${NC}"
    sleep 35

    local not_found_on_nodes=0
    
    for node_id in "${node_ids[@]}"; do
        local blob_path="${insi_home}/${node_id}/blobs/${data_scope}/${key_hash}"
        if [ ! -f "${blob_path}" ]; then
            not_found_on_nodes=$((not_found_on_nodes + 1))
        else
            echo -e "${WARNING_EMOJI} Blob still exists on node ${node_id} at path ${blob_path}${NC}" >&2
        fi
    done

    if [ "$not_found_on_nodes" -eq "$total_nodes" ]; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}Verified blob file was physically deleted from all ${total_nodes} nodes.${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: Blob was not physically deleted from all nodes. Still found on $((total_nodes - not_found_on_nodes)) node(s).${NC}"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        echo -e "${INFO_EMOJI}${YELLOW} > Final state: running tree on '${insi_home}'...${NC}"
        tree "${insi_home}"
    fi
}


# --- Main Execution ---
main() {
    if [ -z "$1" ]; then
        echo -e "${FAILURE_EMOJI} ${RED}Usage: $0 <path_to_insic_executable>${NC}"
        exit 1
    fi
    INSIC_PATH="$1"

    # Check for yq dependency
    if ! command -v yq &>/dev/null; then
        echo -e "${FAILURE_EMOJI} ${RED}FATAL: 'yq' is not installed. This script requires 'yq' to parse cluster.yaml.${NC}"
        echo -e "${INFO_EMOJI} Please install yq (e.g., 'sudo snap install yq' or 'brew install yq')."
        exit 1
    fi

    if [ ! -f "$INSIC_PATH" ]; then
        echo -e "${FAILURE_EMOJI} ${RED}Error: insic executable not found at '$INSIC_PATH'${NC}"
        exit 1
    fi
    if [ ! -x "$INSIC_PATH" ]; then
        echo -e "${FAILURE_EMOJI} ${RED}Error: insic executable at '$INSIC_PATH' is not executable.${NC}"
        exit 1
    fi

    # Setup test directory
    mkdir -p "${TEST_DIR}"
    echo -e "${INFO_EMOJI} Test directory prepared at ${TEST_DIR}"

    print_header "Pre-Test: Server Ping Check"
    local ping_output=$(run_insic "ping")
    local ping_exit_code=$?
    if [ "$ping_exit_code" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}CRITICAL: insic ping failed. Aborting tests.${NC}"
        rm -rf "${TEST_DIR}"
        exit 1
    else
        echo -e "${SUCCESS_EMOJI} ${GREEN}Server is online. Proceeding with blob tests.${NC}"
    fi

    test_blob_lifecycle

    print_header "Test Summary"
    echo -e "${SUCCESS_EMOJI} Successful tests: ${SUCCESSFUL_TESTS_COUNT}"
    if [ "$FAILED_TESTS_COUNT" -gt 0 ]; then
        echo -e "${FAILURE_EMOJI} Failed tests:     ${FAILED_TESTS_COUNT}"
        echo -e "${RED}Some tests failed. Please review the output above.${NC}"
        # Cleanup and exit
        rm -rf "${TEST_DIR}"
        exit 1
    else
        echo -e "${GREEN}All blob tests passed successfully!${NC}"
    fi
    
    # Cleanup
    rm -rf "${TEST_DIR}"
    echo -e "${INFO_EMOJI} Test directory cleaned up."
}

# Run main
main "$@"
