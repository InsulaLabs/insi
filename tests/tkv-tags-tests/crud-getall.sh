#!/bin/bash

# Script to test TKV tag iteration with offset and limit using insic CLI

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
# $1: subcommand (e.g., "set", "get", "iterate")
# $2+: arguments for the subcommand
run_insic() {
    local subcommand="$1"
    shift
    local args=("$@")
    local cmd_output
    local exit_code

    echo -e "${INFO_EMOJI} Running: ${INSIC_PATH} --config ${DEFAULT_CONFIG_PATH} ${subcommand} ${args[*]}" >&2
    cmd_output=$("${INSIC_PATH}" --config "${DEFAULT_CONFIG_PATH}" "${subcommand}" "${args[@]}")
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
    fi
}

# Function to assert an expected error (not strictly needed for these tests but good practice)
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
    fi
}

# Function to assert output matches an expected list of keys (order matters)
# $1: Test description
# $2: Exit code of the command
# $3: Actual output (multi-line string of keys)
# $4+: Expected keys (passed as separate arguments)
assert_keys_match() {
    local description="$1"
    local exit_code="$2"
    local actual_output="$3"
    shift 3
    # Handle the case of no expected keys (e.g. an empty array)
    local expected_keys_array=()
    if [ "$#" -gt 0 ]; then
        expected_keys_array=("$@")
    fi

    if [ "$exit_code" -ne 0 ]; then
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} - Command failed unexpectedly with exit code $exit_code${NC}"
        echo -e "   Output: $actual_output"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        return 1
    fi

    local actual_keys_array=()
    while IFS= read -r line; do
        if [ -n "$line" ]; then # Avoid empty lines if any
            actual_keys_array+=("$line")
        fi
    done <<< "$actual_output"

    local expected_count=${#expected_keys_array[@]}
    local actual_count=${#actual_keys_array[@]}
    local match=true

    if [ "$actual_count" -ne "$expected_count" ]; then
        match=false
    else
        for i in "${!expected_keys_array[@]}"; do
            if [[ "${actual_keys_array[$i]}" != "${expected_keys_array[$i]}" ]]; then
                match=false
                break
            fi
        done
    fi

    if $match; then
        echo -e "${SUCCESS_EMOJI} ${GREEN}SUCCESS: ${description}${NC}"
        SUCCESSFUL_TESTS_COUNT=$((SUCCESSFUL_TESTS_COUNT + 1))
        # For debugging or verbose success:
        # if [ "$expected_count" -gt 0 ]; then
        #     echo -e "   Expected (${expected_count} items): (${expected_keys_array[*]})"
        #     echo -e "   Got      (${actual_count} items): (${actual_keys_array[*]})"
        # else
        #     echo -e "   Expected and got 0 items."
        # fi
    else
        echo -e "${FAILURE_EMOJI} ${RED}FAILURE: ${description} - Key mismatch.${NC}"
        echo -e "   Expected (${expected_count} items): (${expected_keys_array[*]})"
        echo -e "   Got      (${actual_count} items): (${actual_keys_array[*]})"
        FAILED_TESTS_COUNT=$((FAILED_TESTS_COUNT + 1))
        return 1
    fi
    return 0
}


# --- Test Functions ---

test_tag_iteration_and_pagination() {
    print_header "Test: Tag Iteration with Offset and Limit"
    local base_key_name="tkp_key_$(date +%s)" # tkp for TagKeyPaginate
    local tag_name="tkp_tag_$(date +%s)"
    local num_items=7
    local all_expected_keys=()
    local set_output
    local set_exit_code
    local tag_output
    local tag_exit_code

    echo -e "${INFO_EMOJI} Setting up $num_items keys with tag '$tag_name'"
    for i in $(seq 1 $num_items); do
        local key="${base_key_name}_${i}"
        local value="val_for_${key}"
        all_expected_keys+=("$key")

        set_output=$(run_insic "set" "$key" "$value")
        set_exit_code=$?
        expect_success "Set key '$key' to '$value'" "$set_exit_code" # "$set_output" -> too verbose

        tag_output=$(run_insic "tag" "$key" "$tag_name")
        tag_exit_code=$?
        expect_success "Tag key '$key' with '$tag_name'" "$tag_exit_code" # "$tag_output"
    done
    echo -e "${INFO_EMOJI} Expected keys for tag '$tag_name': (${all_expected_keys[*]})"


    # Test Case 1: Get all (limit greater than total items)
    local output exit_code
    local limit_high=$((num_items + 3))
    output=$(run_insic "iterate" "tag" "$tag_name" "0" "$limit_high")
    exit_code=$?
    assert_keys_match "Iterate tag '$tag_name', offset 0, limit $limit_high (all items)" "$exit_code" "$output" "${all_expected_keys[@]}"

    # Test Case 2: Get first N items (limit < total)
    local limit_small=3
    local expected_subset_tc2=("${all_expected_keys[@]:0:$limit_small}")
    output=$(run_insic "iterate" "tag" "$tag_name" "0" "$limit_small")
    exit_code=$?
    assert_keys_match "Iterate tag '$tag_name', offset 0, limit $limit_small (first $limit_small)" "$exit_code" "$output" "${expected_subset_tc2[@]}"

    # Test Case 3: Get middle N items (offset and limit)
    local offset_mid=2
    local limit_mid=3
    local expected_subset_tc3=("${all_expected_keys[@]:$offset_mid:$limit_mid}")
    output=$(run_insic "iterate" "tag" "$tag_name" "$offset_mid" "$limit_mid")
    exit_code=$?
    assert_keys_match "Iterate tag '$tag_name', offset $offset_mid, limit $limit_mid (middle $limit_mid)" "$exit_code" "$output" "${expected_subset_tc3[@]}"

    # Test Case 4: Get items towards the end (offset + limit might exceed total, check partial)
    local offset_near_end=$((num_items - 2)) # e.g., if num_items=7, offset=5
    local limit_near_end=3                  # e.g., request 3 items from index 5 (keys 6, 7)
    local num_expected_tc4=$((num_items - offset_near_end))
    if [ $num_expected_tc4 -gt $limit_near_end ]; then num_expected_tc4=$limit_near_end; fi
    local expected_subset_tc4=("${all_expected_keys[@]:$offset_near_end:$num_expected_tc4}")
    output=$(run_insic "iterate" "tag" "$tag_name" "$offset_near_end" "$limit_near_end")
    exit_code=$?
    assert_keys_match "Iterate tag '$tag_name', offset $offset_near_end, limit $limit_near_end (partial end)" "$exit_code" "$output" "${expected_subset_tc4[@]}"

    # Test Case 5: Offset beyond total items
    local offset_beyond=$((num_items + 2))
    output=$(run_insic "iterate" "tag" "$tag_name" "$offset_beyond" "3")
    exit_code=$?
    assert_keys_match "Iterate tag '$tag_name', offset $offset_beyond (beyond total, expect 0)" "$exit_code" "$output" # No expected keys

    # Test Case 6: Default offset and limit (CLI defaults: 0 offset, 100 limit)
    output=$(run_insic "iterate" "tag" "$tag_name")
    exit_code=$?
    assert_keys_match "Iterate tag '$tag_name', default offset/limit (all items)" "$exit_code" "$output" "${all_expected_keys[@]}"

    # Test Case 7: Limit 0 (server should default to 100, so all items if <100)
    output=$(run_insic "iterate" "tag" "$tag_name" "0" "0")
    exit_code=$?
    assert_keys_match "Iterate tag '$tag_name', offset 0, limit 0 (server defaults limit, all items)" "$exit_code" "$output" "${all_expected_keys[@]}"

    # Test Case 8: Negative offset (server should default to 0)
    local limit_for_neg_offset=3
    local expected_subset_tc8=("${all_expected_keys[@]:0:$limit_for_neg_offset}")
    output=$(run_insic "iterate" "tag" "$tag_name" "-5" "$limit_for_neg_offset")
    exit_code=$?
    assert_keys_match "Iterate tag '$tag_name', negative offset, limit $limit_for_neg_offset (server defaults offset)" "$exit_code" "$output" "${expected_subset_tc8[@]}"

    # Test Case 9: Non-existent tag
    local non_existent_tag="nonexistent_tag_$(date +%s)"
    output=$(run_insic "iterate" "tag" "$non_existent_tag")
    exit_code=$?
    assert_keys_match "Iterate non-existent tag '$non_existent_tag' (expect 0 items)" "$exit_code" "$output" # No expected keys

    # Cleanup
    echo -e "${INFO_EMOJI} Cleaning up $num_items keys for tag '$tag_name'"
    for key_to_delete in "${all_expected_keys[@]}"; do
        local del_output
        local del_exit_code
        del_output=$(run_insic "delete" "$key_to_delete")
        del_exit_code=$?
        expect_success "Delete key '$key_to_delete'" "$del_exit_code" # "$del_output"
    done
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

    echo -e "${INFO_EMOJI} Starting TKV tag iteration tests with insic: ${INSIC_PATH}"

    test_tag_iteration_and_pagination

    echo -e "\n${GREEN}All TKV tag iteration tests completed.${NC}"

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


# All crud tests for tags as much as the http endpoint permits. We have "get all keys with tag" that expectes an offest and a limit
# we need to exercise this enough to validate that the limit offset logic IS CORRECT.