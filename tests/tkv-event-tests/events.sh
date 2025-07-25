#!/bin/bash

# Script to test insic event publish/subscribe functionality

# Exit on any error
set -e

# Configuration (can be overridden by environment variables or command line argument)
# If a command line argument is provided, use it as INSIC_EXE
if [ -n "$1" ]; then
    INSIC_EXE_CMD_ARG="$1"
else
    INSIC_EXE_CMD_ARG=""
fi

INSIC_EXE="${INSIC_EXE_CMD_ARG:-${INSIC_EXE:-../../build/insic}}" # Path to insic executable
CONFIG_FILE="${CONFIG_FILE:-/tmp/insi-test-cluster/cluster.yaml}" # Path to the cluster config
TARGET_NODE="${TARGET_NODE:-node0}" # Target node for commands

# Create a unique temporary directory for this test run
TEST_DIR=$(mktemp -d -t insic_events_test_XXXXXX)
OUTPUT_FILE="$TEST_DIR/output.txt"
SUBSCRIBER_PID_FILE="$TEST_DIR/subscriber.pid"

# Generate a unique topic name for this test run to avoid collisions
TOPIC="test-event-topic-$$"

# Expected messages
MESSAGES=(
    "Hello_World_1_$$"
    "Another_Message_2_$$"
    "Test_Data_3_$$"
)
NUM_MESSAGES=${#MESSAGES[@]}

# --- Helper Functions ---
cleanup() {
    echo "--- Cleaning up ---"
    
    # Clean up the main test subscriber
    if [ -f "$SUBSCRIBER_PID_FILE" ]; then
        SUB_PID=$(cat "$SUBSCRIBER_PID_FILE")
        if ps -p "$SUB_PID" > /dev/null; then
            echo "Killing subscriber process (PID: $SUB_PID)..."
            kill "$SUB_PID" || echo "Failed to kill subscriber PID $SUB_PID, it might have already exited."
            # Wait a moment for the process to terminate
            sleep 2
            if ps -p "$SUB_PID" > /dev/null; then
                echo "Subscriber PID $SUB_PID still alive, sending SIGKILL..."
                kill -9 "$SUB_PID" || echo "Failed to SIGKILL subscriber PID $SUB_PID."
            fi
        else
            echo "Subscriber process (PID: $SUB_PID) already exited."
        fi
    fi
    
    # Clean up any purge test subscribers if they exist
    for PID_VAR in SUB1_PID SUB2_PID SUB3_PID; do
        if [ -n "${!PID_VAR}" ]; then
            PID="${!PID_VAR}"
            if ps -p "$PID" > /dev/null; then
                echo "Killing purge test subscriber (PID: $PID)..."
                kill "$PID" 2>/dev/null || true
                sleep 1
                if ps -p "$PID" > /dev/null; then
                    kill -9 "$PID" 2>/dev/null || true
                fi
            fi
        fi
    done
    
    if [ -d "$TEST_DIR" ]; then
        echo "Removing temporary directory: $TEST_DIR"
        rm -rf "$TEST_DIR"
    fi
    echo "---------------------"
}

# Ensure cleanup runs on script exit or interruption
trap cleanup EXIT SIGINT SIGTERM

# --- Test Steps ---

echo "--- Test Setup ---"
echo "Using insic: $INSIC_EXE"
echo "Using config: $CONFIG_FILE"
echo "Targeting node: $TARGET_NODE"
echo "Test directory: $TEST_DIR"
echo "Output file: $OUTPUT_FILE"
echo "Topic: $TOPIC"
echo "------------------"

# 1. Start the subscriber in the background
echo "Starting subscriber for topic '$TOPIC'..."
# Use --root for simplicity in testing, assuming root key is available via config
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root subscribe "$TOPIC" > "$OUTPUT_FILE" 2>&1 &
SUB_PID=$!
echo "$SUB_PID" > "$SUBSCRIBER_PID_FILE"
echo "Subscriber started with PID: $SUB_PID. Outputting to: $OUTPUT_FILE"

# Give the subscriber a moment to initialize
sleep 5

# Check if subscriber is still running
if ! ps -p "$SUB_PID" > /dev/null; then
    echo "ERROR: Subscriber process (PID: $SUB_PID) failed to start or exited prematurely."
    echo "--- Subscriber Logs (last 20 lines from $OUTPUT_FILE) ---"
    tail -n 20 "$OUTPUT_FILE"
    echo "---------------------------------------------------------"
    exit 1
fi
echo "Subscriber process seems to be running."

# Wait until the subscriber reports a successful connection
echo "Waiting for subscriber to connect..."
if ! timeout 10s grep -q "Successfully connected" <(tail -f "$OUTPUT_FILE"); then
    echo "ERROR: Subscriber did not report successful connection within 10 seconds."
    echo "--- Subscriber Logs ---"
    cat "$OUTPUT_FILE"
    echo "-----------------------"
    exit 1
fi
echo "Subscriber connected."

# 2. Publish messages
echo "Publishing $NUM_MESSAGES messages to topic '$TOPIC'..."
for MSG_DATA in "${MESSAGES[@]}"; do
    echo "Publishing: '$MSG_DATA'"
    "$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root publish "$TOPIC" "$MSG_DATA"
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to publish message '$MSG_DATA'."
        exit 1
    fi
    sleep 0.5 # Small delay between publishes
done
echo "All messages published."

# 3. Wait for messages to be processed by the subscriber
echo "Waiting for messages to be processed (10 seconds)..."
sleep 10

# 4. Kill the subscriber (handled by trap cleanup, but can be explicit here too)
echo "Stopping subscriber (PID: $SUB_PID)..."
kill "$SUB_PID"
wait "$SUB_PID" 2>/dev/null # Suppress "Terminated" message from wait
echo "Subscriber stopped."

# 5. Verify the output
echo "Verifying received messages in $OUTPUT_FILE..."
MISSING_MESSAGES=0
for MSG_DATA in "${MESSAGES[@]}"; do
    # Grep for the data part of the event, as insic subscribe has some prefix
    # Example line: Received event on topic 'test-event-topic-12345': Data=Test_Data_3_12345
    if grep -q "Received event on topic '$TOPIC': Data=$MSG_DATA" "$OUTPUT_FILE"; then
        echo "OK: Found message '$MSG_DATA'"
    else
        echo "ERROR: Did not find message '$MSG_DATA'"
        MISSING_MESSAGES=$((MISSING_MESSAGES + 1))
    fi
done

echo "--- Test Summary ---"
if [ "$MISSING_MESSAGES" -eq 0 ]; then
    echo "SUCCESS: All $NUM_MESSAGES messages were received!"
    echo "See $OUTPUT_FILE for details."
else
    echo "FAILURE: $MISSING_MESSAGES out of $NUM_MESSAGES messages were NOT received."
    echo "--- Full Subscriber Output ($OUTPUT_FILE) ---"
    cat "$OUTPUT_FILE"
    echo "----------------------------------------------"
    exit 1
fi
echo "--------------------"

# --- Additional Test: Purge Functionality ---
echo ""
echo "--- Testing Purge Functionality ---"

# Start multiple subscribers
echo "Starting 3 subscribers for purge test..."
PURGE_OUTPUT_DIR="$TEST_DIR/purge_test"
mkdir -p "$PURGE_OUTPUT_DIR"

# Start subscriber 1
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root subscribe "purge-topic-1" > "$PURGE_OUTPUT_DIR/sub1.txt" 2>&1 &
SUB1_PID=$!
echo "Subscriber 1 started with PID: $SUB1_PID"

# Start subscriber 2
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root subscribe "purge-topic-2" > "$PURGE_OUTPUT_DIR/sub2.txt" 2>&1 &
SUB2_PID=$!
echo "Subscriber 2 started with PID: $SUB2_PID"

# Start subscriber 3 (same topic as subscriber 1)
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root subscribe "purge-topic-1" > "$PURGE_OUTPUT_DIR/sub3.txt" 2>&1 &
SUB3_PID=$!
echo "Subscriber 3 started with PID: $SUB3_PID"

# Wait for all subscribers to connect
echo "Waiting for all subscribers to connect..."
sleep 5

# Check subscriber outputs for successful connection
echo "Checking subscriber connection status..."
for i in 1 2 3; do
    SUB_FILE="$PURGE_OUTPUT_DIR/sub$i.txt"
    if grep -q "Successfully connected" "$SUB_FILE"; then
        echo "Subscriber $i connected successfully"
    else
        echo "WARNING: Subscriber $i may not have connected. Output:"
        cat "$SUB_FILE"
        echo "---"
    fi
done

# Check that all subscribers are running
ALL_RUNNING=true
RUNNING_COUNT=0
for PID in $SUB1_PID $SUB2_PID $SUB3_PID; do
    if ps -p "$PID" > /dev/null; then
        RUNNING_COUNT=$((RUNNING_COUNT + 1))
    else
        echo "ERROR: Subscriber PID $PID is not running"
        ALL_RUNNING=false
    fi
done

echo "Running subscribers: $RUNNING_COUNT out of 3"

if [ "$RUNNING_COUNT" -eq 0 ]; then
    echo "ERROR: No subscribers are running. Check subscriber outputs:"
    for i in 1 2 3; do
        echo "--- Subscriber $i output ---"
        cat "$PURGE_OUTPUT_DIR/sub$i.txt"
        echo "---"
    done
    exit 1
fi

# Run purge command even if not all subscribers started (to test partial purge)
echo "Running purge command..."
PURGE_OUTPUT="$PURGE_OUTPUT_DIR/purge_result.txt"
# Use || true to prevent set -e from exiting on non-zero exit code
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root purge > "$PURGE_OUTPUT" 2>&1 || PURGE_EXIT_CODE=$?
# If the command succeeded, PURGE_EXIT_CODE will be unset, so set it to 0
PURGE_EXIT_CODE=${PURGE_EXIT_CODE:-0}

# Check purge output
echo "Purge command output:"
if [ -f "$PURGE_OUTPUT" ]; then
    cat "$PURGE_OUTPUT"
else
    echo "ERROR: Purge output file not found at $PURGE_OUTPUT"
    echo "Directory contents:"
    ls -la "$PURGE_OUTPUT_DIR" || echo "Directory does not exist"
    exit 1
fi
echo "Purge exit code: $PURGE_EXIT_CODE"

if [ $PURGE_EXIT_CODE -ne 0 ]; then
    echo "ERROR: Purge command failed with exit code $PURGE_EXIT_CODE"
    # Show more diagnostic info
    echo "--- Diagnostic Information ---"
    echo "Current running subscribers:"
    for PID in $SUB1_PID $SUB2_PID $SUB3_PID; do
        if ps -p "$PID" > /dev/null; then
            echo "  PID $PID is still running"
        else
            echo "  PID $PID has exited"
        fi
    done
    # Clean up
    for PID in $SUB1_PID $SUB2_PID $SUB3_PID; do
        if ps -p "$PID" > /dev/null; then
            kill "$PID" 2>/dev/null || true
        fi
    done
    exit 1
fi

# Extract disconnected count from output
DISCONNECTED_COUNT=$(grep -oE "Successfully purged ([0-9]+) event subscription" "$PURGE_OUTPUT" | grep -oE "[0-9]+" | head -1)
if [ -z "$DISCONNECTED_COUNT" ]; then
    # Check for "No active event subscriptions" message
    if grep -q "No active event subscriptions found to purge" "$PURGE_OUTPUT"; then
        DISCONNECTED_COUNT=0
    else
        echo "ERROR: Could not parse purge output"
        exit 1
    fi
fi


echo "Purge reported $DISCONNECTED_COUNT disconnected sessions"

# Wait a moment for processes to actually die
sleep 2

# Check that subscriber processes have terminated
STILL_RUNNING=0
TERMINATED=0
for PID in $SUB1_PID $SUB2_PID $SUB3_PID; do
    if ps -p "$PID" > /dev/null; then
        echo "WARNING: Subscriber PID $PID is still running after purge"
        STILL_RUNNING=$((STILL_RUNNING + 1))
        # Force kill it for cleanup
        kill -9 "$PID" 2>/dev/null || true
    else
        echo "OK: Subscriber PID $PID has terminated"
        TERMINATED=$((TERMINATED + 1))
    fi
done

echo "--- Purge Test Summary ---"
echo "- Started with $RUNNING_COUNT running subscribers"
echo "- Purge reported $DISCONNECTED_COUNT disconnected sessions"
echo "- $TERMINATED subscribers terminated after purge"
echo "- $STILL_RUNNING subscribers still running after purge"

# Success if purge count matches what was running and all have terminated
if [ "$DISCONNECTED_COUNT" -eq "$RUNNING_COUNT" ] && [ "$STILL_RUNNING" -eq 0 ] && [ "$RUNNING_COUNT" -gt 0 ]; then
    echo "SUCCESS: Purge functionality working correctly!"
else
    echo "FAILURE: Purge test failed"
    echo "- Expected to disconnect $RUNNING_COUNT sessions"
    echo "- Purge reported $DISCONNECTED_COUNT disconnected"
    echo "- $STILL_RUNNING processes still running after purge"
    exit 1
fi
echo "--------------------------"

exit 0
