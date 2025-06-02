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

INSIC_EXE="${INSIC_EXE_CMD_ARG:-${INSIC_EXE:-../../bin/insic}}" # Path to insic executable
CONFIG_FILE="${CONFIG_FILE:-../../cluster.yaml}" # Path to the cluster config
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
sleep 3

# Check if subscriber is still running
if ! ps -p "$SUB_PID" > /dev/null; then
    echo "ERROR: Subscriber process (PID: $SUB_PID) failed to start or exited prematurely."
    echo "--- Subscriber Logs (last 20 lines from $OUTPUT_FILE) ---"
    tail -n 20 "$OUTPUT_FILE"
    echo "---------------------------------------------------------"
    exit 1
fi
echo "Subscriber process seems to be running."

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
    # Example line: Received event on topic 'test-event-topic-12345': Test_Data_3_12345
    if grep -q "Received event on topic '$TOPIC': $MSG_DATA" "$OUTPUT_FILE"; then
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

exit 0
