#!/bin/bash

# Advanced tests for event purge functionality
# This script tests edge cases and error conditions

# Exit on any error
set -e

# Configuration
if [ -n "$1" ]; then
    INSIC_EXE_CMD_ARG="$1"
else
    INSIC_EXE_CMD_ARG=""
fi

INSIC_EXE="${INSIC_EXE_CMD_ARG:-${INSIC_EXE:-../../build/insic}}"
CONFIG_FILE="${CONFIG_FILE:-/tmp/insi-test-cluster/cluster.yaml}"
TARGET_NODE="${TARGET_NODE:-node0}"

# Create test directory
TEST_DIR=$(mktemp -d -t insic_purge_advanced_test_XXXXXX)
OUTPUT_DIR="$TEST_DIR/outputs"
mkdir -p "$OUTPUT_DIR"

# Cleanup function
cleanup() {
    echo "--- Cleaning up ---"
    # Kill any remaining test processes
    for PID_FILE in "$TEST_DIR"/*.pid; do
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if ps -p "$PID" > /dev/null 2>&1; then
                echo "Killing process $PID"
                kill "$PID" 2>/dev/null || true
                sleep 1
                if ps -p "$PID" > /dev/null 2>&1; then
                    kill -9 "$PID" 2>/dev/null || true
                fi
            fi
        fi
    done
    rm -rf "$TEST_DIR"
    echo "---------------------"
}

trap cleanup EXIT SIGINT SIGTERM

echo "=== Advanced Event Purge Tests ==="
echo "Using insic: $INSIC_EXE"
echo "Using config: $CONFIG_FILE"
echo "Test directory: $TEST_DIR"
echo "================================="

# Test 1: Purge with no active subscriptions
echo ""
echo "--- Test 1: Purge with no active subscriptions ---"
PURGE_OUTPUT="$OUTPUT_DIR/test1_purge.txt"
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root purge > "$PURGE_OUTPUT" 2>&1 || true

if grep -q "No active event subscriptions found to purge\." "$PURGE_OUTPUT"; then
    echo "✓ PASS: Correctly handled no active subscriptions"
else
    echo "✗ FAIL: Did not handle empty subscription case properly"
    cat "$PURGE_OUTPUT"
    exit 1
fi

# Test 2: Multiple purges in succession
echo ""
echo "--- Test 2: Multiple purges in succession ---"
# Start a subscriber
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root subscribe "test-topic-multi" > "$OUTPUT_DIR/test2_sub.txt" 2>&1 &
SUB_PID=$!
echo "$SUB_PID" > "$TEST_DIR/test2_sub.pid"
sleep 3

# First purge
PURGE1_OUTPUT="$OUTPUT_DIR/test2_purge1.txt"
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root purge > "$PURGE1_OUTPUT" 2>&1 || true

# Second purge (should find nothing)
sleep 2
PURGE2_OUTPUT="$OUTPUT_DIR/test2_purge2.txt"
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root purge > "$PURGE2_OUTPUT" 2>&1 || true

if grep -q "Successfully purged 1 event subscription" "$PURGE1_OUTPUT" && grep -q "No active event subscriptions found to purge\." "$PURGE2_OUTPUT"; then
    echo "✓ PASS: Multiple purges handled correctly"
else
    echo "✗ FAIL: Multiple purges not handled properly"
    echo "First purge:"
    cat "$PURGE1_OUTPUT"
    echo "Second purge:"
    cat "$PURGE2_OUTPUT"
    exit 1
fi

# Test 3: Purge with mixed API keys (create additional API key)
echo ""
echo "--- Test 3: Purge with mixed API keys ---"
# Create a new API key
API_KEY_OUTPUT="$OUTPUT_DIR/test3_apikey.txt"
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root api add "test-key-purge" > "$API_KEY_OUTPUT" 2>&1
NEW_API_KEY=$(grep "API Key:" "$API_KEY_OUTPUT" | awk '{print $3}')

if [ -z "$NEW_API_KEY" ]; then
    echo "✗ FAIL: Could not create test API key"
    cat "$API_KEY_OUTPUT"
    exit 1
fi

echo "Created test API key: ${NEW_API_KEY:0:20}..."

# Start subscribers with root key
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root subscribe "mixed-topic-1" > "$OUTPUT_DIR/test3_root_sub1.txt" 2>&1 &
ROOT_SUB1_PID=$!
echo "$ROOT_SUB1_PID" > "$TEST_DIR/test3_root_sub1.pid"

"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root subscribe "mixed-topic-2" > "$OUTPUT_DIR/test3_root_sub2.txt" 2>&1 &
ROOT_SUB2_PID=$!
echo "$ROOT_SUB2_PID" > "$TEST_DIR/test3_root_sub2.pid"

# Start subscriber with new API key
INSI_API_KEY="$NEW_API_KEY" "$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" subscribe "mixed-topic-1" > "$OUTPUT_DIR/test3_new_sub.txt" 2>&1 &
NEW_SUB_PID=$!
echo "$NEW_SUB_PID" > "$TEST_DIR/test3_new_sub.pid"

sleep 5

# Purge only the new API key's subscriptions
PURGE_NEW_OUTPUT="$OUTPUT_DIR/test3_purge_new.txt"
INSI_API_KEY="$NEW_API_KEY" "$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" purge > "$PURGE_NEW_OUTPUT" 2>&1 || true

# Check that only the new key's subscription was purged
sleep 2
ROOT_SUBS_ALIVE=0
if ps -p "$ROOT_SUB1_PID" > /dev/null 2>&1; then
    ROOT_SUBS_ALIVE=$((ROOT_SUBS_ALIVE + 1))
fi
if ps -p "$ROOT_SUB2_PID" > /dev/null 2>&1; then
    ROOT_SUBS_ALIVE=$((ROOT_SUBS_ALIVE + 1))
fi

NEW_SUB_ALIVE=false
if ps -p "$NEW_SUB_PID" > /dev/null 2>&1; then
    NEW_SUB_ALIVE=true
fi

if [ "$ROOT_SUBS_ALIVE" -eq 2 ] && [ "$NEW_SUB_ALIVE" = false ] && grep -q "Successfully purged 1 event subscription" "$PURGE_NEW_OUTPUT"; then
    echo "✓ PASS: API key isolation working correctly"
else
    echo "✗ FAIL: API key isolation not working"
    echo "Root subs alive: $ROOT_SUBS_ALIVE (expected 2)"
    echo "New sub alive: $NEW_SUB_ALIVE (expected false)"
    cat "$PURGE_NEW_OUTPUT"
    exit 1
fi

# Clean up test 3 subscribers
kill "$ROOT_SUB1_PID" "$ROOT_SUB2_PID" 2>/dev/null || true

# Delete the test API key
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root api delete "$NEW_API_KEY" > /dev/null 2>&1 || true

# Test 4: Concurrent purge and subscribe
echo ""
echo "--- Test 4: Concurrent purge and subscribe ---"
# Start multiple subscribers
for i in {1..5}; do
    "$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root subscribe "concurrent-topic-$i" > "$OUTPUT_DIR/test4_sub$i.txt" 2>&1 &
    echo "$!" > "$TEST_DIR/test4_sub$i.pid"
done

sleep 3

# Start new subscribers while purging
(
    sleep 1
    for i in {6..8}; do
        "$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root subscribe "concurrent-topic-$i" > "$OUTPUT_DIR/test4_sub$i.txt" 2>&1 &
        echo "$!" > "$TEST_DIR/test4_sub$i.pid"
        sleep 0.5
    done
) &

# Purge existing connections
PURGE_CONCURRENT_OUTPUT="$OUTPUT_DIR/test4_purge.txt"
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root purge > "$PURGE_CONCURRENT_OUTPUT" 2>&1 || true

# Wait for everything to settle
sleep 3

# Count how many were purged
PURGED_COUNT=$(grep -oE "Successfully purged ([0-9]+) event subscription" "$PURGE_CONCURRENT_OUTPUT" | grep -oE "[0-9]+" | head -1)
echo "Purged $PURGED_COUNT subscriptions during concurrent operations"

if [ -n "$PURGED_COUNT" ] && [ "$PURGED_COUNT" -ge 3 ]; then
    echo "✓ PASS: Concurrent operations handled safely"
else
    echo "✗ FAIL: Concurrent operations issue"
    cat "$PURGE_CONCURRENT_OUTPUT"
    exit 1
fi

# Kill any remaining test 4 processes
for i in {1..8}; do
    if [ -f "$TEST_DIR/test4_sub$i.pid" ]; then
        PID=$(cat "$TEST_DIR/test4_sub$i.pid")
        kill "$PID" 2>/dev/null || true
    fi
done

# Test 5: Purge after network interruption simulation
echo ""
echo "--- Test 5: Purge behavior with connection issues ---"
# This test verifies that purge properly handles subscribers that may have stale connections

# Start a subscriber
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root subscribe "network-test-topic" > "$OUTPUT_DIR/test5_sub.txt" 2>&1 &
SUB_PID=$!
echo "$SUB_PID" > "$TEST_DIR/test5_sub.pid"
sleep 3

# Send SIGSTOP to simulate a frozen client (network issue)
kill -STOP "$SUB_PID" 2>/dev/null || true
echo "Simulated network freeze for subscriber"

# Try to purge
PURGE_FROZEN_OUTPUT="$OUTPUT_DIR/test5_purge.txt"
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root purge > "$PURGE_FROZEN_OUTPUT" 2>&1 || true

# Resume the process
kill -CONT "$SUB_PID" 2>/dev/null || true

# The process should now exit due to closed connection
sleep 2

if ! ps -p "$SUB_PID" > /dev/null 2>&1 && grep -q "Successfully purged 1 event subscription" "$PURGE_FROZEN_OUTPUT"; then
    echo "✓ PASS: Handled frozen client correctly"
else
    echo "✗ FAIL: Did not handle frozen client properly"
    cat "$PURGE_FROZEN_OUTPUT"
    exit 1
fi

# Test 6: Rate limiting behavior
echo ""
echo "--- Test 6: Rate limiting on purge endpoint ---"
# Perform multiple rapid purge requests
RATE_LIMIT_HIT=false
for i in {1..20}; do
    PURGE_RL_OUTPUT="$OUTPUT_DIR/test6_purge_$i.txt"
    if ! "$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root purge > "$PURGE_RL_OUTPUT" 2>&1; then
        if grep -q "429" "$PURGE_RL_OUTPUT" || grep -q "rate limit" "$PURGE_RL_OUTPUT"; then
            RATE_LIMIT_HIT=true
            echo "Rate limit hit at request $i"
            break
        fi
    fi
done

echo "✓ INFO: Made $i rapid purge requests"
if [ "$RATE_LIMIT_HIT" = true ]; then
    echo "✓ INFO: Rate limiting is active on purge endpoint"
else
    echo "✓ INFO: Rate limiting threshold not reached in test"
fi

# Test 7: Per-node purge isolation verification
echo ""
echo "--- Test 7: Per-node purge isolation verification ---"
# This test verifies that purging from a node only affects subscriptions on THAT node
# and does NOT affect subscriptions on other nodes (correct isolation behavior)

# Get list of nodes from cluster config (assuming nodes are named node0, node1, node2, etc.)
# For this test, we'll check the first 3 nodes or use an environment variable
NODES_TO_TEST="${CLUSTER_NODES:-node0 node1 node2}"
echo "Testing across nodes: $NODES_TO_TEST"

# Create API key for this test
API_KEY_OUTPUT="$OUTPUT_DIR/test7_apikey.txt"
"$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root api add "test-key-cross-node" > "$API_KEY_OUTPUT" 2>&1
CROSS_NODE_API_KEY=$(grep "API Key:" "$API_KEY_OUTPUT" | awk '{print $3}')

if [ -z "$CROSS_NODE_API_KEY" ]; then
    echo "✗ FAIL: Could not create test API key for cross-node test"
    cat "$API_KEY_OUTPUT"
    exit 1
fi

echo "Created test API key for cross-node test: ${CROSS_NODE_API_KEY:0:20}..."

# Start subscribers on different nodes
SUBSCRIBER_PIDS=()
NODE_COUNT=0
for NODE in $NODES_TO_TEST; do
    # Check if node is accessible
    if ! "$INSIC_EXE" --config "$CONFIG_FILE" --target "$NODE" --root ping > /dev/null 2>&1; then
        echo "⚠ WARNING: Node $NODE not accessible, skipping"
        continue
    fi
    
    NODE_COUNT=$((NODE_COUNT + 1))
    
    # Start 2 subscribers per node with the test API key
    for i in 1 2; do
        SUB_OUTPUT="$OUTPUT_DIR/test7_${NODE}_sub${i}.txt"
        INSI_API_KEY="$CROSS_NODE_API_KEY" "$INSIC_EXE" --config "$CONFIG_FILE" --target "$NODE" subscribe "cross-node-topic-$i" > "$SUB_OUTPUT" 2>&1 &
        PID=$!
        echo "$PID" > "$TEST_DIR/test7_${NODE}_sub${i}.pid"
        SUBSCRIBER_PIDS+=($PID)
        echo "Started subscriber on $NODE with PID $PID"
    done
done

if [ "$NODE_COUNT" -eq 0 ]; then
    echo "⚠ WARNING: No nodes accessible for cross-node test, skipping"
    # Clean up API key
    "$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root api delete "$CROSS_NODE_API_KEY" > /dev/null 2>&1 || true
else
    EXPECTED_SUBS=$((NODE_COUNT * 2))
    echo "Started $EXPECTED_SUBS subscribers across $NODE_COUNT nodes"
    
    # Wait for all subscribers to connect
    sleep 5
    
    # Count how many are actually running
    RUNNING_COUNT=0
    for PID in "${SUBSCRIBER_PIDS[@]}"; do
        if ps -p "$PID" > /dev/null 2>&1; then
            RUNNING_COUNT=$((RUNNING_COUNT + 1))
        fi
    done
    echo "Verified $RUNNING_COUNT subscribers are running"
    
    # Create a map to track which PIDs belong to which node
    declare -A PID_TO_NODE
    declare -A NODE_TO_PIDS
    
    # Build the mapping
    i=0
    for NODE in $NODES_TO_TEST; do
        if ! "$INSIC_EXE" --config "$CONFIG_FILE" --target "$NODE" --root ping > /dev/null 2>&1; then
            continue
        fi
        NODE_TO_PIDS[$NODE]=""
        for j in 1 2; do
            if [ $i -lt ${#SUBSCRIBER_PIDS[@]} ]; then
                PID=${SUBSCRIBER_PIDS[$i]}
                PID_TO_NODE[$PID]=$NODE
                NODE_TO_PIDS[$NODE]="${NODE_TO_PIDS[$NODE]} $PID"
                i=$((i + 1))
            fi
        done
    done
    
    # Now iteratively purge from each node and verify isolation
    TOTAL_PURGED=0
    NODES_PROCESSED=0
    
    for NODE in $NODES_TO_TEST; do
        if ! "$INSIC_EXE" --config "$CONFIG_FILE" --target "$NODE" --root ping > /dev/null 2>&1; then
            continue
        fi
        
        NODES_PROCESSED=$((NODES_PROCESSED + 1))
        
        # Count how many subs are still running on THIS node before purge
        NODE_SUBS_BEFORE=0
        for PID in ${NODE_TO_PIDS[$NODE]}; do
            if ps -p "$PID" > /dev/null 2>&1; then
                NODE_SUBS_BEFORE=$((NODE_SUBS_BEFORE + 1))
            fi
        done
        
        # Count how many subs are running on OTHER nodes before purge
        OTHER_SUBS_BEFORE=0
        for OTHER_NODE in $NODES_TO_TEST; do
            if [ "$OTHER_NODE" != "$NODE" ]; then
                for PID in ${NODE_TO_PIDS[$OTHER_NODE]}; do
                    if ps -p "$PID" > /dev/null 2>&1; then
                        OTHER_SUBS_BEFORE=$((OTHER_SUBS_BEFORE + 1))
                    fi
                done
            fi
        done
        
        echo ""
        echo "=== Purging from node: $NODE ==="
        echo "  Subscribers on $NODE before purge: $NODE_SUBS_BEFORE"
        echo "  Subscribers on other nodes before purge: $OTHER_SUBS_BEFORE"
        
        # Purge from this node
        PURGE_OUTPUT="$OUTPUT_DIR/test7_purge_${NODE}.txt"
        INSI_API_KEY="$CROSS_NODE_API_KEY" "$INSIC_EXE" --config "$CONFIG_FILE" --target "$NODE" purge > "$PURGE_OUTPUT" 2>&1 || true
        
        # Extract purged count
        NODE_PURGED=$(grep -oE "Successfully purged ([0-9]+) event subscription" "$PURGE_OUTPUT" | grep -oE "[0-9]+" | head -1)
        if [ -z "$NODE_PURGED" ]; then
            NODE_PURGED=0
        fi
        
        echo "  Purge reported: $NODE_PURGED subscription(s) disconnected"
        TOTAL_PURGED=$((TOTAL_PURGED + NODE_PURGED))
        
        # Wait for disconnections to complete
        sleep 2
        
        # Verify that ONLY this node's subscriptions were purged
        NODE_SUBS_AFTER=0
        for PID in ${NODE_TO_PIDS[$NODE]}; do
            if ps -p "$PID" > /dev/null 2>&1; then
                NODE_SUBS_AFTER=$((NODE_SUBS_AFTER + 1))
            fi
        done
        
        OTHER_SUBS_AFTER=0
        for OTHER_NODE in $NODES_TO_TEST; do
            if [ "$OTHER_NODE" != "$NODE" ]; then
                for PID in ${NODE_TO_PIDS[$OTHER_NODE]}; do
                    if ps -p "$PID" > /dev/null 2>&1; then
                        OTHER_SUBS_AFTER=$((OTHER_SUBS_AFTER + 1))
                    fi
                done
            fi
        done
        
        echo "  Subscribers on $NODE after purge: $NODE_SUBS_AFTER (expected: 0)"
        echo "  Subscribers on other nodes after purge: $OTHER_SUBS_AFTER (expected: $OTHER_SUBS_BEFORE)"
        
        # Verify correct behavior
        if [ "$NODE_SUBS_AFTER" -ne 0 ]; then
            echo "  ✗ FAIL: Not all subscriptions on $NODE were purged!"
            exit 1
        fi
        
        if [ "$OTHER_SUBS_AFTER" -ne "$OTHER_SUBS_BEFORE" ]; then
            echo "  ✗ FAIL: Purge on $NODE affected other nodes! (isolation violation)"
            echo "    Other nodes had $OTHER_SUBS_BEFORE before, but $OTHER_SUBS_AFTER after"
            exit 1
        fi
        
        if [ "$NODE_PURGED" -ne "$NODE_SUBS_BEFORE" ]; then
            echo "  ✗ FAIL: Purge count mismatch! Expected $NODE_SUBS_BEFORE but reported $NODE_PURGED"
            exit 1
        fi
        
        echo "  ✓ PASS: Node $NODE correctly purged only its local subscriptions"
    done
    
    # Final cleanup - kill any remaining processes
    for PID in "${SUBSCRIBER_PIDS[@]}"; do
        if ps -p "$PID" > /dev/null 2>&1; then
            kill -9 "$PID" 2>/dev/null || true
        fi
    done
    
    # Clean up test API key
    "$INSIC_EXE" --config "$CONFIG_FILE" --target "$TARGET_NODE" --root api delete "$CROSS_NODE_API_KEY" > /dev/null 2>&1 || true
    
    echo ""
    echo "--- Per-Node Purge Isolation Test Summary ---"
    echo "- Nodes tested: $NODES_PROCESSED"
    echo "- Total subscribers started: $RUNNING_COUNT"
    echo "- Total purged: $TOTAL_PURGED"
    echo "✓ PASS: Per-node purge isolation working correctly!"
    echo "  Each node only purged its local subscriptions without affecting other nodes"
fi

echo ""
echo "=== All Advanced Purge Tests Completed ==="
echo "✓ All tests passed!"

exit 0 