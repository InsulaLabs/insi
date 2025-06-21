cwd=$(pwd)
rm -rf /tmp/insi-test-cluster
mkdir -p /tmp/insi-test-cluster
cp ../build/insid /tmp/insi-test-cluster/insid
cp ../build/insic /tmp/insi-test-cluster/insic
cp test-cluster.yaml /tmp/insi-test-cluster/cluster.yaml
cp tkv-event-tests/* /tmp/insi-test-cluster/
cp tkv-data-tests/* /tmp/insi-test-cluster/
cp tkv-cache-tests/* /tmp/insi-test-cluster/
cd /tmp/insi-test-cluster

function start_insid() {
    ./insid --host --config cluster.yaml &
    insid_pid=$!
    echo "insid pid: $insid_pid"
    sleep 15
}

function stop_insid() {
    kill $insid_pid

    # clean up data dir for next test
    rm -rf /tmp/insi-test-cluster/data
}



function run_test_script() {

    start_insid
    $1 /tmp/insi-test-cluster/insic
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "❌ Error: Test script $1 failed with exit code $exit_code"
        stop_insid # Ensure cleanup even on failure
        cd $cwd # Return to original directory
        exit $exit_code
    fi
    stop_insid
}

echo "🚀 Running insight.sh..."
run_test_script /tmp/insi-test-cluster/insight.sh
echo "✅ Success: insight.sh completed."

echo "🚀 Running metrics.sh..."
run_test_script /tmp/insi-test-cluster/metrics.sh
echo "✅ Success: metrics.sh completed."

echo "🚀 Running api-keys.sh..."
run_test_script /tmp/insi-test-cluster/api-keys.sh
echo "✅ Success: api-keys.sh completed."

echo "🚀 Running crud-iter.sh..."
run_test_script /tmp/insi-test-cluster/crud-iter.sh
echo "✅ Success: crud-iter.sh completed."

echo "🚀 Running cas-setnx.sh..."
run_test_script /tmp/insi-test-cluster/cas-setnx.sh
echo "✅ Success: cas-setnx.sh completed."

echo "🚀 Running events.sh..."
run_test_script /tmp/insi-test-cluster/events.sh
echo "✅ Success: events.sh completed."

echo "🚀 Running get-set-delete.sh..."
run_test_script /tmp/insi-test-cluster/get-set-delete.sh
echo "✅ Success: get-set-delete.sh completed."

echo "🚀 Running cache-cas-setnx.sh..."
run_test_script /tmp/insi-test-cluster/cache-cas-setnx.sh
echo "✅ Success: cache-cas-setnx.sh completed."

echo "🚀 Running blob.sh..."
run_test_script /tmp/insi-test-cluster/blob.sh
echo "✅ Success: blob.sh completed."


sleep 5 # wait for insid to finish stopping and logging out

cd $cwd

echo "🎉 All tests passed successfully!"

