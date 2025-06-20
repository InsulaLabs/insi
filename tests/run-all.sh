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


echo "🚀 Running blob.sh..."
run_test_script /tmp/insi-test-cluster/blob.sh
echo "✅ Success: blob.sh completed."

sleep 5 # wait for insid to finish stopping and logging out

cd $cwd

echo "🎉 All tests passed successfully!"

