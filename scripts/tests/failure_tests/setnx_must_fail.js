// This script is designed to fail.
// It sets a key and then tries to set it again with setnx.
// setnx should return false, and we will throw an error to signal
// to the test runner that the operation failed as expected.
function main() {
    var key = "test-key-setnx-fail-" + time.stamp();
    vs.set(key, "initial_value");

    var success = vs.setnx(key, "new_value");
    if (success) {
        // This should not happen. If it does, the underlying logic is wrong.
        console.error("setnx returned true unexpectedly for key: " + key);
        return 1; // Return non-zero for failure
    }

    // setnx returned false, which is correct.
    // Now, throw an error so runExpectFailure can catch it and pass the test.
    throw new Error("setnx correctly prevented overwrite.");
}

try {
    main();
} catch(e) {
    // Re-throw to ensure the script execution fails.
    throw e;
} 