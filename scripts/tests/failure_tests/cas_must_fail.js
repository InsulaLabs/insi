// This script is designed to fail.
function main() {
    var key = "test-key-cas-fail-" + time.stamp();
    vs.set(key, "initial_value");

    var success = vs.cas(key, "wrong_old_value", "new_value");
    if (success) {
        // This should not happen.
        console.error("cas returned true unexpectedly for key: " + key);
        return 1;
    }

    // cas returned false, which is correct.
    // Throw to signal failure to runExpectFailure.
    throw new Error("cas correctly prevented overwrite with wrong old value.");
}

try {
    main();
} catch(e) {
    throw e;
} 