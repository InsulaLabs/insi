// This script tests if the memory usage limit is correctly enforced.
// It should complete successfully by catching the expected error.
function main() {
    var limitsResponse = admin.getLimits();
    if (!limitsResponse || !limitsResponse.api_key) {
        throw new Error("Failed to get current API key to run memory limit test.");
    }
    var currentApiKey = limitsResponse.api_key;
    var originalLimits = limitsResponse.max_limits;

    console.log("Temporarily setting memory limit to 1 byte for key: " + currentApiKey);

    try {
        admin.setLimits(currentApiKey, { bytes_in_memory: 1 });
        time.sleep(1000);

        try {
            cache.set("memory-limit-test", "value-that-is-too-long");
            test.Aww("Memory limit was not enforced. The 'set' operation succeeded unexpectedly.");
            return 1; // Failure
        } catch (e) {
            if (e.name === "MemoryLimitError") {
                test.Yay("Correctly caught expected MemoryLimitError: " + e.message);
                return 0; // Success
            } else {
                test.Aww("Caught an unexpected error. Expected MemoryLimitError, got " + e.name + ": " + e.message);
                return 1; // Failure
            }
        }
    } finally {
        // CRITICAL: Restore the original limits.
        console.log("Restoring original limits for key: " + currentApiKey);
        admin.setLimits(currentApiKey, originalLimits);
        test.Yay("Restored original limits.");
    }
}

try {
    var result = main();
    result;
} catch (e) {
    test.Aww("An unexpected error occurred in the memory_limit test script: " + e);
    1;
}
