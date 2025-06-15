// This script tests if the disk usage limit is correctly enforced.
// It should complete successfully by catching the expected error.
function main() {
    var limitsResponse = admin.getLimits();
    if (!limitsResponse || !limitsResponse.api_key) {
        throw new Error("Failed to get current API key to run disk limit test.");
    }
    var currentApiKey = limitsResponse.api_key;
    var originalLimits = limitsResponse.max_limits;

    console.log("Temporarily setting disk limit to 1 byte for key: " + currentApiKey);

    try {
        admin.setLimits(currentApiKey, { 
            bytes_on_disk: 1,
            bytes_in_memory: 1,
            events_emitted: 1,
            subscribers: 1
        });
        time.sleep(1000);

        try {
            vs.set("disk-limit-test", "value-that-is-too-long");
            test.Aww("Disk limit was not enforced. The 'set' operation succeeded unexpectedly.");
            return 1; // Failure
        } catch (e) {
            if (e.name === "DiskLimitError") {
                test.Yay("Correctly caught expected DiskLimitError: " + e.message);
                return 0; // Success
            } else {
                test.Aww("Caught an unexpected error. Expected DiskLimitError, got " + e.name + ": " + e.message);
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
    test.Aww("An unexpected error occurred in the disk_limit test script: " + e);
    1;
}
