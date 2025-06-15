// This script tests if the event emission limit is correctly enforced.
// It should complete successfully by catching the expected error.
function main() {
    var limitsResponse = admin.getLimits();
    if (!limitsResponse || !limitsResponse.api_key) {
        throw new Error("Failed to get current API key to run event limit test.");
    }
    var currentApiKey = limitsResponse.api_key;
    var originalLimits = limitsResponse.max_limits;
    var currentUsage = limitsResponse.usage.events_emitted;

    console.log("Temporarily setting event limit to " + currentUsage + " for key: " + currentApiKey);

    try {
        admin.setLimits(currentApiKey, { events_emitted: currentUsage });
        time.sleep(1000);

        try {
            emit("event-limit-test", { "foo": "bar" });
            test.Aww("Event limit was not enforced. The 'emit' operation succeeded unexpectedly.");
            return 1; // Failure
        } catch (e) {
            if (e.name === "EventsLimitError") {
                test.Yay("Correctly caught expected EventsLimitError: " + e.message);
                return 0; // Success
            } else {
                test.Aww("Caught an unexpected error. Expected EventsLimitError, got " + e.name + ": " + e.message);
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
    test.Aww("An unexpected error occurred in the event_limit test script: " + e);
    1;
}
