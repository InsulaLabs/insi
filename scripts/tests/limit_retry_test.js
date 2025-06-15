/*
    BOSLEY: June 14, 2025
    
    This test ensures that the OVM runtime that heavily relies on the insi client can handle correctly
    the occurance of hitting a rate limiter.

    We test with a system command as it has an extremly low rate limit relative to other endpoint categories

    We then pull the insight.metrics (available only in admin object so we need to be root) and verify
    that the vm did in fact cycle its retry logic.
*/

console.log("running rate limit test");

function testRateLimit() {
    test.Yay("Starting rate limit test using a system command. This may take a moment...");
    var iterations = 30; // Should be enough to trigger 'system' rate limits (limit: 10, burst: 15).

    var startTime = time.stamp();

    for (var i = 0; i < iterations; i++) {
        try {
            // Use a system-level command which has a much lower rate limit.
            admin.getLimits();
            console.log("admin.getLimits() iteration " + (i + 1) + "/" + iterations);
        } catch (e) {
            var errMsg = "admin.getLimits() failed unexpectedly during rate limit test at iteration " + i + ": " + e.message;
            test.Aww(errMsg);
            throw new Error(errMsg);
        }
    }

    var endTime = time.stamp();
    var duration = endTime - startTime;

    test.Yay("Finished " + iterations + " getLimits operations in " + duration + "ms.");

    // The real test is that the script didn't crash. The OVM's retry logic
    // should have handled any 429s by sleeping and retrying.
    // A long duration is evidence of this. Now we verify retries happened.

    var metrics = admin.insight.getMetrics();
    if (!metrics || typeof metrics.rate_limit_retries === 'undefined') {
        var errMsg = "Failed to get metrics from admin.insight";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    test.Yay("OVM metrics retrieved. Retries detected: " + metrics.rate_limit_retries);

    if (metrics.rate_limit_retries === 0) {
        // This should be considered a failure of the test's premise.
        // If we didn't hit the rate limiter, we didn't test the retry logic.
        var errMsg = "Test did not trigger any rate limit retries. Increase iterations or check server rate limit configuration.";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    test.Yay("Rate limit test passed! The OVM successfully handled all operations without crashing.");
}


try {
    testRateLimit();
    0; // success
} catch (e) {
    // The error is already reported by test.Aww in the function.
    1; // failure
}
