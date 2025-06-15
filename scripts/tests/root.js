console.log("running root test script");


function main() {
    try {
        test.setDir("scripts/tests");
    } catch (e) {
        test.Aww("Failed to set test directory:", e.message);
        return 1; // return non-zero for failure
    }

    var testSuccess = [
        "limit_retry_test",
        "ovm_test",
        "vs_test",
        "cache_test",
        "events_test",
        "subscription_test",
        "limits_test",
        "admin_test",
        "access_failures_test",
        "disk_limit",
        "memory_limit",
        "event_limit"
    ];

    var runTest = function(testName) {
        console.log("running " + testName + ".js");
        test.runExpectSuccess(testName + ".js");
        console.log(testName + ".js PASSED");
    }

    for (var i = 0; i < testSuccess.length; i++) {
        try {
            runTest(testSuccess[i]);
        } catch (e) {
            test.Aww("Test " + testSuccess[i] + " failed: " + e);
            return 1; // return non-zero for failure
        }
        test.Yay("Test " + testSuccess[i] + " passed");
    }

    return 0; // return 0 for success
}

main();
