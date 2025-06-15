console.log("running access failures test suite");

function main() {
    // This test suite runs other scripts that are *expected* to fail.
    // The `test.runExpectFailure` function will pass if the sub-script
    // throws an error or returns a non-zero exit code. It will throw
    // an error if the sub-script succeeds (returns 0).

    var testsToFail = [
        "failure_tests/setnx_must_fail.js",
        "failure_tests/cas_must_fail.js",
        "failure_tests/empty_key_must_fail.js",
        "failure_tests/event_topic_must_fail.js",
    ];

    for (var i = 0; i < testsToFail.length; i++) {
        var testName = testsToFail[i];
        console.log("Running expected failure test: " + testName);
        // This call should complete without throwing.
        // If it throws, it means the sub-script *succeeded* when it should have failed.
        test.runExpectFailure(testName);
        test.Yay("Test " + testName + " failed as expected and was caught correctly.");
    }

    // --- Path Traversal Test ---
    console.log("Running path traversal failure test...");
    try {
        // This call itself should throw an exception from within the OVM Go code.
        test.runExpectFailure("../README.md");
        // If we reach here, the security check failed to throw an error.
        var errMsg = "Path traversal test did NOT fail as expected.";
        test.Aww(errMsg);
        throw new Error(errMsg);
    } catch (e) {
        // We expect an error here.
        test.Yay("Path traversal test failed as expected.");
        console.log("Path traversal correctly blocked with error: " + e.message);
    }


    console.log("All access failure tests behaved as expected.");
    return 0;
}

try {
    var result = main();
    // Return the result from main to the OVM.
    // This is important because if main returns 1, the test has failed.
    result;
} catch (e) {
    test.Aww("An unexpected error occurred in access_failures_test.js: " + e);
    1; // failure
} 