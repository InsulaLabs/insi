function testAdminFunctionality() {
    console.log("--- Running Admin Functionality Test ---");

    var newKeyName = "js-test-key-" + time.stamp();
    var newApiKey = null;

    try {
        // 1. Get Limits for the current (root) key
        console.log("1. Getting limits for the current key...");
        var rootLimits = admin.getLimits();
        if (!rootLimits || !rootLimits.max_limits) {
            throw new Error("Failed to get limits for the root key. Response: " + JSON.stringify(rootLimits));
        }
        test.Yay("Successfully retrieved limits for the root key.");

        // 2. Create a new API key
        console.log("\n2. Creating a new API key: " + newKeyName);
        var createResponse = admin.createKey(newKeyName);
        if (!createResponse || !createResponse.key) {
            throw new Error("Failed to create a new API key. Response: " + JSON.stringify(createResponse));
        }
        newApiKey = createResponse.key;
        test.Yay("Successfully created new API key: " + newKeyName);

        time.sleep(1000); // Allow for propagation

        // 3. Get initial limits for the new key
        console.log("\n3. Getting initial limits for new key...");
        var initialLimits = admin.getLimitsForKey(newApiKey);
        if (initialLimits.usage.bytes_on_disk !== 0) {
            throw new Error("Initial disk usage should be 0, but was " + initialLimits.usage.bytes_on_disk);
        }
        if (initialLimits.usage.bytes_in_memory !== 0) {
            throw new Error("Initial memory usage should be 0, but was " + initialLimits.usage.bytes_in_memory);
        }
        test.Yay("Initial limits for new key are correct (zero usage).");

        // 4. Set new limits for the key
        console.log("\n4. Setting new limits for the key...");
        var newLimits = {
            bytes_on_disk: 1024 * 1024,
            bytes_in_memory: 512 * 1024,
            events_emitted: 100,
            subscribers: 5
        };
        admin.setLimits(newApiKey, newLimits);
        test.Yay("Set new limits for the key.");

        time.sleep(1000); // Allow for propagation

        // 5. Verify the new limits
        console.log("\n5. Verifying the new limits...");
        var updatedLimits = admin.getLimitsForKey(newApiKey);
        if (updatedLimits.max_limits.bytes_on_disk !== newLimits.bytes_on_disk) {
            throw new Error("bytes_on_disk limit mismatch. Expected " + newLimits.bytes_on_disk + ", got " + updatedLimits.max_limits.bytes_on_disk);
        }
        if (updatedLimits.max_limits.bytes_in_memory !== newLimits.bytes_in_memory) {
            throw new Error("bytes_in_memory limit mismatch. Expected " + newLimits.bytes_in_memory + ", got " + updatedLimits.max_limits.bytes_in_memory);
        }
        if (updatedLimits.max_limits.events_emitted !== newLimits.events_emitted) {
            throw new Error("events_emitted limit mismatch. Expected " + newLimits.events_emitted + ", got " + updatedLimits.max_limits.events_emitted);
        }
        if (updatedLimits.max_limits.subscribers !== newLimits.subscribers) {
            throw new Error("subscribers limit mismatch. Expected " + newLimits.subscribers + ", got " + updatedLimits.max_limits.subscribers);
        }
        test.Yay("Successfully verified all new limits.");

        // 6. Test error case: get limits for a non-existent key
        console.log("\n6. Testing error handling for a deleted key...");
        var tempKeyName = "temp-delete-key-" + time.stamp();
        var tempKeyResponse = admin.createKey(tempKeyName);
        time.sleep(500);
        admin.deleteKey(tempKeyResponse.key);
        console.log("Created and deleted temporary key: " + tempKeyName);
        time.sleep(1000); // Allow for propagation

        var errorCaught = false;
        try {
            admin.getLimitsForKey(tempKeyResponse.key);
        } catch (e) {
            if (e.message.indexOf("api key not found") !== -1) {
                test.Yay("Correctly caught expected error for getting limits of a deleted key.");
                errorCaught = true;
            } else {
                throw new Error("Caught an error, but it was not the expected 'api key not found' error. Got: " + e.message);
            }
        }
        if (!errorCaught) {
            throw new Error("Did not catch an error when getting limits for a deleted key.");
        }

    } finally {
        // Cleanup
        if (newApiKey) {
            try {
                console.log("\n--- Cleanup ---");
                console.log("Deleting key: " + newKeyName);
                admin.deleteKey(newApiKey);
                test.Yay("Cleanup successful. Deleted key " + newKeyName);
            } catch (e) {
                test.Aww("Cleanup failed for key " + newKeyName + ": " + e.message);
                // Don't rethrow from finally, but log it's bad.
            }
        }
    }
}

try {
    testAdminFunctionality();
    0; // success
} catch (e) {
    test.Aww("Admin test failed: " + e.message);
    1; // failure
}
