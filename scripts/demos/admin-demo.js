// Example script for demonstrating the Admin object functionality in OVM.
// This script assumes it is being run with a root-level API key.

console.log("--- Admin Object Demo ---");

var newKeyName = "js-demo-key-" + time.stamp();
var newApiKey = null; // Will hold the created key: "insi_..."

try {
    // 1. Get Limits for the current (root) key
    console.log("1. Getting limits for the current key (root)...");
    var rootLimits = admin.getLimits();
    console.log("Root limits: " + JSON.stringify(rootLimits));
    if (!rootLimits || !rootLimits.max_limits) {
        throw new Error("Failed to get limits for the root key.");
    }

    // 2. Create a new API key
    console.log("\n2. Creating a new API key named '" + newKeyName + "'...");
    var createResponse = admin.createKey(newKeyName);
    console.log("Create response: " + JSON.stringify(createResponse));
    if (!createResponse || !createResponse.key) {
        throw new Error("Failed to create a new API key.");
    }
    newApiKey = createResponse.key;

    // Give the cluster a moment to sync the new key
    time.sleep(1000);

    // 3. Get the initial (default) limits for the new key
    console.log("\n3. Getting initial limits for new key '" + newKeyName + "'...");
    var initialLimits = admin.getLimitsForKey(newApiKey);
    console.log("Initial limits for new key: " + JSON.stringify(initialLimits));
    if (initialLimits.usage.bytes_on_disk !== 0) {
        throw new Error("Initial disk usage should be 0, but was " + initialLimits.usage.bytes_on_disk);
    }

    // 4. Set new limits for the created key
    console.log("\n4. Setting new limits for key '" + newKeyName + "'...");
    var newLimits = {
        bytes_on_disk: 1000000000, // 1 GB
        bytes_in_memory: 500000000,  // 500 MB
        events_emitted: 1000,        // Max total events
        subscribers: 10
    };
    admin.setLimits(newApiKey, newLimits);
    console.log("Set limits operation complete.");

    time.sleep(1000);

    // 5. Verify the new limits have been applied
    console.log("\n5. Verifying new limits for key '" + newKeyName + "'...");
    var updatedLimits = admin.getLimitsForKey(newApiKey);
    console.log("Updated limits: " + JSON.stringify(updatedLimits));
    if (updatedLimits.max_limits.bytes_on_disk !== newLimits.bytes_on_disk) {
        throw new Error("Disk limit was not updated correctly!");
    }
    if (updatedLimits.max_limits.events_emitted !== newLimits.events_emitted) {
        throw new Error("Events limit was not updated correctly!");
    }

    // 6. Test error handling by getting limits for a non-existent key
    console.log("\n6. Testing error handling for a deleted key...");
    var tempKeyName = "temp-delete-test-" + time.stamp();
    var tempKey = admin.createKey(tempKeyName).key;
    console.log("Created temporary key: " + tempKeyName);
    time.sleep(500);
    admin.deleteKey(tempKey);
    console.log("Deleted temporary key.");
    time.sleep(1000); // Allow time for deletion to propagate
    
    var errorCaught = false;
    try {
        console.log("Attempting to get limits for the deleted key (this should fail)...");
        admin.getLimitsForKey(tempKey);
    } catch (e) {
        console.log("Successfully caught expected error: " + e.message);
        if (e.message.indexOf("api key not found") === -1) {
            throw new Error("Caught an error, but it was not the expected 'api key not found' error. Got: " + e.message);
        }
        errorCaught = true;
    }

    if (!errorCaught) {
        throw new Error("Did not catch an error when getting limits for a deleted key.");
    }

} catch (e) {
    console.error("\n--- SCRIPT FAILED ---");
    console.error("Error: " + e.message);
    // Re-throwing the error will ensure the OVM runner reports a failure.
    throw e;
} finally {
    // Cleanup: Ensure the key we created is deleted
    console.log("\n--- Cleanup ---");
    if (newApiKey) {
        try {
            console.log("Deleting key '" + newKeyName + "'...");
            admin.deleteKey(newApiKey);
            console.log("Cleanup complete.");
        } catch (e) {
            console.error("Cleanup failed: " + e.message);
        }
    } else {
        console.log("No key to clean up.");
    }
}

console.log("\n--- Admin Object Demo Complete ---");
