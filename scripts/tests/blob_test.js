function testSetGetDelete() {
    console.log("running blob testSetGetDelete");
    var key = "blob_test_key_1_" + time.stamp();
    var value = "this is some blob data";

    // Test set
    blob.set(key, value);

    // Poll for the key to appear
    var retrievedValue;
    var attempts = 0;
    var maxAttempts = 10;
    var success = false;

    while (attempts < maxAttempts) {
        retrievedValue = blob.get(key);
        if (retrievedValue === value) {
            success = true;
            break;
        }
        time.sleep(300); // Wait for replication
        attempts++;
    }

    if (!success) {
        var errMsg = "blob.get after set failed: expected '" + value + "', got '" + retrievedValue + "'";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    // Test delete
    blob.delete(key);

    // Poll for the key to disappear
    attempts = 0;
    success = false;
    while (attempts < maxAttempts) {
        retrievedValue = blob.get(key);
        if (retrievedValue === null) {
            success = true;
            break;
        }
        time.sleep(300);
        attempts++;
    }

    if (!success) {
        var errMsg = "blob.delete failed: key should be null, but got " + retrievedValue;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    // Test deleting a non-existent key (should not throw)
    try {
        blob.delete(key);
    } catch (e) {
        var errMsg = "blob.delete on a non-existent key should not throw an error, but it did: " + e.message;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    test.Yay("testSetGetDelete: PASS");
}

function testIterate() {
    console.log("running blob testIterate");
    var prefix = "blob_test_iterate/" + time.stamp() + "/";
    blob.set(prefix + "1", "blob1");
    blob.set(prefix + "2", "blob2");
    blob.set(prefix + "3", "blob3");

    var keys;
    var attempts = 0;
    var maxAttempts = 10;
    var success = false;

    while (attempts < maxAttempts) {
        time.sleep(300); // Wait for replication
        keys = blob.iterateByPrefix(prefix, 0, 10);
        if (keys.length === 3) {
            success = true;
            break;
        }
        attempts++;
        console.log("Retrying blob iteration, attempt " + attempts + " found " + keys.length + " keys");
    }

    if (!success) {
        var errMsg = "blob.iterateByPrefix failed after " + maxAttempts + " attempts: expected 3 keys, got " + keys.length;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    keys.sort();

    if (keys[0] !== prefix + "1" || keys[1] !== prefix + "2" || keys[2] !== prefix + "3") {
        var errMsg = "blob.iterateByPrefix returned wrong keys: " + keys;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    // Clean up
    blob.delete(prefix + "1");
    blob.delete(prefix + "2");
    blob.delete(prefix + "3");

    test.Yay("testIterate: PASS");
}


try {
    testSetGetDelete();
    testIterate();
    0; // success
} catch (e) {
    console.log("blob_test.js failed: " + e);
    // The error is already reported by test.Aww in the functions.
    // We just need to bubble up the failure.
    1; // failure
}
