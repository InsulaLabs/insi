console.log("running cache test");

function testSetGetDelete() {
    console.log("running testSetGetDelete (cache)");
    var key = "cache_test_key_1";
    var value = "cache_test_value_1";

    // Clean up before test
    cache.delete(key);
    time.sleep(200);

    // Test set
    cache.set(key, value);
    time.sleep(200);

    // Test get
    var retrievedValue = cache.get(key);
    if (retrievedValue !== value) {
        var errMsg = "cache.get failed: expected " + value + ", got " + retrievedValue;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    // Test delete
    cache.delete(key);
    time.sleep(200);
    retrievedValue = cache.get(key);
    if (retrievedValue !== null) {
        var errMsg = "cache.delete failed: key should be null, but got " + retrievedValue;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    test.Yay("testSetGetDelete (cache): PASS");
}

function testSetNX() {
    console.log("running testSetNX (cache)");
    var key = "cache_test_key_nx";
    var value1 = "value1";
    var value2 = "value2";

    // Clean up
    cache.delete(key);
    time.sleep(200);

    var result = cache.setnx(key, value1);
    if (result !== true) {
        var errMsg = "cache.setnx failed: expected true on first set";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    time.sleep(200);

    var retrieved = cache.get(key);
    if (retrieved !== value1) {
        var errMsg = "cache.get after setnx failed: expected " + value1 + " got " + retrieved;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    result = cache.setnx(key, value2);
    if (result !== false) {
        var errMsg = "cache.setnx failed: expected false on second set";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    time.sleep(200);

    retrieved = cache.get(key);
    if (retrieved !== value1) {
        var errMsg = "cache.get after second setnx failed: expected " + value1 + " got " + retrieved;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    cache.delete(key);
    time.sleep(200);
    test.Yay("testSetNX (cache): PASS");
}


function testCAS() {
    console.log("running testCAS (cache)");
    var key = "cache_test_key_cas";
    var value1 = "cas_value1";
    var value2 = "cas_value2";

    cache.delete(key);
    time.sleep(200);
    cache.set(key, value1);
    time.sleep(200);

    var result = cache.cas(key, "wrong_old_value", value2);
    if (result !== false) {
        var errMsg = "cache.cas should have failed due to wrong old value";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    result = cache.cas(key, value1, value2);
    if (result !== true) {
        var errMsg = "cache.cas should have succeeded";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    time.sleep(200);

    var retrieved = cache.get(key);
    if (retrieved !== value2) {
        var errMsg = "get after cache.cas failed: expected " + value2 + " got " + retrieved;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    cache.delete(key);
    time.sleep(200);
    test.Yay("testCAS (cache): PASS");
}


function testIterate() {
    console.log("running testIterate (cache)");
    var prefix = "cache_test_iterate/";
    cache.set(prefix + "1", "1");
    time.sleep(200);
    cache.set(prefix + "2", "2");
    time.sleep(200);
    cache.set(prefix + "3", "3");
    time.sleep(200);

    var keys = cache.iterateByPrefix(prefix, 0, 10);
    if (keys.length !== 3) {
        var errMsg = "cache.iterateByPrefix failed: expected 3 keys, got " + keys.length;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    keys.sort();

    if (keys[0] !== prefix+"1" || keys[1] !== prefix+"2" || keys[2] !== prefix+"3") {
        var errMsg = "cache.iterateByPrefix returned wrong keys: " + keys;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    cache.delete(prefix + "1");
    time.sleep(200);
    cache.delete(prefix + "2");
    time.sleep(200);
    cache.delete(prefix + "3");
    time.sleep(200);
    test.Yay("testIterate (cache): PASS");
}


try {
    testSetGetDelete();
    testSetNX();
    testCAS();
    testIterate();
    0; // success
} catch (e) {
    // The error is already reported by test.Aww in the functions.
    // We just need to bubble up the failure.
    1; // failure
}