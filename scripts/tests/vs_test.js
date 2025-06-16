function testSetGetDelete() {
    console.log("running testSetGetDelete");
    var key = "vs_test_key_1";
    var value = "vs_test_value_1";

    // Clean up before test
    vs.delete(key);
    time.sleep(200);

    // Test set
    vs.set(key, value);
    time.sleep(200);

    // Test get
    var retrievedValue = vs.get(key);
    if (retrievedValue !== value) {
        var errMsg = "vs.get failed: expected " + value + ", got " + retrievedValue;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    // Test delete
    vs.delete(key);
    time.sleep(200);
    retrievedValue = vs.get(key);
    if (retrievedValue !== null) {
        var errMsg = "vs.delete failed: key should be null, but got " + retrievedValue;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    test.Yay("testSetGetDelete: PASS");
}

function testSetNX() {
    console.log("running testSetNX");
    var key = "vs_test_key_nx";
    var value1 = "value1";
    var value2 = "value2";

    // Clean up
    vs.delete(key);
    time.sleep(200);

    var result = vs.setnx(key, value1);
    if (result !== true) {
        var errMsg = "vs.setnx failed: expected true on first set";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    time.sleep(200);

    var retrieved = vs.get(key);
    if (retrieved !== value1) {
        var errMsg = "vs.get after setnx failed: expected " + value1 + " got " + retrieved;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    result = vs.setnx(key, value2);
    if (result !== false) {
        var errMsg = "vs.setnx failed: expected false on second set";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    time.sleep(200);

    retrieved = vs.get(key);
    if (retrieved !== value1) {
        var errMsg = "vs.get after second setnx failed: expected " + value1 + " got " + retrieved;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    vs.delete(key);
    time.sleep(200);
    test.Yay("testSetNX: PASS");
}


function testCAS() {
    console.log("running testCAS");
    var key = "vs_test_key_cas";
    var value1 = "cas_value1";
    var value2 = "cas_value2";

    vs.delete(key);
    time.sleep(200);
    vs.set(key, value1);
    time.sleep(200);

    var result = vs.cas(key, "wrong_old_value", value2);
    if (result !== false) {
        var errMsg = "cas should have failed due to wrong old value";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    result = vs.cas(key, value1, value2);
    if (result !== true) {
        var errMsg = "cas should have succeeded";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    time.sleep(200);

    var retrieved = vs.get(key);
    if (retrieved !== value2) {
        var errMsg = "get after cas failed: expected " + value2 + " got " + retrieved;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    vs.delete(key);
    time.sleep(200);
    test.Yay("testCAS: PASS");
}


function testIterate() {
    console.log("running testIterate");
    var prefix = "vs_test_iterate/";
    vs.set(prefix + "1", "1");
    vs.set(prefix + "2", "2");
    vs.set(prefix + "3", "3");
    time.sleep(200);

    var keys = vs.iterateByPrefix(prefix, 0, 10);
    if (keys.length !== 3) {
        var errMsg = "iterateByPrefix failed: expected 3 keys, got " + keys.length;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    keys.sort();

    if (keys[0] !== prefix+"1" || keys[1] !== prefix+"2" || keys[2] !== prefix+"3") {
        var errMsg = "iterateByPrefix returned wrong keys: " + keys;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    vs.delete(prefix + "1");
    vs.delete(prefix + "2");
    vs.delete(prefix + "3");
    time.sleep(200);
    test.Yay("testIterate: PASS");
}

function testBump() {
    console.log("running testBump");
    var key = "vs_test_key_bump";

    // Cleanup
    vs.delete(key);
    time.sleep(200);

    // Test: Set initial value, bump up, bump down
    vs.set(key, "10");
    time.sleep(200);

    vs.bump(key, 5); // 10 + 5 = 15
    time.sleep(200);
    var retrieved = vs.get(key);
    if (retrieved !== "15") {
        let errMsg = "vs.bump up failed: expected '15', got '" + retrieved + "'";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    vs.bump(key, -10); // 15 - 10 = 5
    time.sleep(200);
    retrieved = vs.get(key);
    if (retrieved !== "5") {
        let errMsg = "vs.bump down failed: expected '5', got '" + retrieved + "'";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    // Test bumping below zero (should be clamped at 0 by server)
    vs.bump(key, -10); // 5 - 10 should be 0
    time.sleep(200);
    retrieved = vs.get(key);
    if (retrieved !== "0") {
        let errMsg = "vs.bump below zero failed: expected '0', got '" + retrieved + "'";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    vs.delete(key);
    time.sleep(200);

    // Test: Bump non-existent key
    var newKey = "vs_test_key_bump_new";
    vs.delete(newKey); // ensure clean
    time.sleep(200);

    vs.bump(newKey, 100);
    time.sleep(200);
    retrieved = vs.get(newKey);
    if (retrieved !== "100") {
        let errMsg = "vs.bump on new key failed: expected '100', got '" + retrieved + "'";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    vs.delete(newKey);
    time.sleep(200);

    // Test: Bump key with non-integer value (should throw)
    var nonIntKey = "vs_test_key_bump_non_int";
    vs.set(nonIntKey, "i-am-not-a-number");
    time.sleep(200);

    var didThrow = false;
    try {
        vs.bump(nonIntKey, 5);
    } catch (e) {
        didThrow = true;
        console.log("Caught expected error for bumping non-integer value: " + e.message);
    }
    if (!didThrow) {
        let errMsg = "vs.bump on a non-integer value should have thrown an error";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    vs.delete(nonIntKey);

    test.Yay("testBump: PASS");
}


try {
    testSetGetDelete();
    testSetNX();
    testCAS();
    testIterate();
    testBump();
    0; // success
} catch (e) {
    // The error is already reported by test.Aww in the functions.
    // We just need to bubble up the failure.
    1; // failure
}
