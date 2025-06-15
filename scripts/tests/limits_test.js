function testUsageTracking() {
    console.log("running testUsageTracking");
    time.sleep(1000); // Start with a sleep to ensure we're getting a settled value

    // 1. Calibration Step to find the prefix length
    console.log("Calibrating for key prefix...");
    var calibrationLimitsBefore = admin.getLimits().usage;
    var calKey = "calibration_key_vs";
    var calVal = "c";
    vs.set(calKey, calVal);
    time.sleep(1000);
    var calibrationLimitsAfter = admin.getLimits().usage;
    var actualCalIncrease = calibrationLimitsAfter.bytes_on_disk - calibrationLimitsBefore.bytes_on_disk;
    var expectedCalIncrease = calKey.length + calVal.length;
    var prefixLen = actualCalIncrease - expectedCalIncrease;
    vs.delete(calKey); // cleanup calibration key
    time.sleep(1000);
    console.log("Calibration complete. Detected prefix length: " + prefixLen);

    // 2. Baseline
    var initialLimits = admin.getLimits().usage;
    console.log("Initial usage: " + JSON.stringify(initialLimits));

    // --- 3. Value Store Test ---
    var vsKey = "limit-test-vs-" + time.stamp();
    var vsValue = "some disk value to test byte counting, this should be long enough";
    var expectedVsIncrease = vsKey.length + vsValue.length + prefixLen;

    vs.set(vsKey, vsValue);
    console.log("Set a value store key: " + vsKey);
    time.sleep(1000); // Wait for usage to update

    var limitsAfterVsSet = admin.getLimits().usage;
    console.log("Usage after VS set: " + JSON.stringify(limitsAfterVsSet));
    
    var actualVsIncrease = limitsAfterVsSet.bytes_on_disk - initialLimits.bytes_on_disk;
    if (actualVsIncrease !== expectedVsIncrease) {
        var errMsg = "vs.set bytes_on_disk tracking is not perfect. Expected increase of " + expectedVsIncrease + ", but got " + actualVsIncrease;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    test.Yay("vs.set correctly increased bytes_on_disk by exactly " + expectedVsIncrease + " bytes.");

    // --- 4. Cache Store Test ---
    var cacheKey = "limit-test-cache-" + time.stamp();
    var cacheValue = "some memory value to test byte counting in the cache store";
    // NOTE: We assume the same prefixing logic for cache as for value store.
    var expectedCacheIncrease = cacheKey.length + cacheValue.length + prefixLen; 

    cache.set(cacheKey, cacheValue);
    console.log("Set a cache store key: " + cacheKey);
    time.sleep(1000);

    var limitsAfterCacheSet = admin.getLimits().usage;
    console.log("Usage after Cache set: " + JSON.stringify(limitsAfterCacheSet));
    
    var actualCacheIncrease = limitsAfterCacheSet.bytes_in_memory - limitsAfterVsSet.bytes_in_memory;
    if (actualCacheIncrease !== expectedCacheIncrease) {
         var errMsg = "cache.set bytes_in_memory tracking is not perfect. Expected increase of " + expectedCacheIncrease + ", but got " + actualCacheIncrease;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    test.Yay("cache.set correctly increased bytes_in_memory by exactly " + expectedCacheIncrease + " bytes.");

    // --- 5. Event Test ---
    var topic = "limit-test-events-" + time.stamp();
    emit(topic, { data: "foo" });
    console.log("Emitted an event.");
    time.sleep(1000);

    var limitsAfterEmit = admin.getLimits().usage;
    console.log("Usage after emit: " + JSON.stringify(limitsAfterEmit));
    if (limitsAfterEmit.events_emitted <= limitsAfterCacheSet.events_emitted) {
         var errMsg = "events_emitted did not increase after emit. Before: " + limitsAfterCacheSet.events_emitted + ", New: " + limitsAfterEmit.events_emitted;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    test.Yay("emit correctly increased events_emitted.");

    // --- 6. Cleanup ---
    console.log("Cleaning up test keys...");
    vs.delete(vsKey);
    time.sleep(1000);

    var limitsAfterVsDelete = admin.getLimits().usage;
    console.log("Usage after VS delete: " + JSON.stringify(limitsAfterVsDelete));
    if (limitsAfterVsDelete.bytes_on_disk !== initialLimits.bytes_on_disk) {
        var errMsg = "bytes_on_disk did not decrease correctly after vs.delete. Expected: " + initialLimits.bytes_on_disk + ", Final: " + limitsAfterVsDelete.bytes_on_disk;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
    test.Yay("bytes_on_disk correctly restored after vs.delete.");

    cache.delete(cacheKey);
    time.sleep(1000);

    var finalLimits = admin.getLimits().usage;
    console.log("Final usage after cleanup: " + JSON.stringify(finalLimits));
    
    // After deleting the cache key, memory should return to the state before it was set.
    if (finalLimits.bytes_in_memory !== limitsAfterVsSet.bytes_in_memory) {
         var errMsg = "bytes_in_memory did not decrease correctly after cache.delete. Expected: " + limitsAfterVsSet.bytes_in_memory + ", Final: " + finalLimits.bytes_in_memory;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }
     test.Yay("bytes_in_memory correctly restored after cache.delete.");
}

try {
    testUsageTracking();
    0; // success
} catch (e) {
    1; // failure
}
