function getLogger(what) {
    return function(msg) {
        console.log(what + ": " + msg);
    }
}

var mainLogger = getLogger("main");

function cache_stress(n) {
    var log = getLogger("cache_stress");
    log("Starting cache stress test for " + n + " keys.");

    var keys = [];
    var prefix = "test.stress.cache." + time.stamp() + "/";
    for (var i = 0; i < n; i++) {
        keys.push(prefix + i);
    }
    test.Yay("Created " + keys.length + " keys locally.");

    try {
        // 1. SET phase
        log("Setting " + n + " keys...");
        admin.insight.resetMetrics();
        var set_time_sum = 0;
        for (var i = 0; i < n; i++) {
            try {
                var set_start = time.stamp();
                cache.set(keys[i], "initial");
                var set_time = time.stamp() - set_start;
                set_time_sum += set_time;
                if (i > 0 && i % 10 == 0) {
                    log("Set " + i + " keys");
                }
            } catch (e) {
                log("Error setting key " + keys[i] + ": " + e);
            }
        }
        test.Yay("[SET] Average time per set: " + (set_time_sum / n) + "ms");
        var metrics = admin.insight.getMetrics();
        test.Yay("[SET] OVM metrics retrieved. Retries detected: " + metrics.rate_limit_retries + ". Redirects: " + metrics.accumulated_redirects);

        // 2. CAS phase
        log("CASing " + n + " keys...");
        admin.insight.resetMetrics();
        var cas_time_sum = 0;
        for (var i = 0; i < n; i++) {
            try {
                var cas_start = time.stamp();
                var success = cache.cas(keys[i], "initial", "updated");
                var cas_time = time.stamp() - cas_start;
                cas_time_sum += cas_time;
                if (!success) {
                    log("CAS failed for key " + keys[i]);
                }
                 if (i > 0 && i % 10 == 0) {
                    log("CASed " + i + " keys");
                }
            } catch (e) {
                log("Error CASing key " + keys[i] + ": " + e);
            }
        }
        test.Yay("[CAS] Average time per CAS: " + (cas_time_sum / n) + "ms");
        metrics = admin.insight.getMetrics();
        test.Yay("[CAS] OVM metrics retrieved. Retries detected: " + metrics.rate_limit_retries + ". Redirects: " + metrics.accumulated_redirects);
        
        time.sleep(500); // Allow for potential replication delay before verification

        // 3. GET (verify) phase
        log("Getting and verifying " + n + " keys...");
        var get_time_sum = 0;
        var failed_verifications = 0;
        for (var i = 0; i < n; i++) {
            var get_start = time.stamp();
            var value = cache.get(keys[i]);
            var get_time = time.stamp() - get_start;
            get_time_sum += get_time;
            if (value !== "updated") {
                log("Verification failed for key " + keys[i] + ". Expected 'updated', got '" + value + "'");
                failed_verifications++;
            }
        }
        if (failed_verifications > 0) {
            var errMsg = "Found " + failed_verifications + " incorrect values during verification.";
            test.Aww(errMsg);
            throw new Error(errMsg);
        }
        test.Yay("[GET] Verified all keys are updated. Average time per get: " + (get_time_sum / n) + "ms");

        // 4. ITERATE phase
        log("Iterating keys with prefix: " + prefix);
        var iterate_start = time.stamp();
        var iteratedKeys = cache.iterateByPrefix(prefix, 0, n + 10); // ask for more to ensure we get all
        var iterate_time = time.stamp() - iterate_start;
        if (iteratedKeys.length !== n) {
            var errMsg = "iterateByPrefix failed. Expected " + n + " keys, got " + iteratedKeys.length;
            test.Aww(errMsg);
            throw new Error(errMsg);
        }
        test.Yay("[ITERATE] Successfully listed all " + n + " keys in " + iterate_time + "ms.");

        // 5. DELETE phase
        log("Deleting " + n + " keys...");
        admin.insight.resetMetrics();
        var delete_time_sum = 0;
        for (var i = 0; i < n; i++) {
            try {
                var del_start = time.stamp();
                cache.delete(keys[i]);
                var del_time = time.stamp() - del_start;
                delete_time_sum += del_time;
            } catch (e) {
                log("Error deleting key " + keys[i] + ": " + e);
            }
        }
        test.Yay("[DELETE] Average time per delete: " + (delete_time_sum / n) + "ms");

        time.sleep(500); // Allow for potential replication delay for deletes

        // 6. Verify Deletion
        log("Verifying deletion of " + n + " keys...");
        var not_deleted_count = 0;
        for (var i = 0; i < n; i++) {
            if (cache.get(keys[i]) !== null) {
                not_deleted_count++;
            }
        }
        if (not_deleted_count > 0) {
             var errMsg = not_deleted_count + " keys were not properly deleted.";
             test.Aww(errMsg);
             throw new Error(errMsg);
        }
        test.Yay("Verified all keys were deleted.");
        
        var remainingKeys = cache.iterateByPrefix(prefix, 0, n);
        if (remainingKeys.length !== 0) {
             var errMsg = "iterateByPrefix found " + remainingKeys.length + " keys after deletion.";
             test.Aww(errMsg);
             throw new Error(errMsg);
        }
        test.Yay("Verified iteration shows 0 keys after deletion.");


    } catch (e) {
        test.Aww("Cache stress test failed: " + e);
        throw e;
    } finally {
        log("Test cleanup: ensuring all keys are deleted.");
        for (var i = 0; i < n; i++) {
            cache.delete(keys[i]);
        }
    }
}

mainLogger("Starting cache stress test");
var start = time.stamp();
cache_stress(1000);
var end = time.stamp();
mainLogger("Time taken: " + (end - start) + "ms");
mainLogger("Done");
