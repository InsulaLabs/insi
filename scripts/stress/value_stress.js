function getLogger(what) {
    return function(msg) {
        console.log(what + ": " + msg);
    }
}

var mainLogger = getLogger("main");

function run_and_get_metrics(f) {
    admin.insight.resetMetrics();
    start = time.stamp();
    f();
    var end = time.stamp();
    var metrics = admin.insight.getMetrics();
    return {
        time: end - start,
        retries: metrics.rate_limit_retries,
    }
}

function atomic_stress(n) {

    // Reset the metrics (retry count)

    var log = getLogger("atomic_stress");

    log("Starting atomic stress test");
    var keys = []
    for (var i = 0; i < n; i++) {
        keys.push("test.stress.vs." + i);
    }
    test.Yay("Created " + keys.length + " keys (local js-only operation)");

    log("Setting keys (non-atomic) and resetting insight metrics");

    admin.insight.resetMetrics();


    var set_time_sum = 0;
    for (var i = 0; i < keys.length; i++) {
        try {
            var sts_start = time.stamp();
            vs.set(keys[i], "old");
            var set_time = time.stamp() - sts_start;
            set_time_sum += set_time;
            if (i % 10 == 0 && i > 0) {
                log("Set " + i + " keys");
            }
        } catch (e) {
            log("Error pre-cas setup atomic_stress: " + e);
        }
    }

    test.Yay("[SET] Average time per set: " + (set_time_sum / keys.length) + "ms");

    var metrics = admin.insight.getMetrics();
    if (!metrics || typeof metrics.rate_limit_retries === 'undefined' || typeof metrics.accumulated_redirects === 'undefined') {
        var errMsg = "Failed to get metrics from admin.insight";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    test.Yay("[SET] OVM metrics retrieved. Retries detected: " + metrics.rate_limit_retries + ". Redirects: " + metrics.accumulated_redirects);

    log("CASing keys");

    admin.insight.resetMetrics();

    var cas_time_sum = 0;
    for (var i = 0; i < keys.length; i++) {
        try {
            var cas_start = time.stamp();
            vs.cas(keys[i], "old", "new");
            var cas_time = time.stamp() - cas_start;
            cas_time_sum += cas_time;
            if (i % 10 == 0 && i > 0) {
                log("CASed " + i + " keys");
            }
        } catch (e) {
            log("Error cas atomic_stress: " + e);
        }
    }

    test.Yay("[CAS] Average time per cas: " + (cas_time_sum / keys.length) + "ms");

    var metrics = admin.insight.getMetrics();
    if (!metrics || typeof metrics.rate_limit_retries === 'undefined' || typeof metrics.accumulated_redirects === 'undefined') {
        var errMsg = "Failed to get metrics from admin.insight";
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    test.Yay("[CAS] OVM metrics retrieved. Retries detected: " + metrics.rate_limit_retries + ". Redirects: " + metrics.accumulated_redirects);

    /*
    
            The internal insi client uses a random selector to distribute
            the load across all nodes. This means that any given read may
            be behind if attempting to immediately read the data.

            This is the nature of the distributed system.
    
    */
    log("letting data settle");
    time.sleep(1000);

    log("confirming keys");

    var get_time_sum = 0;
    for (var i = 0; i < keys.length; i++) {
        var get_start = time.stamp();
        var value = vs.get(keys[i]);
        var get_time = time.stamp() - get_start;
        get_time_sum += get_time;
        if (value != "new") {
            log("Key " + keys[i] + " has value " + value + " instead of new");
        }
        if (i % 10 == 0 && i > 0) {
            log("Confirmed " + i + " keys");
        }
    }

    test.Yay("[GET] Average time per get: " + (get_time_sum / keys.length) + "ms");

    log("Done");

    log("Total time: " + (set_time_sum + cas_time_sum + get_time_sum) + "ms");
    log("Average time per operation: " + ((set_time_sum + cas_time_sum + get_time_sum) / keys.length) + "ms");

}

mainLogger("Starting atomic stress test");

var start = time.stamp();
atomic_stress(1000);
var end = time.stamp();
mainLogger("Time taken: " + (end - start) + "ms");
mainLogger("Done");