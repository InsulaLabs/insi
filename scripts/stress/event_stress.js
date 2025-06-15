function getLogger(what) {
    return function(msg) {
        console.log(what + ": " + msg);
    }
}

var mainLogger = getLogger("main");

function event_stress(n) {
    var log = getLogger("event_stress");
    log("Starting event stress test for " + n + " events.");

    var topic = "test.stress.events." + time.stamp();

    try {
        // 1. Subscribe to the topic
        log("Subscribing to topic: " + topic);
        var sub_start = time.stamp();
        subscriptions.subscribe(topic);
        var sub_time = time.stamp() - sub_start;
        log("Subscription took " + sub_time + "ms");
        time.sleep(1000); // Give time for subscription to propagate across the cluster

        // 2. Emit N events and measure
        log("Emitting " + n + " events.");
        admin.insight.resetMetrics();
        var emit_time_sum = 0;
        for (var i = 0; i < n; i++) {
            var event = { counter: i, message: "stress test event" };
            try {
                var emit_start = time.stamp();
                emit(topic, event);
                var emit_time = time.stamp() - emit_start;
                emit_time_sum += emit_time;
                if (i > 0 && i % 10 == 0) {
                    log("Emitted " + i + " events");
                }
            } catch (e) {
                log("Error emitting event " + i + ": " + e);
            }
        }
        test.Yay("[EMIT] Average time per emit: " + (emit_time_sum / n) + "ms");
        var metrics = admin.insight.getMetrics();
        if (!metrics || typeof metrics.rate_limit_retries === 'undefined' || typeof metrics.accumulated_redirects === 'undefined') {
            var errMsg = "Failed to get metrics from admin.insight";
            test.Aww(errMsg);
            throw new Error(errMsg);
        }
        test.Yay("[EMIT] OVM metrics retrieved. Retries detected: " + metrics.rate_limit_retries + ". Redirects: " + metrics.accumulated_redirects);

        log("Waiting for events to be collected by subscription service...");
        time.sleep(1000); 

        // 3. Poll for events and measure
        log("Polling for " + n + " events.");
        var received_count = 0;
        var poll_time_sum = 0;
        var all_events = [];
        
        var poll_attempts = 0;
        var max_poll_attempts = 30; // ~15 seconds with 500ms sleep
        while (received_count < n && poll_attempts < max_poll_attempts) {
            poll_attempts++;
            var poll_start = time.stamp();
            var events = subscriptions.poll(topic, n - received_count); 
            var poll_time = time.stamp() - poll_start;
            poll_time_sum += poll_time;

            if (events && events.length > 0) {
                received_count += events.length;
                all_events = all_events.concat(events);
                log("Poll attempt " + poll_attempts + ": received " + events.length + " events. Total received: " + received_count);
            } else {
                log("Poll attempt " + poll_attempts + ": received 0 events. Total received: " + received_count);
            }

            if (received_count < n) {
                 time.sleep(500);
            }
        }

        test.Yay("[POLL] Average time per poll call: " + (poll_time_sum / poll_attempts) + "ms over " + poll_attempts + " attempts.");

        if (received_count !== n) {
             var errMsg = "Event subscription stress test failed: Expected " + n + " events, but received " + received_count;
             test.Aww(errMsg);
             throw new Error(errMsg);
        }
        test.Yay("Successfully received all " + n + " events.");

        log("Verifying event contents...");
        var seen_counters = {};
        for (var i = 0; i < all_events.length; i++) {
            var event = all_events[i];
            if (typeof event.counter !== 'undefined') {
                if (seen_counters[event.counter]) {
                     log("Duplicate event counter received: " + event.counter);
                }
                seen_counters[event.counter] = true;
            }
        }

        var seen_count = Object.keys(seen_counters).length;
        if (seen_count !== n) {
            var errMsg = "Verification failed: saw " + seen_count + " unique counters, expected " + n;
            test.Aww(errMsg);
            throw new Error(errMsg);
        } else {
            test.Yay("Event content verification passed. All counters unique.");
        }

    } catch (e) {
        test.Aww("Event stress test failed: " + e);
        throw e;
    } finally {
        // 4. Unsubscribe
        log("Unsubscribing from topic: " + topic);
        var unsub_start = time.stamp();
        subscriptions.unsubscribe(topic);
        var unsub_time = time.stamp() - unsub_start;
        log("Unsubscription took " + unsub_time + "ms");
    }
}

mainLogger("Starting event/subscription stress test");

var start = time.stamp();
event_stress(100);
var end = time.stamp();
mainLogger("Time taken: " + (end - start) + "ms");
mainLogger("Done");
