function testEventSubscription() {
    console.log("running testEventSubscription");
    var topic = "ovm-subscription-test-" + time.stamp();
    var event1 = { message: "hello" };
    var event2 = { message: "world" };

    try {
        // 1. Subscribe
        console.log("Subscribing to topic: " + topic);
        subscriptions.subscribe(topic);
        time.sleep(1000); // Give time for subscription to establish

        // 2. Emit events
        console.log("Emitting event 1");
        emit(topic, event1);
        console.log("Emitting event 2");
        emit(topic, event2);
        time.sleep(1000); // Give time for events to be collected

        // 3. Poll for events
        console.log("Polling for events");
        var receivedEvents = subscriptions.poll(topic, 5);

        if (receivedEvents.length !== 2) {
            throw new Error("Expected 2 events, but got " + receivedEvents.length);
        }

        // Deep comparison is tricky in otto, so we'll just check a property.
        if (receivedEvents[0].message !== "hello" || receivedEvents[1].message !== "world") {
            throw new Error("Received events do not match emitted events. Got: " + JSON.stringify(receivedEvents));
        }

        test.Yay("Successfully received emitted events via subscription!");

        // 4. Poll again, should be empty
        var emptyPoll = subscriptions.poll(topic, 5);
        if (emptyPoll.length !== 0) {
            throw new Error("Expected poll to be empty after consuming events, but got " + emptyPoll.length + " events.");
        }
        test.Yay("Event collector is empty after polling, as expected.");


    } catch (e) {
        test.Aww("Event subscription test failed: " + e);
        throw e; // rethrow to fail the test script
    } finally {
        // 5. Unsubscribe
        console.log("Unsubscribing from topic: " + topic);
        subscriptions.unsubscribe(topic);
    }
}

try {
    testEventSubscription();
    0; // success
} catch (e) {
    1; // failure
} 