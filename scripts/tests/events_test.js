function testEventEmission() {
    console.log("running testEventEmission");

    // 1. Get initial limits
    var initialLimits = admin.getLimits().usage;
    var initialEvents = initialLimits.events_emitted || 0;
    console.log("Initial events emitted: " + initialEvents);
    time.sleep(500); // Give a moment before starting

    // 2. Emit an event
    var topic = "ovm-test-topic-" + time.stamp();
    var eventData = { test: "data", value: 123 };
    console.log("Emitting event to topic: " + topic);
    emit(topic, eventData);

    // 3. Wait for propagation
    time.sleep(1000); // Wait for usage data to be updated

    // 4. Get new limits
    var newLimits = admin.getLimits().usage;
    var newEvents = newLimits.events_emitted || 0;
    console.log("New events emitted: " + newEvents);

    // 5. Assert
    var expectedEvents = initialEvents + 1;
    if (newEvents !== expectedEvents) {
        var errMsg = "Event emission tracking failed. Expected " + expectedEvents + " events, but counted " + newEvents;
        test.Aww(errMsg);
        throw new Error(errMsg);
    }

    test.Yay("Event emission was tracked correctly!");
}

try {
    testEventEmission();
    0; // success
} catch (e) {
    1; // failure
}
