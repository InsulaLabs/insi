// Example script for demonstrating the event emitter (emit) functionality in OVM.
// Note: Subscribing to events must be done in a separate process, e.g., using 'insic subscribe'.

console.log("--- Event Emitter (emit) Demo ---");

var topic = "ovm-events";
var count = 3;

console.log("Will emit " + count + " events on topic '" + topic + "'...");

for (var i = 1; i <= count; i++) {
    var eventData = {
        message: "Hello from OVM!",
        sequence: i,
        timestamp: time.stamp()
    };

    console.log("Emitting event " + i + " with data: " + JSON.stringify(eventData));
    
    // The emit function takes the topic and the data payload.
    // The data payload can be any object that can be marshaled to JSON.
    emit(topic, eventData);

    console.log("Event " + i + " emitted. Sleeping for 1 second...");
    time.sleep(1000); // Sleep for 1 second between events
}

console.log("--- Event Emitter Demo Complete ---");
console.log("You can listen for these events by running the following command in another terminal:");
console.log("insic subscribe " + topic);
