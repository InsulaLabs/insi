// Example script for demonstrating the Value Store (vs) functionality in OVM.

console.log("--- Value Store (vs) Demo ---");

// 1. Set a value
var key = "mykey";
var value = "myvalue";
console.log("Setting key '" + key + "' to '" + value + "'");
vs.set(key, value);
console.log("Set operation complete.");

time.sleep(100); // Wait 100ms for settling

// 2. Get the value
console.log("Getting key '" + key + "'...");
var retrievedValue = vs.get(key);
console.log("Retrieved value: " + retrievedValue);
if (retrievedValue !== value) {
    throw new Error("Value mismatch! Expected '" + value + "', got '" + retrievedValue + "'");
}

// 3. SetNX (Set if Not Exists)
var keyNX = "newkey";
console.log("Attempting to setnx on existing key '" + key + "' (should fail)...");
var resultNX_fail = vs.setnx(key, "some other value");
console.log("setnx result for existing key: " + resultNX_fail); // Should be false
if (resultNX_fail) {
    throw new Error("setnx should have failed for existing key!");
}

console.log("Attempting to setnx on new key '" + keyNX + "' (should succeed)...");
var resultNX_success = vs.setnx(keyNX, "this is a new value");
console.log("setnx result for new key: " + resultNX_success); // Should be true
if (!resultNX_success) {
    throw new Error("setnx should have succeeded for new key!");
}

// 4. Compare-And-Swap (CAS)
console.log("Attempting CAS on key '" + key + "' with wrong old value (should fail)...");
var resultCAS_fail = vs.cas(key, "wrong old value", "new value");
console.log("CAS result with wrong old value: " + resultCAS_fail); // Should be false
if (resultCAS_fail) {
    throw new Error("CAS should have failed with wrong old value!");
}


console.log("Attempting CAS on key '" + key + "' with correct old value (should succeed)...");
var resultCAS_success = vs.cas(key, value, "new and improved value");
console.log("CAS result with correct old value: " + resultCAS_success); // Should be true
if (!resultCAS_success) {
    throw new Error("CAS should have succeeded with correct old value!");
}
var updatedValue = vs.get(key);
console.log("Value after successful CAS: " + updatedValue);


// 5. Iterate by Prefix
var prefix = "user:";
console.log("Setting some keys with prefix '" + prefix + "' for iteration...");
vs.set(prefix + "1", "Alice");
vs.set(prefix + "2", "Bob");
vs.set(prefix + "3", "Charlie");

time.sleep(200); // Allow time for writes to settle

console.log("Iterating keys with prefix '" + prefix + "'...");
var keys = vs.iterateByPrefix(prefix, 0, 10);
console.log("Found keys: " + JSON.stringify(keys));
if (keys.length !== 3) {
    throw new Error("Iteration did not return the expected number of keys!");
}


// 6. Delete
console.log("Deleting key '" + key + "'...");
vs.delete(key);
var deletedValue = vs.get(key);
console.log("Value after delete: " + deletedValue); // Should be null
if (deletedValue !== null) {
    throw new Error("Key should have been deleted!");
}

console.log("--- Value Store Demo Complete ---");
