// Example script for demonstrating the Cache Store (cache) functionality in OVM.

console.log("--- Cache Store (cache) Demo ---");

// 1. Set a value in the cache
var key = "cachekey";
var value = "cachevalue";
console.log("Setting cache key '" + key + "' to '" + value + "'");
cache.set(key, value);
time.sleep(500);
console.log("Set operation complete.");

time.sleep(100); // Wait 100ms for settling

// 2. Get the value from the cache
console.log("Getting cache key '" + key + "'...");
var retrievedValue = cache.get(key);
time.sleep(500);
console.log("Retrieved value: " + retrievedValue);
if (retrievedValue !== value) {
    throw new Error("Cache value mismatch! Expected '" + value + "', got '" + retrievedValue + "'");
}

// 3. SetNX (Set if Not Exists) in cache
var keyNX = "newcachekey";
console.log("Attempting to cache.setnx on existing key '" + key + "' (should fail)...");
var resultNX_fail = cache.setnx(key, "some other value");
time.sleep(500);
console.log("cache.setnx result for existing key: " + resultNX_fail); // Should be false
if (resultNX_fail) {
    throw new Error("cache.setnx should have failed for existing key!");
}

console.log("Attempting to cache.setnx on new key '" + keyNX + "' (should succeed)...");
var resultNX_success = cache.setnx(keyNX, "this is a new cache value");
time.sleep(500);
console.log("cache.setnx result for new key: " + resultNX_success); // Should be true
if (!resultNX_success) {
    throw new Error("cache.setnx should have succeeded for new key!");
}

// 4. Compare-And-Swap (CAS) in cache
console.log("Attempting cache.cas on key '" + key + "' with wrong old value (should fail)...");
var resultCAS_fail = cache.cas(key, "wrong old value", "new cache value");
time.sleep(500);
console.log("cache.cas result with wrong old value: " + resultCAS_fail); // Should be false
if (resultCAS_fail) {
    throw new Error("cache.cas should have failed with wrong old value!");
}


console.log("Attempting cache.cas on key '" + key + "' with correct old value (should succeed)...");
var resultCAS_success = cache.cas(key, value, "new and improved cache value");
time.sleep(500);
console.log("cache.cas result with correct old value: " + resultCAS_success); // Should be true
if (!resultCAS_success) {
    throw new Error("cache.cas should have succeeded with correct old value!");
}
var updatedValue = cache.get(key);
time.sleep(500);
console.log("Value after successful cache.cas: " + updatedValue);


// 5. Iterate by Prefix in cache
var prefix = "session:";
console.log("Setting some cache keys with prefix '" + prefix + "' for iteration...");
cache.set(prefix + "alpha", "user1_data");
time.sleep(500);
cache.set(prefix + "beta", "user2_data");
time.sleep(500);
cache.set(prefix + "gamma", "user3_data");
time.sleep(500);

time.sleep(200); // Allow time for writes to settle

console.log("Iterating cache keys with prefix '" + prefix + "'...");
var keys = cache.iterateByPrefix(prefix, 0, 10);
time.sleep(500);
console.log("Found cache keys: " + JSON.stringify(keys));
if (keys.length !== 3) {
    throw new Error("Cache iteration did not return the expected number of keys!");
}


// 6. Delete from cache
console.log("Deleting cache key '" + key + "'...");
cache.delete(key);
time.sleep(500);
var deletedValue = cache.get(key);
console.log("Value after cache.delete: " + deletedValue); // Should be null
if (deletedValue !== null) {
    throw new Error("Cache key should have been deleted!");
}

// 7. Cleanup other keys created during the demo
console.log("Cleaning up other demo keys...");
cache.delete(keyNX);
time.sleep(500);
cache.delete(prefix + "alpha");
time.sleep(500);
cache.delete(prefix + "beta");
time.sleep(500);
cache.delete(prefix + "gamma");
time.sleep(500);
console.log("Cleanup complete.");

console.log("--- Cache Store Demo Complete ---");
