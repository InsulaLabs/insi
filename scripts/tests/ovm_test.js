// This script demonstrates the sandboxed execution of `ovm.run()`.

console.log("--- OVM Sub-Execution Demo ---");

// 1. Define a sub-script that defines a variable and returns a value.
// This script will be stored in the value store.
var subScript = "console.log(\"[sub-script] Executing.\");" +
    "var x = 100;" +
    "console.log(\"[sub-script] > Variable 'x' defined as: \" + x);" + // This should be 'undefined' as it runs in a separate scope.
    "console.log(\"[sub-script] > Attempting to access parent's 'y' variable (should be undefined): \" + (typeof y));" +
    "'sub-script-result';"; // This is the return value.

var scriptKey = "demo/sub_script";

// 2. Clean up any previous runs and store the sub-script.
console.log("[main-script] Storing sub-script in value store at key: " + scriptKey);
vs.delete(scriptKey); // a clean slate
vs.set(scriptKey, subScript);

// 3. Define a variable in the main script's scope.
var y = 50;
console.log("[main-script] Variable 'y' is defined as: " + y);

// 4. Retrieve and execute the sub-script.
console.log("[main-script] Retrieving script from value store...");
var retrievedScript = vs.get(scriptKey);

console.log("[main-script] Executing retrieved script via ovm.run()...");
var result = ovm.run(retrievedScript);
console.log("[main-script] ...sub-script execution finished.");

console.log("[main-script] Result from sub-script: '" + result + "'");

// 5. Verify that the main script's scope was not polluted.
// If the sub-script's execution wasn't sandboxed, 'x' would be 100 here.
console.log("[main-script] Checking for pollution. Variable 'x' in main scope is: " + (typeof x));

if (typeof x === 'undefined') {
  console.log("[main-script] ✅ SUCCESS: Main scope was not polluted by the sub-script.");
} else {
  console.error("[main-script] ❌ FAILURE: Main scope was polluted. 'x' is " + x);
  throw new Error("Scope pollution detected!");
}

if (result !== 'sub-script-result') {
    console.error("[main-script] ❌ FAILURE: Did not get expected result from sub-script. Got: '" + result + "'");
    throw new Error("Incorrect result from sub-script.");
} else {
    console.log("[main-script] ✅ SUCCESS: Correct result returned from sub-script.");
}

console.log("--- Demo Complete ---");

// Exit with 0 to indicate success for test runners.
0;
