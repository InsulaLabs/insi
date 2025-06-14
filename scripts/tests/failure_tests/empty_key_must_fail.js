// This script is designed to fail by using an empty key.
function main() {
    // This should cause a panic in the OVM.
    vs.set("", "some_value");
    return 0; // Should not be reached.
}

try {
    main();
} catch(e) {
    throw e;
} 