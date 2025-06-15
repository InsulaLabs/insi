// This script is designed to fail by using an empty event topic.
function main() {
    // This should cause a panic in the OVM.
    emit("", { data: "test" });
    return 0; // Should not be reached.
}

try {
    main();
} catch(e) {
    throw e;
} 