try {
    var response = admin.createKey("test-key-" + time.stamp());
    test.Yay("Created key: " + response.key);
} catch (e) {
    test.Aww("Error creating key: " + e);
    throw e;
}