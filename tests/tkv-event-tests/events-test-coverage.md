### **Checklist for Event System (Pub/Sub) Testing**

This document outlines the test coverage provided by the `events.sh` script for the `insic` event publish/subscribe system.

#### I. Core Pub/Sub Lifecycle
-   [x] ✅ **Subscribe:** A client can connect and subscribe to a namespaced topic.
-   [x] ✅ **Publish:** A client can publish a message to a specific topic.
-   [x] ✅ **Receive:** A subscriber correctly receives messages published to its topic after it has subscribed.
-   [x] ✅ **Multi-Message Delivery:** The system correctly delivers multiple, sequential messages to a subscriber.
-   [x] ✅ **Message Integrity:** The content of the received message matches the content of the published message.

#### II. Test Script & Environment Sanity
-   [x] ✅ **Test Isolation:** The script uses a unique, dynamically generated topic name for each test run to ensure isolation.
-   [x] ✅ **Subscriber Backgrounding:** The test successfully starts the subscriber as a background process.
-   [x] ✅ **Subscriber Liveness Check:** The script confirms the subscriber process is running before publishing messages.
-   [x] ✅ **Subscriber Connection Confirmation:** The test waits for the subscriber to explicitly signal a successful connection before proceeding.
-   [x] ✅ **Resource Cleanup:** The script ensures the background subscriber process is terminated and temporary test files are removed on exit, failure, or interruption (via `trap`).

#### III. Command & Authentication
-   [x] ✅ **Root-Level Access:** Both `publish` and `subscribe` commands work correctly when authenticated with root privileges (`--root` flag).
-   [x] ✅ **Node Targeting:** The commands correctly operate on the specified target node (`--target` flag).

#### IV. Failure & Timeout Handling
-   [x] ✅ **Subscriber Start Failure:** The test correctly identifies and fails if the subscriber process exits prematurely.
-   [x] ✅ **Subscriber Timeout:** The test fails if the subscriber does not confirm a connection within a specified timeout period.
-   [x] ✅ **Publish Failure:** The test correctly identifies and fails if a `publish` command returns an error.
-   [x] ✅ **Verification Failure:** The test run fails if the set of received messages does not match the set of published messages.
