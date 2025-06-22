### **Checklist for API Key Usage & Resource Limits Testing**

This document outlines the test coverage provided by the `usage.sh` script, which validates the enforcement of resource limits (disk, memory, events, subscribers) for API keys.

#### I. Disk Usage Limits (`blob` storage)
-   [x] ✅ **Set Disk Limit:** A root user can set a per-key disk storage limit (`--disk`).
-   [x] ✅ **Verify Initial State:** A key's disk usage correctly reports as zero before any blobs are uploaded.
-   [x] ✅ **Enforce Limit (Single Upload):** An attempt to upload a single blob larger than the key's total limit is correctly rejected with a "disk limit exceeded" error.
-   [x] ✅ **No Usage on Failure:** A failed upload does not consume any of the key's disk quota.
-   [x] ✅ **Allow Usage (Under Limit):** An upload smaller than the limit is successful.
-   [x] ✅ **Track Usage:** After a successful upload, the key's `limits` report correctly reflects the new amount of "Bytes on Disk".
-   [x] ✅ **Enforce Limit (Cumulative):** An attempt to upload a second blob that would cause the *total* usage to exceed the limit is correctly rejected.
-   [x] ✅ **Key Deletion Cleanup:** The test cleans up the key and its associated data after the test.

#### II. Memory Usage Limits (`cache`)
-   [x] ✅ **Set Memory Limit:** A root user can set a per-key in-memory cache limit (`--mem`).
-   [x] ✅ **Verify Initial State:** A key's memory usage correctly reports as zero before any data is cached.
-   [x] ✅ **Enforce Limit (Single Set):** A `cache set` operation with a value larger than the key's total limit is rejected with a "memory limit exceeded" error.
-   [x] ✅ **No Usage on Failure:** A failed `cache set` does not consume any of the key's memory quota.
-   [x] ✅ **Allow Usage (Under Limit):** A `cache set` with a value smaller than the limit is successful.
-   [x] ✅ **Track Usage:** After a successful `cache set`, the key's memory usage report correctly shows a non-zero value.
-   [x] ✅ **Enforce Limit (Cumulative):** A second `cache set` that would cause the total usage to exceed the remaining memory quota is correctly rejected.

#### III. Event Rate Limits (`publish`)
-   [x] ✅ **Set Event Rate Limit:** A root user can set an events-per-second limit (`--events`).
-   [x] ✅ **Verify Limit:** The key can view its own configured event rate limit.
-   [x] ✅ **Enforce Rate Limit (Throttling):** A rapid burst of `publish` commands is correctly throttled after an initial allowance.
-   [x] ✅ **Burst Allowance:** The token-bucket implementation correctly allows an initial burst of events before rate-limiting begins.
-   [x] ✅ **Correct Error on Throttle:** Throttled requests fail with the expected "limit exceeded" or "Too Many Requests" error.

#### IV. Concurrent Subscriber Limits (`subscribe`)
-   [x] ✅ **Set Subscriber Limit:** A root user can set a maximum concurrent subscriber limit (`--subs`) for a key.
-   [x] ✅ **Verify Limit:** The key can view its own configured subscriber limit.
-   [x] ✅ **Global Limit Enforcement:** The subscriber limit is enforced globally for the key across all nodes in the cluster.
-   [x] ✅ **Allow Connections (Under Limit):** The key can successfully establish a number of subscriber connections up to its limit, distributed across different nodes.
-   [x] ✅ **Reject Connections (Over Limit):** An attempt to establish one more connection than the allowed limit is correctly rejected with a "subscriber limit exceeded" error.

#### V. Test Script Sanity & Reliability
-   [x] ✅ **Pre-flight Check:** The script verifies server responsiveness with `ping` before running tests, retrying several times to ensure the cluster is ready.
-   [x] ✅ **Dynamic Resource Naming:** Tests use unique, timestamped API key names to ensure isolation and prevent collisions between test runs.
-   [x] ✅ **State Propagation:** The script includes `sleep` commands to allow time for limits set on the leader to propagate to follower nodes before testing against them.
-   [x] ✅ **Comprehensive Cleanup:** Each test function deletes the API key it creates, and a `trap` is used in the subscriber test to ensure background processes are killed, even on script failure.
