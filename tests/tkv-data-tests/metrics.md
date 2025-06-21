### **Checklist for Metrics System Testing**

This document outlines the test coverage for the administrative metrics endpoint, which provides real-time operations-per-second counters.

#### I. Endpoint Access & Security
-   [x] ✅ **Root-Only Access:** The `/admin/metrics/ops` endpoint can only be accessed with the `--root` flag. Attempts to access it with a standard or non-existent key are rejected.
-   [x] ✅ **Successful Access:** The endpoint can be successfully queried with the `--root` flag, returning a properly formatted response.
-   [x] ✅ **Response Structure:** The response correctly contains the `Operations Per Second:` header and labels for all tracked metrics (System, Value Store, Cache, etc.).

#### II. Rate Calculation & Tracking
-   [x] ✅ **Value Store Tracking:** Sustained `insic set` operations correctly result in a positive, non-zero `Value Store` ops/sec metric.
-   [x] ✅ **Cache Tracking:** Sustained `insic cache set` operations correctly result in a positive, non-zero `Cache` ops/sec metric.
-   [x] ✅ **Subscriber Tracking:** A sustained load of new `insic subscribe` connections correctly results in a positive, non-zero `Subscribers` ops/sec metric.
-   [x] ✅ **Event Tracking:** A sustained load of `insic publish` commands correctly results in a positive, non-zero `Events` ops/sec metric.
-   [x] ✅ **Metric Quiescence:** When load generation stops, the corresponding ops/sec metrics correctly return to zero on subsequent checks, demonstrating that the rate calculation is time-sensitive and not a cumulative counter. (Implicitly tested by the pauses between test sections).
