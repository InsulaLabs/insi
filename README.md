### cache

A note on cache

The cache does not "bump" on read - its a fully time-based ephemeral value that will exist on the nodes ONLY for the given TTL.

### cURL

```bash
curl -L -k -X POST 'https://localhost:8444/set' -d '{"key": "y", "value": "344"}' -H 'content-type: application/json'
```

Note: `-L` is required to follow redirects since we redirect to the current leader if a non-leader node is hit on writes
but once written, its duplicated to all nodes and can be easily "read" from any node

```bash
curl -k 'https://localhost:8443/get?key=y' 
```
