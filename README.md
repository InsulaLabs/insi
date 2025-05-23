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


TODO:

PUBSUB

If we make a pub-sub service and make a route for posting publish requests then we can write tot he raft log the same way
as a "set" and when its applied to the remote node we can inform their pubsub server that a new publish happened and it can
fwd the event to whatever subscribers are bound to that instance so subs can be on any node of the cluster

Note: we WILL have to forward these events to the master still, just like sets, as its the only one who can add to the log