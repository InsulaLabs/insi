# About

The INSI database is a distributed database application that utilizes raft. In addition to standard k/v storage contextualized by accessor prefix,
insi also provides *events, cache, atomics, and queues.* This means using insi gives most of the functionality requred to perform a lot of common
distributed tasks.

# Server

INSI's server is _insid_. running with `--host` will cause it to spin up a cluster locally using any `cluster.yaml` that is in its working dir, or a config can
be specified with args. Using input argument `--as <node-id>` will load that specific node's configuration from the yaml and start a local instance as the
given node to serve as part of a cluster. It will attempt to auto-join other nodes if they exist. In the event that its a NEW server on the system
it will wait and serve and auto join if a peer becomes available. If its a re-start the instance will attempt to load the raft snapshots and catch up/
autojoin (See `--help` for more info.)

## K/V Storage

Backed by badger, insi uses a pretty standard no-fuss k/v storage that is scoped to the prefix assigned to the authenticated token working with them.
In fact, all of the endpoints offered by insi leverages this scoping.

As a user, the only way this manifests itself is that you won't be able to access keys/values not set by the key being used.

Insi offers iterating, batch setting, and batch deleting of k/v pairs

## Cache

Using a TTL cache and raft insi distributes "k/v" pairs set to a cache endpoint to persist in-memory for a given TTL. There is a configurable upper limit
to this TTL. The cache used is not a bump-cache. This means that a read from the cache does not implicitly increase its TTL. Death here is a sort of contract.
To avoid having to track the entry and then write to raft when a bump were to occur, we assume that the cache item will expire simultaniously across the network
(a calculation is done to offset raft transfer to distributed nodes to ensure tight ttl sync across the network)

## Atomics

Due to the nature of the K/V storage its important to offer a means to indicate something atomically. For this insi offers atomic operations that are scoped
unique to "atomic" values to ensure they don't tramble regular pairs. These atomics are floored at 0 and only permit creation, destruction, and adding (negative
additions permitted.)

## Queues

Syncing a queue for multiple readers across the network we use raft to offer a push/pop in-memory (not disk backed) queue that maps some string->string permitting
a caller to organize and synchronize tasking

## Events

Events leverage the raft network to offer the standard pub/sub architecture expected when considering events. A publish to a topic is sent over raft and
any websocket connections on that node subscribed to that topic will have the event deleveiered. There are a configurable number of web socket sessions per
node offered, meaning one could "load-balance" the subscribers across the node-scape and have real-time event delivery for quite a few clients.

# Client

The _insic_ is a client to interact with an `insid`. Giving it the configuration and `--root` will enable interacting with the server using the shared secret
as the authentication mechanism (only way currently supported at time of writing.)