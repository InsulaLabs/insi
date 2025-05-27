# InsuDB

Insi offers a redis-like system(ish) where we have a potentially distributed
k/v database that we can interface with over https. insi has a couple of concepts
that may or may not be in redis (idk ive never really used it). Insi has the ability
to specifically tag and sort-by-tag the key/values and maintains an in-memory ttl cache
of ephemeral k/v data. Inis uses raft to sync ops across any followers

The insu DB node performs the following actions on startup:

Checks the given indudbDir given by the config and makes sure it exists.
Insi utilizes the insula core library tkv for key/value storage (based on badger)
The insula tkv has:

    Two Key/Value databases
    One "ttl cache"

The badger databases are used to store the standard `key->value`(kv) mapping common
with kv databases. The second is a "tag" database that maps `tag->key` (tk).
The choice to have two databases rather than one containing both `kvs` and `tks`
was honestly arbitrary and done only because it _felt_ right. It might be more
effecient to have one database. We can do testing later to confirm (not important now)

The `ttl cache` is an arbitrary k/v store that site in-memory only where each entry
expires after a given time. This ttl cache has been configured specifically to not
"bump on hit" as the intention is to keep ephemeral values around network-wide
and having the bump on hit would make insi a bit more complex:
    If we enable bump on hit here then every "bump" that occurs would potentially
    be needed to propogate to all nodes to bump their respective caches if we expect
    to keep 1:1 parity on all nodes on disk and in memory (Databases AND caches)
    Having no bump on hit is what we initially wanted for security uses and it simplifies
    the synchornization as all nodes know when the item should perish and agree to kill
    it at the given time (we expect the ttl cache to function properly obv which means
    parity is acheived)

# Access

In the config there exists an "instanceSecret" var. A sha256 of this is used to create
an "administrator token" so ensure the instanceSecret has high entropy.
The admin token is what is used to create/delete `api keys` that are known to all of the nodes.
When the admin token is used to create an api key it gives an arbitrary "entity" string
that can be used to correspond to internal records of the calling system, and a UUID + meta.
When an api key is used to modify the system each value stored by the api key will be prefixed
by this UUID + meta data stored in the key to distinguish and "lock" the agent of the api key
to working with only data that they themselves have stored.

# client

There is a client library to make everything easier to work with than raw http commands in
go. The `insic` client application uses the same configuration as the nodes so it can connect
to various nodes and perform actions with some
given api key.

```bash

./insic --target node1 get x  

```