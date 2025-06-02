package models

import "time"

/*
	Payloads for the various KV, Key, and Cache operations.
	These are all prefixed by the caller's api key unique identifier.
	Contextually seperating the accessable keys, and caches for each
	api key.
*/

type KVPayload struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KeyPayload struct {
	Key string `json:"key"`
}

type CachePayload struct {
	Key   string        `json:"key"`
	Value string        `json:"value"`
	TTL   time.Duration `json:"ttl"`
	SetAt time.Time     `json:"set_at"`
}

/*
	Events emitted out to the system. Any given api key can publish to and subscribe
	to any given topic. The event topics and subscriptions are contextually locked
	to the api key meaning one user's "update" topic will be different from another
	users "event" topic as they are hard-scoped to the api key's unique identifier.
*/

type Event struct {
	Topic string `json:"topic"`
	Data  any    `json:"data"`
}

type EventPayload struct {
	Topic     string    `json:"topic"`
	Data      any       `json:"data"`
	EmittedAt time.Time `json:"emitted_at"`
}

// ObjectPayload is used for transferring objects.
// The Value is expected to be a byte slice representing the object data.
type ObjectPayload struct {
	Key   string `json:"key"`
	Value []byte `json:"value"` // Changed from string to []byte
}

type BatchSetRequest struct {
	Items []KVPayload `json:"items"`
}

type BatchDeleteRequest struct {
	Keys []string `json:"keys"`
}

type TokenData struct {
	Entity string `json:"entity"`
	UUID   string `json:"uuid"`
}
