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

type BatchSetRequest struct {
	Items []KVPayload `json:"items"`
}

type BatchDeleteRequest struct {
	Keys []string `json:"keys"`
}
