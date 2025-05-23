package models

import "time"

type KVPayload struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type TagPayload struct {
	Key string `json:"key"`
	Tag string `json:"tag"`
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
