package models

import "time"

/*
	Payloads for the various KV, Tag, Key, and Cache operations.
	These are all prefixed by the caller's api key unique identifier.
	Contextually seperating the accessable keys, tags, and caches for each
	api key.
*/

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

/*
	Ephemeral tokens that external users can use to see if some
	given token is valid for some given scopes.

	The validatity of the token means nothing to the system inherently.
	Tokens are node-local meaning they are not distributed across nodes.

	Its simply for a user of the system to generate a secure, epehemeral
	1-time use token that is tied to some nebulous "scope" data that
	is contextually relevant to the user.
*/

type EtokenRequest struct {
	Scopes map[string]string `json:"scopes"`
	TTL    time.Duration     `json:"ttl"`
}

type EtokenResponse struct {
	Token string `json:"token"`
}

type EtokenVerifyRequest struct {
	Token  string            `json:"token"`  // The token to verify
	Scopes map[string]string `json:"scopes"` // The scopes that are being requested (must be subset of the scopes in the token)
}

type EtokenVerifyResponse struct {
	Verified bool `json:"verified"`
}
