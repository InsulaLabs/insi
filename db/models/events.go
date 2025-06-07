package models

import "time"

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
