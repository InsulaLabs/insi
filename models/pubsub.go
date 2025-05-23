package models

import (
	"context"
	"time"
)

type Event struct {
	EmittedAt time.Time
	EmitterID string
	Key       string // topic
	Value     string // message
}

type Subscriber interface {
	GetID() string
	OnEvent(ctx context.Context, event Event) error
}

type Publisher interface {
	Publish(ctx context.Context, message string) error
}

type PubSubManager interface {
	GetPublisherForTopic(topic string, emitterID string) (Publisher, error)
	SubscribeToTopic(topic string, subscriber Subscriber) error
	UnsubscribeFromTopic(topic string, subscriberID string) error // from GetID()
}

// ---------------------------------------------------------------------------------------------------------------------
