package events

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrTopicNotPermitted = errors.New("topic not permitted")
)

type Event struct {
	EventID   string
	Topic     string
	EmittedAt time.Time
	Emitter   string
	Data      []byte
}

type TopicPublisher interface {
	// Publish is handed a context that should be respected by the EventRouter provider
	// such that if the context is cancelled the event should not be published
	// (if it takes to long or whatever the reason - use the ctx to cancel the publish)
	Publish(ctx context.Context, data []byte) error
}

// TopicSubscriber is the interface that is used to receive events from a topic.
// When returned from the PubSub.Subscribe method it is the responsibility of the
// caller to call the Unsubscriber function to unsubscribe from the topic.
type TopicSubscriber interface {
	OnMessage(ctx context.Context, event Event)
}

// Call to unsubscribe from a topic
type Unsubscriber func()

// The actual function that fulfills the event publishing logic
// that is handed to the pubsub implementation so the underlying
// transport can be swapped out for something else.
type EventRouter func(ctx context.Context, event Event) error

type PubSub interface {
	GetPermittedTopics() ([]string, error)
	GetPublisher(emitterId, topic string) (TopicPublisher, error)
	Subscribe(topic string, subscriber TopicSubscriber) (Unsubscriber, error)
}

type Config struct {
	Router EventRouter
	Topics []string
}

func NewPubSub(config Config) PubSub {
	return &pubSubImpl{
		permittedTopics:  config.Topics,
		topics:           make(map[string]TopicPublisher),
		subscribers:      make(map[string][]TopicSubscriber),
		topicsMutex:      sync.RWMutex{},
		subscribersMutex: sync.RWMutex{},
		router:           config.Router,
	}
}
