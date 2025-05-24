package events

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

// The actual implementation of the TopicPublisher interface that
// is handed to a caller who wants to publish events to a topic.
// When they request a publisher for a topic we validate the topic
// is permitted and then return a publisher that will publish the
// event to the topic on their behalf.
type topicPublisherImpl struct {
	emitterId string
	topic     string

	// The actual function that fulfills the event publishing logic
	// that is handed to the pubsub implementation so the underlying
	// transport can be swapped out for something else.
	router EventRouter
}

func (tp *topicPublisherImpl) Publish(ctx context.Context, data []byte) error {
	return tp.router(ctx, Event{
		EventID:   uuid.NewString(),
		Topic:     tp.topic,
		EmittedAt: time.Now(),
		Emitter:   tp.emitterId,
		Data:      data,
	})
}

// The actual implementation of the PubSub interface that
// is handed to the caller who wants to subscribe to events
// from a topic. When they request a subscriber for a topic
// we validate the topic is permitted and then return a
// subscriber that will receive the events from the topic.
// This is also the implementation that is used to generate
// publishers for the caller to use.
type pubSubImpl struct {
	permittedTopics []string
	topics          map[string]TopicPublisher
	subscribers     map[string][]TopicSubscriber

	topicsMutex      sync.RWMutex
	subscribersMutex sync.RWMutex

	router EventRouter
}

func (ps *pubSubImpl) GetPermittedTopics() ([]string, error) {
	return ps.permittedTopics, nil
}

func (ps *pubSubImpl) GetPublisher(emitterId, topic string) (TopicPublisher, error) {
	newPublisher := &topicPublisherImpl{
		emitterId: emitterId,
		topic:     topic,
		router:    ps.router,
	}
	return newPublisher, nil
}

func (ps *pubSubImpl) Subscribe(topic string, subscriber TopicSubscriber) (Unsubscriber, error) {
	if !slices.Contains(ps.permittedTopics, topic) {
		return nil, ErrTopicNotPermitted
	}

	ps.subscribersMutex.Lock()
	defer ps.subscribersMutex.Unlock()

	ps.subscribers[topic] = append(ps.subscribers[topic], subscriber)

	// Return a function to unsubscribe from the topic that captures the mutex
	// so it can be done safely and at any time from the subscriber owner
	return func() {
		ps.subscribersMutex.Lock()
		defer ps.subscribersMutex.Unlock()

		ps.subscribers[topic] = slices.DeleteFunc(ps.subscribers[topic], func(s TopicSubscriber) bool {
			return s == subscriber
		})
	}, nil
}
