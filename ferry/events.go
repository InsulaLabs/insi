package ferry

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/db/models"
)

type EventHandler func(event models.EventPayload)

type EventPublisher interface {
	Publish(ctx context.Context, data any) error
}

type EventSubscriber interface {
	Subscribe(ctx context.Context, handler EventHandler) error
}

type Events interface {
	GetPublisher(topic string) EventPublisher

	GetSubscriber(topic string) EventSubscriber

	Purge(ctx context.Context) (int, error)

	PurgeAllNodes(ctx context.Context) (int, error)
}

type eventsImpl struct {
	client *client.Client
	logger *slog.Logger
}

type eventPublisherImpl struct {
	topic  string
	client *client.Client
	logger *slog.Logger
}

type eventSubscriberImpl struct {
	topic  string
	client *client.Client
	logger *slog.Logger
}

func NewEvents(c *client.Client, logger *slog.Logger) Events {
	return &eventsImpl{
		client: c,
		logger: logger.WithGroup("events"),
	}
}

func (e *eventsImpl) GetPublisher(topic string) EventPublisher {
	if topic == "" {
		return &eventPublisherImpl{
			topic:  "",
			client: e.client,
			logger: e.logger,
		}
	}

	return &eventPublisherImpl{
		topic:  topic,
		client: e.client,
		logger: e.logger.With("topic", topic),
	}
}

func (e *eventsImpl) GetSubscriber(topic string) EventSubscriber {
	if topic == "" {
		return &eventSubscriberImpl{
			topic:  "",
			client: e.client,
			logger: e.logger,
		}
	}

	return &eventSubscriberImpl{
		topic:  topic,
		client: e.client,
		logger: e.logger.With("topic", topic),
	}
}

func (e *eventsImpl) Purge(ctx context.Context) (int, error) {
	var disconnected int

	err := client.WithRetriesVoid(ctx, e.logger, func() error {
		count, err := e.client.PurgeEventSubscriptions()
		if err != nil {
			return err
		}
		disconnected = count
		return nil
	})

	return disconnected, translateError(err)
}

func (e *eventsImpl) PurgeAllNodes(ctx context.Context) (int, error) {
	disconnected, err := e.client.PurgeEventSubscriptionsAllNodes()
	return disconnected, translateError(err)
}

func (p *eventPublisherImpl) Publish(ctx context.Context, data any) error {
	if p.topic == "" {
		return fmt.Errorf("cannot publish to empty topic")
	}

	err := client.WithRetriesVoid(ctx, p.logger, func() error {
		return p.client.PublishEvent(p.topic, data)
	})

	return translateError(err)
}

func (s *eventSubscriberImpl) Subscribe(ctx context.Context, handler EventHandler) error {
	if s.topic == "" {
		return fmt.Errorf("cannot subscribe to empty topic")
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	onEvent := func(data any) {
		if payload, ok := data.(models.EventPayload); ok {
			handler(payload)
		} else {
			s.logger.Error("Received invalid event payload type", "type", fmt.Sprintf("%T", data))
		}
	}

	err := s.client.SubscribeToEvents(s.topic, ctx, onEvent)

	return translateError(err)
}
