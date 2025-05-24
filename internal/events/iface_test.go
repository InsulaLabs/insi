package events

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// mockSubscriber is a TopicSubscriber that records received messages.
type mockSubscriber struct {
	id       string
	mu       sync.Mutex
	messages []Event
	failOn   bool
}

func newMockSubscriber(id string) *mockSubscriber {
	return &mockSubscriber{
		id:       id,
		messages: make([]Event, 0),
	}
}

func (ms *mockSubscriber) OnMessage(ctx context.Context, event Event) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.failOn {
		return
	}
	ms.messages = append(ms.messages, event)
}

func (ms *mockSubscriber) getMessages() []Event {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	msgsCopy := make([]Event, len(ms.messages))
	copy(msgsCopy, ms.messages)
	return msgsCopy
}

// mockEventRouter simulates event dispatch and captures published events.
type mockEventRouter struct {
	mu              sync.RWMutex
	publishedEvents []Event
	subscribers     map[string][]TopicSubscriber
	simulateError   bool
	publishDelay    time.Duration
}

func newMockEventRouter() *mockEventRouter {
	return &mockEventRouter{
		publishedEvents: make([]Event, 0),
		subscribers:     make(map[string][]TopicSubscriber),
	}
}

func (mer *mockEventRouter) routeEvent(ctx context.Context, event Event) error {
	mer.mu.Lock()

	if mer.simulateError {
		mer.mu.Unlock()
		return errors.New("mock router publish error")
	}

	// Ensure event has an ID if it's missing, for robustness in testing
	// In real scenario, publisher (topicPublisherImpl) ensures this.
	if event.EventID == "" {
		event.EventID = uuid.NewString()
	}

	mer.publishedEvents = append(mer.publishedEvents, event)
	mer.mu.Unlock() // Unlock before dispatching to avoid holding lock during subscriber processing

	if mer.publishDelay > 0 {
		select {
		case <-time.After(mer.publishDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	mer.mu.RLock() // RLock for reading subscribers map
	defer mer.mu.RUnlock()
	if subs, ok := mer.subscribers[event.Topic]; ok {
		for _, s := range subs {
			go s.OnMessage(ctx, event) // Pass the full event
		}
	}
	return nil
}

func (mer *mockEventRouter) getPublishedEvents() []Event {
	mer.mu.RLock()
	defer mer.mu.RUnlock()
	eventsCopy := make([]Event, len(mer.publishedEvents))
	copy(eventsCopy, mer.publishedEvents)
	return eventsCopy
}

func (mer *mockEventRouter) addTestSubscriber(topic string, sub TopicSubscriber) {
	mer.mu.Lock()
	defer mer.mu.Unlock()
	mer.subscribers[topic] = append(mer.subscribers[topic], sub)
}

func (mer *mockEventRouter) removeTestSubscriber(topic string, sub TopicSubscriber) {
	mer.mu.Lock()
	defer mer.mu.Unlock()
	if subs, ok := mer.subscribers[topic]; ok {
		mer.subscribers[topic] = slices.DeleteFunc(subs, func(s TopicSubscriber) bool {
			return s == sub
		})
	}
}

func TestPubSub(t *testing.T) {
	permittedTopics := []string{"topic1", "topic2", "shared_topic"}
	emitterID := "test-emitter"

	t.Run("GetPermittedTopics", func(t *testing.T) {
		router := newMockEventRouter()
		ps := NewPubSub(Config{
			Router: router.routeEvent,
			Topics: permittedTopics,
		})

		topics, err := ps.GetPermittedTopics()
		if err != nil {
			t.Fatalf("GetPermittedTopics() error = %v, wantErr nil", err)
		}
		if !slices.Equal(topics, permittedTopics) {
			t.Errorf("GetPermittedTopics() = %v, want %v", topics, permittedTopics)
		}
	})

	t.Run("GetPublisherAndPublish", func(t *testing.T) {
		router := newMockEventRouter()
		ps := NewPubSub(Config{
			Router: router.routeEvent,
			Topics: permittedTopics,
		})

		publisher, err := ps.GetPublisher(emitterID, "topic1")
		if err != nil {
			t.Fatalf("GetPublisher() error = %v, wantErr nil", err)
		}

		testData := []byte("hello world")
		ctx := context.Background()
		if err := publisher.Publish(ctx, testData); err != nil {
			t.Fatalf("Publish() error = %v, wantErr nil", err)
		}

		publishedEvents := router.getPublishedEvents()
		if len(publishedEvents) != 1 {
			t.Fatalf("expected 1 event published, got %d", len(publishedEvents))
		}
		event := publishedEvents[0]
		if event.Topic != "topic1" {
			t.Errorf("event topic = %s, want topic1", event.Topic)
		}
		if event.Emitter != emitterID {
			t.Errorf("event emitter = %s, want %s", event.Emitter, emitterID)
		}
		if !slices.Equal(event.Data, testData) {
			t.Errorf("event data = %s, want %s", event.Data, testData)
		}
		if event.EventID == "" {
			t.Error("event EventID should not be empty")
		}
		if event.EmittedAt.IsZero() {
			t.Error("event EmittedAt should not be zero")
		}
	})

	t.Run("SubscribeAndReceiveMessageAndUnsubscribe", func(t *testing.T) {
		router := newMockEventRouter()
		ps := NewPubSub(Config{
			Router: router.routeEvent,
			Topics: permittedTopics,
		})
		topic := "topic1"
		subscriber1 := newMockSubscriber("sub1")

		router.addTestSubscriber(topic, subscriber1)

		unsub, err := ps.Subscribe(topic, subscriber1)
		if err != nil {
			t.Fatalf("Subscribe() error = %v, wantErr nil", err)
		}

		publisher, _ := ps.GetPublisher(emitterID, topic)
		testData := []byte("message 1")
		ctx := context.Background()
		if err := publisher.Publish(ctx, testData); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		msgs := subscriber1.getMessages()
		if len(msgs) != 1 {
			t.Fatalf("expected 1 message for subscriber, got %d", len(msgs))
		}
		receivedEvent := msgs[0]
		if !slices.Equal(receivedEvent.Data, testData) {
			t.Errorf("subscriber1 received data = %s, want %s", receivedEvent.Data, testData)
		}
		if receivedEvent.Topic != topic {
			t.Errorf("subscriber1 received topic = %s, want %s", receivedEvent.Topic, topic)
		}
		if receivedEvent.Emitter != emitterID {
			t.Errorf("subscriber1 received emitter = %s, want %s", receivedEvent.Emitter, emitterID)
		}
		if receivedEvent.EventID == "" {
			t.Error("subscriber1 received empty EventID")
		}

		unsub()
		router.removeTestSubscriber(topic, subscriber1)

		testData2 := []byte("message 2")
		if err := publisher.Publish(ctx, testData2); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		msgsAfterUnsub := subscriber1.getMessages()
		if len(msgsAfterUnsub) != 1 {
			t.Errorf("expected 1 message after unsubscribe, got %d. Messages: %v", len(msgsAfterUnsub), msgsAfterUnsub)
		}
	})

	t.Run("SubscribeToNonPermittedTopic", func(t *testing.T) {
		router := newMockEventRouter()
		ps := NewPubSub(Config{
			Router: router.routeEvent,
			Topics: permittedTopics,
		})
		subscriber := newMockSubscriber("sub-non-permitted")

		_, err := ps.Subscribe("non-existent-topic", subscriber)
		if !errors.Is(err, ErrTopicNotPermitted) {
			t.Errorf("Subscribe() error = %v, want %v", err, ErrTopicNotPermitted)
		}
	})

	t.Run("PublishErrorFromRouter", func(t *testing.T) {
		router := newMockEventRouter()
		router.simulateError = true
		ps := NewPubSub(Config{
			Router: router.routeEvent,
			Topics: permittedTopics,
		})

		publisher, _ := ps.GetPublisher(emitterID, "topic1")
		err := publisher.Publish(context.Background(), []byte("data"))
		if err == nil {
			t.Errorf("Publish() expected an error, got nil")
		}
		if err.Error() != "mock router publish error" {
			t.Errorf("Publish() error = %v, want 'mock router publish error'", err)
		}
	})

	t.Run("UnsubscribeMultipleTimes", func(t *testing.T) {
		router := newMockEventRouter()
		ps := NewPubSub(Config{
			Router: router.routeEvent,
			Topics: permittedTopics,
		})
		topic := "topic1"
		subscriber := newMockSubscriber("sub-multi-unsub")
		router.addTestSubscriber(topic, subscriber)

		unsub, err := ps.Subscribe(topic, subscriber)
		if err != nil {
			t.Fatalf("Subscribe() error = %v", err)
		}

		unsub()
		router.removeTestSubscriber(topic, subscriber)

		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Unsubscribe panic: %v", r)
				}
			}()
			unsub()
			unsub()
		}()

		publisher, _ := ps.GetPublisher(emitterID, topic)
		publisher.Publish(context.Background(), []byte("after multi unsub"))
		time.Sleep(50 * time.Millisecond)

		if len(subscriber.getMessages()) != 0 {
			t.Errorf("Subscriber received messages after multiple unsubscribes: %v", subscriber.getMessages())
		}
	})

	t.Run("SubscribeMultipleSubscribersToSameTopic", func(t *testing.T) {
		router := newMockEventRouter()
		ps := NewPubSub(Config{
			Router: router.routeEvent,
			Topics: permittedTopics,
		})
		topic := "topic2"
		subscriber1 := newMockSubscriber("subMulti1")
		subscriber2 := newMockSubscriber("subMulti2")

		router.addTestSubscriber(topic, subscriber1)
		router.addTestSubscriber(topic, subscriber2)

		unsub1, err1 := ps.Subscribe(topic, subscriber1)
		if err1 != nil {
			t.Fatalf("Subscribe (subscriber1) error = %v", err1)
		}
		unsub2, err2 := ps.Subscribe(topic, subscriber2)
		if err2 != nil {
			t.Fatalf("Subscribe (subscriber2) error = %v", err2)
		}

		publisher, _ := ps.GetPublisher(emitterID, topic)
		testData := []byte("multi sub test")
		publisher.Publish(context.Background(), testData)
		time.Sleep(50 * time.Millisecond)

		msgs1 := subscriber1.getMessages()
		msgs2 := subscriber2.getMessages()

		if len(msgs1) != 1 || !slices.Equal(msgs1[0].Data, testData) || msgs1[0].Topic != topic || msgs1[0].EventID == "" {
			t.Errorf("Subscriber1 did not receive the correct event. Got: %+v, want data '%s', topic '%s', non-empty EventID", msgs1[0], testData, topic)
		}
		if len(msgs2) != 1 || !slices.Equal(msgs2[0].Data, testData) || msgs2[0].Topic != topic || msgs2[0].EventID == "" {
			t.Errorf("Subscriber2 did not receive the correct event. Got: %+v, want data '%s', topic '%s', non-empty EventID", msgs2[0], testData, topic)
		}

		unsub1()
		router.removeTestSubscriber(topic, subscriber1)

		testData2 := []byte("after unsub one")
		publisher.Publish(context.Background(), testData2)
		time.Sleep(50 * time.Millisecond)

		msgs1AfterUnsub := subscriber1.getMessages()
		msgs2AfterUnsub := subscriber2.getMessages()

		if len(msgs1AfterUnsub) != 1 {
			t.Errorf("Subscriber1 (unsubscribed) received new message. Msgs: %v", msgs1AfterUnsub)
		}
		if len(msgs2AfterUnsub) != 2 || !slices.Equal(msgs2AfterUnsub[1].Data, testData2) || msgs2AfterUnsub[1].Topic != topic || msgs2AfterUnsub[1].EventID == "" {
			t.Errorf("Subscriber2 did not receive the correct second event. Got: %+v, want data '%s', topic '%s', non-empty EventID", msgs2AfterUnsub[1], testData2, topic)
		}

		unsub2()
		router.removeTestSubscriber(topic, subscriber2)
	})

	t.Run("GetPublisherForNonPermittedTopic", func(t *testing.T) {
		router := newMockEventRouter()
		ps := NewPubSub(Config{
			Router: router.routeEvent,
			Topics: []string{"only_this_topic"},
		})

		nonPermittedTopic := "absolutely_not_allowed"
		publisher, err := ps.GetPublisher(emitterID, nonPermittedTopic)
		if err != nil {
			t.Fatalf("GetPublisher() for non-permitted topic returned error = %v, want nil (based on current impl)", err)
		}
		if publisher == nil {
			t.Fatal("GetPublisher() for non-permitted topic returned nil publisher, want non-nil")
		}

		testData := []byte("sneaky publish")
		err = publisher.Publish(context.Background(), testData)
		if err != nil {
			t.Fatalf("Publish() to a publisher for a non-permitted topic failed: %v", err)
		}

		publishedEvents := router.getPublishedEvents()
		if len(publishedEvents) != 1 {
			t.Fatalf("Expected 1 event published by router, got %d", len(publishedEvents))
		}
		if publishedEvents[0].Topic != nonPermittedTopic {
			t.Errorf("Event topic = %s, want %s", publishedEvents[0].Topic, nonPermittedTopic)
		}
		if publishedEvents[0].EventID == "" {
			t.Error("Published event EventID should not be empty")
		}
	})

	t.Run("ConcurrentSubscribeAndPublish", func(t *testing.T) {
		router := newMockEventRouter()
		router.publishDelay = 1 * time.Millisecond
		ps := NewPubSub(Config{
			Router: router.routeEvent,
			Topics: []string{"shared_topic"},
		})
		topic := "shared_topic"
		numGoroutines := 10
		numMessagesPerGoroutine := 5
		var wg sync.WaitGroup

		subscriber := newMockSubscriber("concurrent-sub")
		router.addTestSubscriber(topic, subscriber)
		unsub, err := ps.Subscribe(topic, subscriber)
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}
		defer unsub()
		defer router.removeTestSubscriber(topic, subscriber)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(gID int) {
				defer wg.Done()
				pub, err := ps.GetPublisher(fmt.Sprintf("emitter-%d", gID), topic)
				if err != nil {
					t.Errorf("Goroutine %d: failed to get publisher: %v", gID, err)
					return
				}
				for j := 0; j < numMessagesPerGoroutine; j++ {
					data := []byte(fmt.Sprintf("msg from g%d m%d", gID, j))
					if err := pub.Publish(context.Background(), data); err != nil {
						fmt.Printf("Goroutine %d: error publishing: %v\n", gID, err)
					}
				}
			}(i)
		}

		wg.Wait()
		time.Sleep(100 * time.Millisecond)

		expectedMessages := numGoroutines * numMessagesPerGoroutine
		receivedMessages := subscriber.getMessages()

		if len(receivedMessages) != expectedMessages {
			t.Errorf("Expected %d messages, got %d", expectedMessages, len(receivedMessages))
		}

		// Basic check on received messages (all should have EventIDs and correct topic)
		for _, evt := range receivedMessages {
			if evt.EventID == "" {
				t.Errorf("Received event with empty EventID: %+v", evt)
			}
			if evt.Topic != topic {
				t.Errorf("Received event with wrong topic. Expected '%s', got '%s': %+v", topic, evt.Topic, evt)
			}
		}
	})
}
