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

// mockImplSubscriber is a TopicSubscriber for implementation tests.
type mockImplSubscriber struct {
	mu       sync.Mutex
	messages []Event
	ID       string // For identifying subscribers in tests
}

func newMockImplSubscriber(id string) *mockImplSubscriber {
	return &mockImplSubscriber{
		ID:       id,
		messages: make([]Event, 0),
	}
}

func (ms *mockImplSubscriber) OnMessage(ctx context.Context, event Event) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.messages = append(ms.messages, event)
}

func (ms *mockImplSubscriber) getMessages() []Event {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// Return a copy
	msgsCopy := make([]Event, len(ms.messages))
	copy(msgsCopy, ms.messages)
	return msgsCopy
}

// mockImplEventRouter is an EventRouter for implementation tests.
// It allows tests to inspect passed events and control routing behavior.
type mockImplEventRouter struct {
	mu              sync.RWMutex
	publishedEvents []Event
	simulateError   bool
	// For this implementation test, the router needs to know about subscribers
	// to simulate actual event delivery to them.
	testSubscribers map[string][]*mockImplSubscriber
}

func newMockImplEventRouter() *mockImplEventRouter {
	return &mockImplEventRouter{
		publishedEvents: make([]Event, 0),
		testSubscribers: make(map[string][]*mockImplSubscriber),
	}
}

// routeEvent is the function that implements EventRouter for the mock.
func (mer *mockImplEventRouter) routeEvent(ctx context.Context, event Event) error {
	mer.mu.Lock()
	defer mer.mu.Unlock()

	if mer.simulateError {
		return errors.New("mock router publish error")
	}
	mer.publishedEvents = append(mer.publishedEvents, event)

	// Simulate dispatch to subscribers known to this mock router for the event's topic
	if subs, ok := mer.testSubscribers[event.Topic]; ok {
		for _, s := range subs {
			// Dispatch in a goroutine to mimic real-world async dispatch
			// and to avoid blocking the publisher if a subscriber is slow.
			go s.OnMessage(ctx, event)
		}
	}
	return nil
}

// addTestSubscriber lets the test register a mock subscriber with the mock router directly.
func (mer *mockImplEventRouter) addTestSubscriber(topic string, sub *mockImplSubscriber) {
	mer.mu.Lock()
	defer mer.mu.Unlock()
	mer.testSubscribers[topic] = append(mer.testSubscribers[topic], sub)
}

// removeTestSubscriber removes a mock subscriber from the mock router.
func (mer *mockImplEventRouter) removeTestSubscriber(topic string, sub *mockImplSubscriber) {
	mer.mu.Lock()
	defer mer.mu.Unlock()
	if subs, ok := mer.testSubscribers[topic]; ok {
		mer.testSubscribers[topic] = slices.DeleteFunc(subs, func(s *mockImplSubscriber) bool {
			return s.ID == sub.ID // Assuming ID is unique for mock subscribers
		})
	}
}

func (mer *mockImplEventRouter) getPublishedEvents() []Event {
	mer.mu.RLock()
	defer mer.mu.RUnlock()
	eventsCopy := make([]Event, len(mer.publishedEvents))
	copy(eventsCopy, mer.publishedEvents)
	return eventsCopy
}

func TestPubSubImpl_GetPermittedTopics(t *testing.T) {
	router := newMockImplEventRouter()
	definedTopics := []string{"topicA", "topicB"}
	ps := &pubSubImpl{
		permittedTopics: definedTopics,
		router:          router.routeEvent,
		topics:          make(map[string]TopicPublisher),
		subscribers:     make(map[string][]TopicSubscriber),
	}

	topics, err := ps.GetPermittedTopics()
	if err != nil {
		t.Fatalf("GetPermittedTopics() error = %v, wantErr nil", err)
	}
	if !slices.Equal(topics, definedTopics) {
		t.Errorf("GetPermittedTopics() got = %v, want %v", topics, definedTopics)
	}
}

func TestPubSubImpl_GetPublisher(t *testing.T) {
	router := newMockImplEventRouter()
	ps := &pubSubImpl{
		permittedTopics: []string{"topic1"},
		router:          router.routeEvent,
		topics:          make(map[string]TopicPublisher),
		subscribers:     make(map[string][]TopicSubscriber),
	}

	emitter := "emitter-001"
	topic := "topic1"

	publisher, err := ps.GetPublisher(emitter, topic)
	if err != nil {
		t.Fatalf("GetPublisher() error = %v, wantErr nil", err)
	}

	tpImpl, ok := publisher.(*topicPublisherImpl)
	if !ok {
		t.Fatalf("GetPublisher() did not return *topicPublisherImpl, got %T", publisher)
	}

	if tpImpl.emitterId != emitter {
		t.Errorf("publisher.emitterId got = %s, want %s", tpImpl.emitterId, emitter)
	}
	if tpImpl.topic != topic {
		t.Errorf("publisher.topic got = %s, want %s", tpImpl.topic, topic)
	}
	// Cannot directly compare functions, but ensure it's not nil
	if tpImpl.router == nil {
		t.Error("publisher.router is nil")
	}
}

func TestTopicPublisherImpl_Publish(t *testing.T) {
	mockRouter := newMockImplEventRouter()
	emitter := "emitter-pub-test"
	topic := "pub-topic"

	tp := &topicPublisherImpl{
		emitterId: emitter,
		topic:     topic,
		router:    mockRouter.routeEvent,
	}

	testData := []byte("event data payload")
	ctx := context.Background()

	if err := tp.Publish(ctx, testData); err != nil {
		t.Fatalf("Publish() error = %v, wantErr nil", err)
	}

	publishedEvents := mockRouter.getPublishedEvents()
	if len(publishedEvents) != 1 {
		t.Fatalf("expected 1 event published to router, got %d", len(publishedEvents))
	}

	event := publishedEvents[0]
	if event.Topic != topic {
		t.Errorf("event.Topic got = %s, want %s", event.Topic, topic)
	}
	if event.Emitter != emitter {
		t.Errorf("event.Emitter got = %s, want %s", event.Emitter, emitter)
	}
	if !slices.Equal(event.Data, testData) {
		t.Errorf("event.Data got = %s, want %s", string(event.Data), string(testData))
	}
	if event.EventID == "" {
		t.Error("event.EventID is empty, should be a UUID")
	}
	if _, err := uuid.Parse(event.EventID); err != nil {
		t.Errorf("event.EventID is not a valid UUID: %v", err)
	}
	if event.EmittedAt.IsZero() {
		t.Error("event.EmittedAt is zero")
	}

	// Test publish error from router
	mockRouter.simulateError = true
	expectedErr := "mock router publish error"
	err := tp.Publish(ctx, []byte("another message"))
	if err == nil || err.Error() != expectedErr {
		t.Errorf("Publish() with router error got = %v, want error string '%s'", err, expectedErr)
	}
}

func TestPubSubImpl_Subscribe_Unsubscribe(t *testing.T) {
	mockRouter := newMockImplEventRouter()
	permitted := []string{"allowed_topic", "another_topic"}
	ps := &pubSubImpl{
		permittedTopics:  permitted,
		router:           mockRouter.routeEvent,
		topics:           make(map[string]TopicPublisher),
		subscribers:      make(map[string][]TopicSubscriber),
		subscribersMutex: sync.RWMutex{},
	}

	subscriberID := "sub-impl-1"
	mockSub := newMockImplSubscriber(subscriberID)
	allowedTopic := "allowed_topic"

	// The mock router needs to know about this subscriber to dispatch messages to it.
	mockRouter.addTestSubscriber(allowedTopic, mockSub)

	unsub, err := ps.Subscribe(allowedTopic, mockSub)
	if err != nil {
		t.Fatalf("Subscribe() to permitted topic error = %v, wantErr nil", err)
	}
	if unsub == nil {
		t.Fatal("Subscribe() returned nil Unsubscriber")
	}

	// Check internal state (if possible without exporting, otherwise infer by behavior)
	ps.subscribersMutex.RLock()
	if len(ps.subscribers[allowedTopic]) != 1 || ps.subscribers[allowedTopic][0] != mockSub {
		ps.subscribersMutex.RUnlock()
		t.Fatalf("subscriber not correctly added to internal map. Got: %v", ps.subscribers[allowedTopic])
	}
	ps.subscribersMutex.RUnlock()

	// Publish a message and check if subscriber receives it
	publisher, _ := ps.GetPublisher("emitter-sub-test", allowedTopic)
	testMsg1 := []byte("message for subscriber")
	if err := publisher.Publish(context.Background(), testMsg1); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // Allow time for async dispatch via goroutine in mock router

	received := mockSub.getMessages()
	if len(received) != 1 {
		t.Fatalf("subscriber expected 1 message, got %d", len(received))
	}
	if !slices.Equal(received[0].Data, testMsg1) {
		t.Errorf("subscriber received data = %s, want %s", string(received[0].Data), string(testMsg1))
	}
	if received[0].Topic != allowedTopic {
		t.Errorf("subscriber received event with topic %s, want %s", received[0].Topic, allowedTopic)
	}

	// Unsubscribe
	unsub()

	// Check internal state after unsubscribe
	ps.subscribersMutex.RLock()
	if len(ps.subscribers[allowedTopic]) != 0 {
		ps.subscribersMutex.RUnlock()
		t.Errorf("subscriber map for topic %s not empty after unsubscribe. Got: %v", allowedTopic, ps.subscribers[allowedTopic])
	}
	ps.subscribersMutex.RUnlock()

	// Critical for this test setup: remove from mock router as well so it doesn't try to send to a de-registered sub
	mockRouter.removeTestSubscriber(allowedTopic, mockSub)

	// Publish another message, subscriber should not receive it
	testMsg2 := []byte("message post-unsubscribe")
	if err := publisher.Publish(context.Background(), testMsg2); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	receivedAfterUnsub := mockSub.getMessages() // should still be the first one only
	if len(receivedAfterUnsub) != 1 {
		t.Errorf("subscriber received %d messages after unsubscribe, expected 1 (the original). Messages: %v", len(receivedAfterUnsub), receivedAfterUnsub)
	}

	// Test subscribing to non-permitted topic
	notAllowedTopic := "forbidden_topic"
	mockSub2 := newMockImplSubscriber("sub-impl-2")
	_, err = ps.Subscribe(notAllowedTopic, mockSub2)
	if !errors.Is(err, ErrTopicNotPermitted) {
		t.Errorf("Subscribe() to non-permitted topic error = %v, want %v", err, ErrTopicNotPermitted)
	}

	// Test Unsubscribe multiple times (should not panic)
	unsub() // Call again
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Unsubscribe panic on multiple calls: %v", r)
			}
		}()
		unsub()
	}()
}

func TestPubSubImpl_Subscribe_Concurrent(t *testing.T) {
	mockRouter := newMockImplEventRouter()
	ps := &pubSubImpl{
		permittedTopics:  []string{"concurrent_topic"},
		router:           mockRouter.routeEvent,
		topics:           make(map[string]TopicPublisher),
		subscribers:      make(map[string][]TopicSubscriber),
		subscribersMutex: sync.RWMutex{},
	}

	topic := "concurrent_topic"
	numGoroutines := 20
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			subID := fmt.Sprintf("concurrent-sub-%d", id)
			mockSub := newMockImplSubscriber(subID)

			// Add to mock router to enable message receipt for verification, if needed by a test variant.
			// For this specific concurrency test of Subscribe/Unsubscribe, we mainly care about race conditions
			// on the ps.subscribers map, not necessarily message delivery through the mockRouter.
			mockRouter.addTestSubscriber(topic, mockSub) // Optional for this specific test's focus

			unsub, err := ps.Subscribe(topic, mockSub)
			if err != nil {
				t.Errorf("Goroutine %d: Subscribe() error = %v", id, err)
				return
			}
			// Simulate some work or hold time
			time.Sleep(time.Duration(id%5) * time.Millisecond)
			unsub()

			// Also remove from mockRouter if it was added
			mockRouter.removeTestSubscriber(topic, mockSub) // Optional for this specific test's focus
		}(i)
	}

	wg.Wait()

	// After all goroutines complete, the subscribers map for the topic should be empty.
	ps.subscribersMutex.RLock()
	defer ps.subscribersMutex.RUnlock()
	if len(ps.subscribers[topic]) != 0 {
		t.Errorf("Expected subscriber map for topic '%s' to be empty after concurrent unsubscribes, got %d subs: %v", topic, len(ps.subscribers[topic]), ps.subscribers[topic])
		// For debugging, print the subscribers if any remain
		// for _, sub := range ps.subscribers[topic] {
		// 	mockImplSub, ok := sub.(*mockImplSubscriber)
		// 	if ok {
		// 		t.Logf("Remaining subscriber: ID %s", mockImplSub.ID)
		// 	}
		// }
	}
}

// TestPubSubImpl_Subscribe_MultipleSubscribers tests multiple subscribers on the same topic.
func TestPubSubImpl_Subscribe_MultipleSubscribers(t *testing.T) {
	mockRouter := newMockImplEventRouter()
	ps := &pubSubImpl{
		permittedTopics:  []string{"multi_sub_topic"},
		router:           mockRouter.routeEvent,
		topics:           make(map[string]TopicPublisher),
		subscribers:      make(map[string][]TopicSubscriber),
		subscribersMutex: sync.RWMutex{},
	}
	topic := "multi_sub_topic"

	mockSub1 := newMockImplSubscriber("multi-1")
	mockSub2 := newMockImplSubscriber("multi-2")

	mockRouter.addTestSubscriber(topic, mockSub1)
	mockRouter.addTestSubscriber(topic, mockSub2)

	unsub1, err1 := ps.Subscribe(topic, mockSub1)
	if err1 != nil {
		t.Fatalf("Subscribe for sub1 failed: %v", err1)
	}
	unsub2, err2 := ps.Subscribe(topic, mockSub2)
	if err2 != nil {
		t.Fatalf("Subscribe for sub2 failed: %v", err2)
	}

	publisher, _ := ps.GetPublisher("emitter-multi", topic)
	msgData := []byte("message for all")
	publisher.Publish(context.Background(), msgData)

	time.Sleep(50 * time.Millisecond)

	msgs1 := mockSub1.getMessages()
	msgs2 := mockSub2.getMessages()

	if len(msgs1) != 1 || !slices.Equal(msgs1[0].Data, msgData) {
		t.Errorf("mockSub1 expected 1 message '%s', got %d messages: %v", string(msgData), len(msgs1), msgs1)
	}
	if len(msgs2) != 1 || !slices.Equal(msgs2[0].Data, msgData) {
		t.Errorf("mockSub2 expected 1 message '%s', got %d messages: %v", string(msgData), len(msgs2), msgs2)
	}

	// Unsubscribe sub1
	unsub1()
	mockRouter.removeTestSubscriber(topic, mockSub1)

	msgData2 := []byte("message for sub2 only")
	publisher.Publish(context.Background(), msgData2)
	time.Sleep(50 * time.Millisecond)

	msgs1AfterUnsub := mockSub1.getMessages() // Should still be 1
	msgs2AfterUnsub := mockSub2.getMessages() // Should be 2

	if len(msgs1AfterUnsub) != 1 {
		t.Errorf("mockSub1 (unsubscribed) got %d messages, expected 1. Msgs: %v", len(msgs1AfterUnsub), msgs1AfterUnsub)
	}
	if len(msgs2AfterUnsub) != 2 || !slices.Equal(msgs2AfterUnsub[1].Data, msgData2) {
		t.Errorf("mockSub2 expected 2nd message '%s', got %d messages. Msgs: %v", string(msgData2), len(msgs2AfterUnsub), msgs2AfterUnsub)
	}

	unsub2()
	mockRouter.removeTestSubscriber(topic, mockSub2)
}
