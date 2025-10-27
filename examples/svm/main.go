package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/svm"
)

/*
	KV Backend
*/

type DemoKV struct {
	data  map[string]string
	scope string
	mu    sync.RWMutex
}

var _ fwi.KV = &DemoKV{}

func NewDemoKV() *DemoKV {
	return &DemoKV{
		data: make(map[string]string),
	}
}

func (d *DemoKV) assembleKey(key string) string {
	if d.scope == "" {
		return key
	}
	return d.scope + "." + key
}

func (d *DemoKV) Get(ctx context.Context, key string) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	fullKey := d.assembleKey(key)
	if value, exists := d.data[fullKey]; exists {
		return value, nil
	}
	return "", fmt.Errorf("key not found")
}

func (d *DemoKV) Set(ctx context.Context, key string, value string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data[d.assembleKey(key)] = value
	return nil
}

func (d *DemoKV) IterateKeys(ctx context.Context, prefix string, offset, limit int) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	fullPrefix := d.assembleKey(prefix)
	var keys []string
	for key := range d.data {
		if strings.HasPrefix(key, fullPrefix) {
			keys = append(keys, strings.TrimPrefix(key, d.scope+"."))
		}
	}
	// Simple sort for consistency
	sort.Strings(keys)
	// Apply offset and limit
	if offset >= len(keys) {
		return []string{}, nil
	}
	end := offset + limit
	if end > len(keys) {
		end = len(keys)
	}
	return keys[offset:end], nil
}

func (d *DemoKV) Delete(ctx context.Context, key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.data, d.assembleKey(key))
	return nil
}

func (d *DemoKV) CompareAndSwap(ctx context.Context, key string, oldValue, newValue string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	fullKey := d.assembleKey(key)
	current, exists := d.data[fullKey]
	if !exists && oldValue != "" {
		return fmt.Errorf("key does not exist")
	}
	if exists && current != oldValue {
		return fmt.Errorf("value mismatch")
	}
	d.data[fullKey] = newValue
	return nil
}

func (d *DemoKV) SetNX(ctx context.Context, key string, value string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	fullKey := d.assembleKey(key)
	if _, exists := d.data[fullKey]; exists {
		return fmt.Errorf("key already exists")
	}
	d.data[fullKey] = value
	return nil
}

func (d *DemoKV) PushScope(scope string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.scope == "" {
		d.scope = scope
	} else {
		d.scope = d.scope + "." + scope
	}
}

func (d *DemoKV) PopScope() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if i := strings.LastIndex(d.scope, "."); i != -1 {
		d.scope = d.scope[:i]
	} else {
		d.scope = ""
	}
}

func (d *DemoKV) GetScope() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.scope
}

/*
	Event backend

	using function calls and maps we emulate the pub/sub concept with channels
	to ensure behavior is similar to real event system
*/

type DemoEvents struct {
	subscribers map[string][]func(data any)
	scope       string
	mu          sync.RWMutex
}

var _ fwi.Events = &DemoEvents{}

func NewDemoEvents() *DemoEvents {
	return &DemoEvents{
		subscribers: make(map[string][]func(data any)),
	}
}

func (d *DemoEvents) assembleTopic(topic string) string {
	if d.scope == "" {
		return topic
	}
	return d.scope + "." + topic
}

func (d *DemoEvents) Subscribe(ctx context.Context, topic string, onEvent func(data any)) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	fullTopic := d.assembleTopic(topic)
	d.subscribers[fullTopic] = append(d.subscribers[fullTopic], onEvent)
	return nil
}

func (d *DemoEvents) Publish(ctx context.Context, topic string, data any) error {
	d.mu.RLock()
	fullTopic := d.assembleTopic(topic)
	subs := make([]func(data any), len(d.subscribers[fullTopic]))
	copy(subs, d.subscribers[fullTopic])
	d.mu.RUnlock()

	// Publish to all subscribers asynchronously to avoid blocking
	for _, onEvent := range subs {
		go onEvent(data)
	}
	return nil
}

func (d *DemoEvents) PushScope(scope string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.scope == "" {
		d.scope = scope
	} else {
		d.scope = d.scope + "." + scope
	}
}

func (d *DemoEvents) PopScope() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if i := strings.LastIndex(d.scope, "."); i != -1 {
		d.scope = d.scope[:i]
	} else {
		d.scope = ""
	}
}

func (d *DemoEvents) GetScope() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.scope
}

func (d *DemoEvents) Purge(ctx context.Context) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	totalPurged := 0
	for topic, subs := range d.subscribers {
		totalPurged += len(subs)
		delete(d.subscribers, topic)
	}
	return totalPurged, nil
}

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	eventsBackend := NewDemoEvents()
	kvBackend := NewDemoKV()

	processor := svm.NewProcessor(
		"test-instance",
		logger,
		kvBackend,
		eventsBackend,
	)

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Info("Starting SVM processor demo")

	// Example: Set up event subscriptions
	eventsBackend.Subscribe(ctx, "demo-topic", func(data any) {
		logger.Info("Received event", "topic", "demo-topic", "data", data)
	})

	// Example: Demonstrate KV operations
	logger.Info("Demonstrating KV operations...")
	kvBackend.Set(ctx, "demo-key", "demo-value")
	if value, err := kvBackend.Get(ctx, "demo-key"); err == nil {
		logger.Info("KV get/set", "key", "demo-key", "value", value)
	}

	// Test SetNX (set if not exists)
	if err := kvBackend.SetNX(ctx, "new-key", "new-value"); err == nil {
		logger.Info("SetNX succeeded for new key")
	}
	if err := kvBackend.SetNX(ctx, "demo-key", "should-fail"); err != nil {
		logger.Info("SetNX correctly failed for existing key", "error", err)
	}

	// Test scoping
	kvBackend.PushScope("scope1")
	kvBackend.Set(ctx, "scoped-key", "scoped-value")
	if value, err := kvBackend.Get(ctx, "scoped-key"); err == nil {
		logger.Info("Scoped KV operation", "scope", kvBackend.GetScope(), "key", "scoped-key", "value", value)
	}
	kvBackend.PopScope()

	// Test iteration
	kvBackend.Set(ctx, "iter-key1", "value1")
	kvBackend.Set(ctx, "iter-key2", "value2")
	if keys, err := kvBackend.IterateKeys(ctx, "", 0, 10); err == nil {
		logger.Info("KV iteration", "keys", keys)
	}

	// Publish a demo event
	logger.Info("Publishing demo event...")
	eventsBackend.Publish(ctx, "demo-topic", "Hello from demo!")

	// Give async event handlers time to complete
	time.Sleep(100 * time.Millisecond)

	logger.Info("Demo setup complete, starting processor...")

	if err := processor.Run(ctx); err != nil {
		logger.Error("failed to run processor", "error", err)
		os.Exit(1)
	}

	logger.Info("Processor completed successfully")
}
