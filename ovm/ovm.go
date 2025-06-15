package ovm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/db/models"
	"github.com/robertkrimen/otto"
)

const (
	colorReset    = "[0m"
	colorHiRed    = "[91m"
	colorHiGreen  = "[92m"
	colorHiYellow = "[93m"
	colorHiCyan   = "[96m"
)

type Config struct {
	Logger     *slog.Logger
	SetupCtx   context.Context
	InsiClient *client.Client
	ScriptArgs []string

	DoAddAdmin   bool
	DoAddConsole bool
	DoAddOS      bool
	DoAddTest    bool
}

type eventCollector struct {
	topic  string
	events []any // Store the 'data' part of the event
	lock   sync.Mutex
	cancel context.CancelFunc // To stop the subscription goroutine
}

type OVM struct {
	logger     *slog.Logger
	vm         *otto.Otto
	insiClient *client.Client
	testDir    string
	config     *Config

	// subscription management
	subscriptionCtx    context.Context
	subscriptionCancel context.CancelFunc
	collectors         map[string]*eventCollector
	collectorsLock     sync.RWMutex
	rateLimitRetries   atomic.Uint64
}

func New(cfg *Config) (*OVM, error) {
	subCtx, subCancel := context.WithCancel(context.Background())
	ovm := &OVM{
		logger:             cfg.Logger,
		vm:                 otto.New(),
		insiClient:         cfg.InsiClient,
		subscriptionCtx:    subCtx,
		subscriptionCancel: subCancel,
		collectors:         make(map[string]*eventCollector),
		config:             cfg,
	}

	if err := ovm.addValueStore(cfg.SetupCtx); err != nil {
		return nil, err
	}
	if err := ovm.addCacheStore(cfg.SetupCtx); err != nil {
		return nil, err
	}
	if err := ovm.addEventEmitter(cfg.SetupCtx); err != nil {
		return nil, err
	}
	if err := ovm.addSubscriptions(cfg.SetupCtx); err != nil {
		return nil, err
	}
	if cfg.DoAddConsole {
		if err := ovm.addConsole(cfg.SetupCtx); err != nil {
			return nil, err
		}
	}
	if cfg.DoAddOS {
		if err := ovm.addOS(cfg.SetupCtx); err != nil {
			return nil, err
		}
	}
	if err := ovm.addTime(cfg.SetupCtx); err != nil {
		return nil, err
	}
	if err := ovm.addArgs(cfg.SetupCtx); err != nil {
		return nil, err
	}
	if err := ovm.addOVM(cfg.SetupCtx); err != nil {
		return nil, err
	}
	if cfg.DoAddAdmin {
		if err := ovm.addAdmin(cfg.SetupCtx); err != nil {
			return nil, err
		}
	}
	if cfg.DoAddTest {
		if err := ovm.addTest(cfg.SetupCtx); err != nil {
			return nil, err
		}
	}

	if err := ovm.addObject(cfg.SetupCtx); err != nil {
		return nil, err
	}

	return ovm, nil
}

func (o *OVM) Close() {
	if o.subscriptionCancel != nil {
		o.logger.Debug("Closing OVM and all active subscriptions")
		o.subscriptionCancel()
	}
}

func (o *OVM) Execute(ctx context.Context, code string) error {
	// NOTE: we prefer panics to we can leverage try/catch in the vm
	_, err := o.vm.Run(code)
	if err != nil {
		return fmt.Errorf("error executing script: %w", err)
	}
	return nil
}

func (o *OVM) addConsole(ctx context.Context) error {
	console, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create console object: %w", err)
	}

	logFn := func(level slog.Level) func(otto.FunctionCall) otto.Value {
		return func(call otto.FunctionCall) otto.Value {
			var sb strings.Builder
			for i, arg := range call.ArgumentList {
				if i > 0 {
					sb.WriteString(" ")
				}
				sb.WriteString(arg.String())
			}
			o.logger.Log(ctx, level, sb.String())
			return otto.Value{}
		}
	}

	if err := console.Set("log", logFn(slog.LevelInfo)); err != nil {
		return err
	}
	if err := console.Set("info", logFn(slog.LevelInfo)); err != nil {
		return err
	}
	if err := console.Set("debug", logFn(slog.LevelDebug)); err != nil {
		return err
	}
	if err := console.Set("warn", logFn(slog.LevelWarn)); err != nil {
		return err
	}
	if err := console.Set("error", logFn(slog.LevelError)); err != nil {
		return err
	}

	return o.vm.Set("console", console)
}

// withRetries is a generic helper that wraps a function call, adding a retry mechanism
// specifically for rate-limiting errors.
func withRetries[R any](o *OVM, ctx context.Context, fn func() (R, error)) (R, error) {
	for {
		result, err := fn()
		if err == nil {
			return result, nil // Success
		}

		var rateLimitErr *client.ErrRateLimited
		if errors.As(err, &rateLimitErr) {
			o.logger.Warn("OVM operation rate limited, sleeping", "duration", rateLimitErr.RetryAfter)
			o.rateLimitRetries.Add(1)
			select {
			case <-time.After(rateLimitErr.RetryAfter):
				o.logger.Debug("Finished rate limit sleep, retrying operation.")
				continue // Slept, continue to retry
			case <-ctx.Done():
				o.logger.Error("Context cancelled during rate limit sleep", "error", ctx.Err())
				var zero R
				return zero, fmt.Errorf("operation cancelled during rate limit sleep: %w", ctx.Err())
			}
		}

		var diskLimitErr *client.ErrDiskLimitExceeded
		if errors.As(err, &diskLimitErr) {
			panic(o.vm.MakeCustomError("DiskLimitError", diskLimitErr.Error()))
		}

		var memoryLimitErr *client.ErrMemoryLimitExceeded
		if errors.As(err, &memoryLimitErr) {
			panic(o.vm.MakeCustomError("MemoryLimitError", memoryLimitErr.Error()))
		}

		var eventsLimitErr *client.ErrEventsLimitExceeded
		if errors.As(err, &eventsLimitErr) {
			panic(o.vm.MakeCustomError("EventsLimitError", eventsLimitErr.Error()))
		}

		// It's some other error, return it.
		var zero R
		return zero, err
	}
}

func (o *OVM) addValueStore(ctx context.Context) error {
	vs, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create vs object: %w", err)
	}

	// Set
	vs.Set("set", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		value, _ := call.Argument(1).ToString()
		if key == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "key cannot be empty"))
		}
		_, err := withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.Set(key, value)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		return otto.Value{}
	})

	// Delete
	vs.Set("delete", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		if key == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "key cannot be empty"))
		}
		_, err := withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.Delete(key)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		return otto.Value{}
	})

	// Get
	vs.Set("get", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		if key == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "key cannot be empty"))
		}
		value, err := withRetries(o, ctx, func() (string, error) {
			return o.insiClient.Get(key)
		})
		if err != nil {
			if errors.Is(err, client.ErrKeyNotFound) {
				return otto.NullValue()
			}
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(value)
		return val
	})

	// IterateByPrefix
	vs.Set("iterateByPrefix", func(call otto.FunctionCall) otto.Value {
		prefix, _ := call.Argument(0).ToString()
		offset, _ := call.Argument(1).ToInteger()
		limit, _ := call.Argument(2).ToInteger()
		if prefix == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "prefix cannot be empty"))
		}
		keys, err := withRetries(o, ctx, func() ([]string, error) {
			return o.insiClient.IterateByPrefix(prefix, int(offset), int(limit))
		})
		if err != nil {
			if errors.Is(err, client.ErrKeyNotFound) {
				val, _ := o.vm.ToValue([]string{})
				return val
			}
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(keys)
		return val
	})

	// SetNX
	vs.Set("setnx", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		value, _ := call.Argument(1).ToString()
		if key == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "key cannot be empty"))
		}
		_, err := withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.SetNX(key, value)
		})
		if err != nil {
			if errors.Is(err, client.ErrConflict) {
				val, _ := o.vm.ToValue(false)
				return val
			}
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(true)
		return val
	})

	// CompareAndSwap (cas)
	vs.Set("cas", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		oldValue, _ := call.Argument(1).ToString()
		newValue, _ := call.Argument(2).ToString()
		if key == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "key cannot be empty"))
		}
		_, err := withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.CompareAndSwap(key, oldValue, newValue)
		})
		if err != nil {
			if errors.Is(err, client.ErrConflict) {
				val, _ := o.vm.ToValue(false)
				return val
			}
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(true)
		return val
	})

	return o.vm.Set("vs", vs)
}

func (o *OVM) addCacheStore(ctx context.Context) error {
	cache, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create cache object: %w", err)
	}

	// Set
	cache.Set("set", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		value, _ := call.Argument(1).ToString()
		if key == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "key cannot be empty"))
		}
		_, err := withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.SetCache(key, value)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		return otto.Value{}
	})

	// Delete
	cache.Set("delete", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		if key == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "key cannot be empty"))
		}
		_, err := withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.DeleteCache(key)
		})
		if err != nil {
			if errors.Is(err, client.ErrKeyNotFound) {
				return otto.Value{} // Deleting non-existent key is not an error
			}
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		return otto.Value{}
	})

	// Get
	cache.Set("get", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		if key == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "key cannot be empty"))
		}
		value, err := withRetries(o, ctx, func() (string, error) {
			return o.insiClient.GetCache(key)
		})
		if err != nil {
			if errors.Is(err, client.ErrKeyNotFound) {
				return otto.NullValue()
			}
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(value)
		return val
	})

	// IterateByPrefix
	cache.Set("iterateByPrefix", func(call otto.FunctionCall) otto.Value {
		prefix, _ := call.Argument(0).ToString()
		offset, _ := call.Argument(1).ToInteger()
		limit, _ := call.Argument(2).ToInteger()
		if prefix == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "prefix cannot be empty"))
		}
		keys, err := withRetries(o, ctx, func() ([]string, error) {
			return o.insiClient.IterateCacheByPrefix(prefix, int(offset), int(limit))
		})
		if err != nil {
			if errors.Is(err, client.ErrKeyNotFound) {
				val, _ := o.vm.ToValue([]string{})
				return val
			}
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(keys)
		return val
	})

	// SetNX
	cache.Set("setnx", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		value, _ := call.Argument(1).ToString()
		if key == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "key cannot be empty"))
		}
		_, err := withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.SetCacheNX(key, value)
		})
		if err != nil {
			if errors.Is(err, client.ErrConflict) {
				val, _ := o.vm.ToValue(false)
				return val
			}
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(true)
		return val
	})

	// CompareAndSwap (cas)
	cache.Set("cas", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		oldValue, _ := call.Argument(1).ToString()
		newValue, _ := call.Argument(2).ToString()
		if key == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "key cannot be empty"))
		}
		_, err := withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.CompareAndSwapCache(key, oldValue, newValue)
		})
		if err != nil {
			if errors.Is(err, client.ErrConflict) {
				val, _ := o.vm.ToValue(false)
				return val
			}
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(true)
		return val
	})

	return o.vm.Set("cache", cache)
}

func (o *OVM) addEventEmitter(ctx context.Context) error {
	return o.vm.Set("emit", func(call otto.FunctionCall) otto.Value {
		topic, err := call.Argument(0).ToString()
		if err != nil {
			panic(o.vm.MakeCustomError("ArgumentError", "topic must be a string"))
		}
		if topic == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "topic cannot be empty"))
		}

		data, err := call.Argument(1).Export()
		if err != nil {
			panic(o.vm.MakeCustomError("ArgumentError", fmt.Sprintf("failed to export data: %v", err)))
		}

		_, err = withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.PublishEvent(topic, data)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}

		return otto.Value{}
	})
}

func (o *OVM) addSubscriptions(ctx context.Context) error {
	subsObj, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create subscriptions object: %w", err)
	}

	// subscribe("topic")
	subsObj.Set("subscribe", func(call otto.FunctionCall) otto.Value {
		topic, _ := call.Argument(0).ToString()
		if topic == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "topic cannot be empty"))
		}

		o.collectorsLock.Lock()
		defer o.collectorsLock.Unlock()

		if _, exists := o.collectors[topic]; exists {
			// Already subscribed, this is a no-op
			o.logger.Debug("Subscription already exists for topic", "topic", topic)
			return otto.Value{}
		}

		collectorCtx, collectorCancel := context.WithCancel(o.subscriptionCtx)

		collector := &eventCollector{
			topic:  topic,
			events: make([]any, 0),
			cancel: collectorCancel,
		}
		o.collectors[topic] = collector

		go func() {
			o.logger.Info("Starting OVM event subscription", "topic", topic)
			onEvent := func(data any) {
				collector.lock.Lock()
				collector.events = append(collector.events, data)
				collector.lock.Unlock()
				o.logger.Debug("OVM event collector received event", "topic", topic)
			}

			err := o.insiClient.SubscribeToEvents(topic, collectorCtx, onEvent)
			if err != nil && !errors.Is(err, context.Canceled) {
				o.logger.Error("OVM event subscription failed", "topic", topic, "error", err)
			}
			o.logger.Info("OVM event subscription stopped", "topic", topic)

			// Clean up collector if subscription ends unexpectedly
			o.collectorsLock.Lock()
			delete(o.collectors, topic)
			o.collectorsLock.Unlock()
		}()

		return otto.Value{}
	})

	// poll("topic", max_events) -> returns array of events and consumes them from buffer
	subsObj.Set("poll", func(call otto.FunctionCall) otto.Value {
		topic, _ := call.Argument(0).ToString()
		max, _ := call.Argument(1).ToInteger()

		if topic == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "topic cannot be empty"))
		}

		o.collectorsLock.RLock()
		collector, exists := o.collectors[topic]
		o.collectorsLock.RUnlock()

		if !exists {
			// Not subscribed, return empty array.
			val, _ := o.vm.ToValue([]any{})
			return val
		}

		collector.lock.Lock()
		defer collector.lock.Unlock()

		if len(collector.events) == 0 {
			val, _ := o.vm.ToValue([]any{})
			return val
		}

		count := int(max)
		if count <= 0 || count > len(collector.events) {
			count = len(collector.events)
		}

		eventsToReturn := make([]any, count)
		copy(eventsToReturn, collector.events[:count])
		collector.events = collector.events[count:] // Consume them

		val, err := o.vm.ToValue(eventsToReturn)
		if err != nil {
			panic(o.vm.MakeCustomError("InternalError", "failed to convert events to JS value: "+err.Error()))
		}
		return val
	})

	// unsubscribe("topic")
	subsObj.Set("unsubscribe", func(call otto.FunctionCall) otto.Value {
		topic, _ := call.Argument(0).ToString()
		if topic == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "topic cannot be empty"))
		}

		o.collectorsLock.Lock()
		defer o.collectorsLock.Unlock()

		collector, exists := o.collectors[topic]
		if !exists {
			return otto.Value{}
		}

		collector.cancel()
		delete(o.collectors, topic)

		return otto.Value{}
	})

	// clear("topic") -> clears event buffer for topic without unsubscribing
	subsObj.Set("clear", func(call otto.FunctionCall) otto.Value {
		topic, _ := call.Argument(0).ToString()
		if topic == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "topic cannot be empty"))
		}

		o.collectorsLock.RLock()
		collector, exists := o.collectors[topic]
		o.collectorsLock.RUnlock()

		if exists {
			collector.lock.Lock()
			collector.events = make([]any, 0)
			collector.lock.Unlock()
		}

		return otto.Value{}
	})

	return o.vm.Set("subscriptions", subsObj)
}

func (o *OVM) addOS(ctx context.Context) error {
	osObj, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create os object: %w", err)
	}

	osObj.Set("getenv", func(call otto.FunctionCall) otto.Value {
		key, _ := call.Argument(0).ToString()
		value := os.Getenv(key)
		val, _ := o.vm.ToValue(value)
		return val
	})

	return o.vm.Set("os", osObj)
}

func (o *OVM) addTime(ctx context.Context) error {
	timeObj, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create time object: %w", err)
	}

	timeObj.Set("stamp", func(call otto.FunctionCall) otto.Value {
		stamp := time.Now().UnixMilli()
		val, _ := o.vm.ToValue(stamp)
		return val
	})

	timeObj.Set("sleep", func(call otto.FunctionCall) otto.Value {
		ms, err := call.Argument(0).ToInteger()
		if err != nil {
			panic(o.vm.MakeCustomError("ArgumentError", "sleep requires an integer argument for milliseconds"))
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		return otto.Value{}
	})

	return o.vm.Set("time", timeObj)
}

func (o *OVM) addArgs(ctx context.Context) error {
	scriptArgs := o.config.ScriptArgs
	if scriptArgs == nil {
		scriptArgs = []string{}
	}
	argsVal, err := o.vm.ToValue(scriptArgs)
	if err != nil {
		return fmt.Errorf("failed to convert script args to JS value: %w", err)
	}
	return o.vm.Set("args", argsVal)
}

func (o *OVM) addOVM(ctx context.Context) error {
	ovmObj, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create ovm object: %w", err)
	}

	ovmObj.Set("run", func(call otto.FunctionCall) otto.Value {
		script, _ := call.Argument(0).ToString()
		if script == "" {
			return otto.UndefinedValue()
		}

		// Create a new config for the sub-OVM.
		// Inherit most settings, but explicitly disable test running
		// to prevent a script from starting a full test suite.
		subOVMConfig := &Config{
			Logger:       o.logger.WithGroup("sub-ovm"),
			SetupCtx:     ctx,
			InsiClient:   o.insiClient,
			DoAddAdmin:   o.config.DoAddAdmin,
			DoAddConsole: o.config.DoAddConsole,
			DoAddOS:      o.config.DoAddOS,
			DoAddTest:    false, // Do not allow running tests from here.
		}

		// Create a new, isolated OVM for the script execution.
		subOVM, err := New(subOVMConfig)
		if err != nil {
			panic(o.vm.MakeCustomError("OVMError", fmt.Sprintf("failed to create sub-OVM: %v", err)))
		}
		defer subOVM.Close()

		subVMResult, err := subOVM.vm.Run(script)
		if err != nil {
			// This will catch JavaScript exceptions thrown from inside the script
			// (e.g., `throw new Error(...)`) and propagate them to the calling script.
			if ottoErr, ok := err.(*otto.Error); ok {
				panic(ottoErr)
			}
			// For other errors (like syntax errors), we create a new JS error.
			panic(o.vm.MakeCustomError("ExecutionError", err.Error()))
		}

		// Export the result from the sub-OVM to a Go type.
		exportedVal, err := subVMResult.Export()
		if err != nil {
			panic(o.vm.MakeCustomError("OVMError", fmt.Sprintf("failed to export result from sub-OVM: %v", err)))
		}

		// Convert the Go type back to a value in the parent OVM's context.
		parentVMVal, err := o.vm.ToValue(exportedVal)
		if err != nil {
			panic(o.vm.MakeCustomError("OVMError", fmt.Sprintf("failed to import result into parent OVM: %v", err)))
		}

		return parentVMVal
	})

	return o.vm.Set("ovm", ovmObj)
}

func (o *OVM) addAdmin(ctx context.Context) error {
	adminObj, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create admin object: %w", err)
	}

	// Create API Key
	adminObj.Set("createKey", func(call otto.FunctionCall) otto.Value {
		keyName, _ := call.Argument(0).ToString()
		if keyName == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "keyName cannot be empty"))
		}
		resp, err := withRetries(o, ctx, func() (*models.ApiKeyCreateResponse, error) {
			return o.insiClient.CreateAPIKey(keyName)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(resp)
		return val
	})

	// Delete API Key
	adminObj.Set("deleteKey", func(call otto.FunctionCall) otto.Value {
		apiKey, _ := call.Argument(0).ToString()
		if apiKey == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "apiKey cannot be empty"))
		}
		_, err := withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.DeleteAPIKey(apiKey)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		return otto.Value{}
	})

	// Get Caller's Limits
	adminObj.Set("getLimits", func(call otto.FunctionCall) otto.Value {
		resp, err := withRetries(o, ctx, func() (*models.LimitsResponse, error) {
			return o.insiClient.GetLimits()
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}

		// The test script expects the API key to be in the response so it can
		// use it to set/restore limits. The /limits endpoint doesn't return it,
		// so we inject it into the response object here for the script's convenience.
		jsObj, err := o.vm.ToValue(resp)
		if err != nil {
			panic(o.vm.MakeCustomError("InternalError", "failed to convert limits response to js object: "+err.Error()))
		}
		if jsObj.IsObject() {
			obj := jsObj.Object()
			if err := obj.Set("api_key", o.insiClient.GetApiKey()); err != nil {
				panic(o.vm.MakeCustomError("InternalError", "failed to set api_key on limits object: "+err.Error()))
			}
			return obj.Value()
		}

		return jsObj // Should not happen, but return original if not an object
	})

	// Get Limits for a specific Key
	adminObj.Set("getLimitsForKey", func(call otto.FunctionCall) otto.Value {
		apiKey, _ := call.Argument(0).ToString()
		if apiKey == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "apiKey cannot be empty"))
		}
		resp, err := withRetries(o, ctx, func() (*models.LimitsResponse, error) {
			return o.insiClient.GetLimitsForKey(apiKey)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(resp)
		return val
	})

	// Set Limits for a specific Key
	adminObj.Set("setLimits", func(call otto.FunctionCall) otto.Value {
		apiKey, _ := call.Argument(0).ToString()
		if apiKey == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "apiKey cannot be empty"))
		}
		limitsObj := call.Argument(1).Object()
		if limitsObj == nil {
			panic(o.vm.MakeCustomError("ArgumentError", "second argument must be a limits object"))
		}

		limits := models.Limits{}

		if val, err := limitsObj.Get("bytes_on_disk"); err == nil && !val.IsUndefined() && !val.IsNull() {
			bytes, _ := val.ToInteger()
			limits.BytesOnDisk = &bytes
		}
		if val, err := limitsObj.Get("bytes_in_memory"); err == nil && !val.IsUndefined() && !val.IsNull() {
			bytes, _ := val.ToInteger()
			limits.BytesInMemory = &bytes
		}
		if val, err := limitsObj.Get("events_emitted"); err == nil && !val.IsUndefined() && !val.IsNull() {
			events, _ := val.ToInteger()
			limits.EventsEmitted = &events
		}
		if val, err := limitsObj.Get("subscribers"); err == nil && !val.IsUndefined() && !val.IsNull() {
			subs, _ := val.ToInteger()
			limits.Subscribers = &subs
		}

		_, err = withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.SetLimits(apiKey, limits)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		return otto.Value{}
	})

	// --- Insight Object ---
	insightObj, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create insight object: %w", err)
	}

	insightObj.Set("getMetrics", func(call otto.FunctionCall) otto.Value {
		metrics := map[string]interface{}{
			"rate_limit_retries":    o.rateLimitRetries.Load(),
			"accumulated_redirects": o.insiClient.GetAccumulatedRedirects(),
		}
		val, err := o.vm.ToValue(metrics)
		if err != nil {
			panic(o.vm.MakeCustomError("InsightError", "failed to convert metrics to JS value: "+err.Error()))
		}
		return val
	})

	insightObj.Set("resetMetrics", func(call otto.FunctionCall) otto.Value {
		o.rateLimitRetries.Store(0)
		o.insiClient.ResetAccumulatedRedirects()
		o.logger.Debug("OVM rate limit retry metric reset")
		return otto.Value{}
	})

	if err := adminObj.Set("insight", insightObj); err != nil {
		return fmt.Errorf("failed to set insight object on admin: %w", err)
	}

	return o.vm.Set("admin", adminObj)
}

func (o *OVM) addTestFeedback(testObj *otto.Object) {
	testObj.Set("Yay", func(call otto.FunctionCall) otto.Value {
		msg, _ := call.Argument(0).ToString()
		fmt.Printf("%s✅ Yay! %s%s\n", colorHiGreen, msg, colorReset)
		return otto.Value{}
	})

	testObj.Set("Aww", func(call otto.FunctionCall) otto.Value {
		msg, _ := call.Argument(0).ToString()
		fmt.Printf("%s💥 Aww! %s%s\n", colorHiRed, msg, colorReset)
		return otto.Value{}
	})
}

func (o *OVM) addTest(ctx context.Context) error {
	testObj, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create test object: %w", err)
	}

	o.addTestFeedback(testObj)

	testObj.Set("setDir", func(call otto.FunctionCall) otto.Value {
		dir, _ := call.Argument(0).ToString()
		if dir == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "directory path cannot be empty"))
		}
		o.testDir = dir
		return otto.Value{}
	})

	runTest := func(call otto.FunctionCall, expectSuccess bool) otto.Value {
		scriptPath, _ := call.Argument(0).ToString()
		if scriptPath == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "script path cannot be empty"))
		}

		if o.testDir == "" {
			panic(o.vm.MakeCustomError("ConfigurationError", "test directory not set, call test.setDir() first"))
		}

		fullPath := filepath.Join(o.testDir, scriptPath)
		if !strings.HasPrefix(fullPath, o.testDir) {
			panic(o.vm.MakeCustomError("SecurityError", "path traversal is not allowed"))
		}

		scriptContent, err := os.ReadFile(fullPath)
		if err != nil {
			panic(o.vm.MakeCustomError("FileError", fmt.Sprintf("failed to read test script %s: %v", fullPath, err)))
		}

		testOVM, err := New(&Config{
			Logger:       o.logger.WithGroup("test"),
			SetupCtx:     ctx,
			InsiClient:   o.insiClient,
			DoAddAdmin:   true,
			DoAddConsole: true,
			DoAddOS:      true,
			DoAddTest:    true, // Allow nested tests
		})
		if err != nil {
			panic(o.vm.MakeCustomError("OVMError", fmt.Sprintf("failed to create test OVM: %v", err)))
		}

		// Inherit test directory from parent OVM
		testOVM.testDir = o.testDir

		result, runErr := testOVM.vm.Run(string(scriptContent))

		if expectSuccess {
			if runErr != nil {
				errMsg := fmt.Sprintf("script %s failed unexpectedly: %v", scriptPath, runErr)
				panic(o.vm.MakeCustomError("TestFailure", fmt.Sprintf("%s😭 %s%s", colorHiRed, errMsg, colorReset)))
			}

			if !result.IsNumber() {
				panic(o.vm.MakeCustomError("TestFailure", fmt.Sprintf("script %s did not return a number, got: %s", scriptPath, result.Class())))
			}

			retVal, _ := result.ToInteger()
			if retVal != 0 {
				errMsg := fmt.Sprintf("script %s expected return 0, got: %d", scriptPath, retVal)
				panic(o.vm.MakeCustomError("TestFailure", fmt.Sprintf("%s👎 %s%s", colorHiRed, errMsg, colorReset)))
			}
			fmt.Printf("%s🎉 Hooray! Test '%s' passed!%s\n", colorHiGreen, scriptPath, colorReset)
			return otto.Value{}
		} else { // expect failure
			if runErr != nil {
				// Script panicked, which is a failure, so test passes.
				fmt.Printf("%s😅 Whew! Test '%s' failed as expected.%s\n", colorHiYellow, scriptPath, colorReset)
				return otto.Value{}
			}

			if !result.IsNumber() {
				panic(o.vm.MakeCustomError("TestFailure", fmt.Sprintf("script %s was expected to fail, but it returned a non-number: %s", scriptPath, result.Class())))
			}

			retVal, _ := result.ToInteger()
			if retVal == 0 {
				errMsg := fmt.Sprintf("script %s was expected to fail, but it returned 0", scriptPath)
				panic(o.vm.MakeCustomError("TestFailure", fmt.Sprintf("%s🤔 %s%s", colorHiYellow, errMsg, colorReset)))
			}

			// Returned non-zero, which is a failure, so test passes.
			fmt.Printf("%s😅 Whew! Test '%s' failed as expected (non-zero return).%s\n", colorHiYellow, scriptPath, colorReset)
			return otto.Value{}
		}
	}

	testObj.Set("runExpectSuccess", func(call otto.FunctionCall) otto.Value {
		return runTest(call, true)
	})

	testObj.Set("runExpectFailure", func(call otto.FunctionCall) otto.Value {
		return runTest(call, false)
	})

	return o.vm.Set("test", testObj)
}

func (o *OVM) addObject(ctx context.Context) error {

	objectObj, err := o.vm.Object(`({})`)
	if err != nil {
		return fmt.Errorf("failed to create object object: %w", err)
	}

	objectObj.Set("upload", func(call otto.FunctionCall) otto.Value {
		filePath, _ := call.Argument(0).ToString()
		if filePath == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "filepath cannot be empty"))
		}

		resp, err := withRetries(o, ctx, func() (*client.ObjectUploadResponse, error) {
			return o.insiClient.ObjectUpload(filePath)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, err := o.vm.ToValue(resp)
		if err != nil {
			panic(o.vm.MakeCustomError("InternalError", "failed to convert upload response to JS value: "+err.Error()))
		}
		return val
	})

	objectObj.Set("download", func(call otto.FunctionCall) otto.Value {
		uuid, _ := call.Argument(0).ToString()
		if uuid == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "uuid cannot be empty"))
		}
		outputPath, _ := call.Argument(1).ToString()
		if outputPath == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "outputPath cannot be empty"))
		}

		_, err := withRetries(o, ctx, func() (any, error) {
			return nil, o.insiClient.ObjectDownload(uuid, outputPath)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		return otto.Value{}
	})

	objectObj.Set("getHash", func(call otto.FunctionCall) otto.Value {
		uuid, _ := call.Argument(0).ToString()
		if uuid == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "uuid cannot be empty"))
		}

		resp, err := withRetries(o, ctx, func() (*client.ObjectHashResponse, error) {
			return o.insiClient.ObjectGetHash(uuid)
		})
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, err := o.vm.ToValue(resp)
		if err != nil {
			panic(o.vm.MakeCustomError("InternalError", "failed to convert hash response to JS value: "+err.Error()))
		}
		return val
	})

	return o.vm.Set("object", objectObj)
}
