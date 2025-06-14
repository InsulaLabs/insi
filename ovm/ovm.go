package ovm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

	DoAddAdmin   bool
	DoAddConsole bool
	DoAddOS      bool
	DoAddTest    bool
}

type OVM struct {
	logger     *slog.Logger
	vm         *otto.Otto
	insiClient *client.Client
	testDir    string
}

func New(cfg *Config) (*OVM, error) {

	ovm := &OVM{
		logger:     cfg.Logger,
		vm:         otto.New(),
		insiClient: cfg.InsiClient,
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

	return ovm, nil
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
		if err := o.insiClient.Set(key, value); err != nil {
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
		if err := o.insiClient.Delete(key); err != nil {
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
		value, err := o.insiClient.Get(key)
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
		keys, err := o.insiClient.IterateByPrefix(prefix, int(offset), int(limit))
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
		err := o.insiClient.SetNX(key, value)
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
		err := o.insiClient.CompareAndSwap(key, oldValue, newValue)
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
		if err := o.insiClient.SetCache(key, value); err != nil {
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
		if err := o.insiClient.DeleteCache(key); err != nil {
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
		value, err := o.insiClient.GetCache(key)
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
		keys, err := o.insiClient.IterateCacheByPrefix(prefix, int(offset), int(limit))
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
		err := o.insiClient.SetCacheNX(key, value)
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
		err := o.insiClient.CompareAndSwapCache(key, oldValue, newValue)
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

		if err := o.insiClient.PublishEvent(topic, data); err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}

		return otto.Value{}
	})
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
		resp, err := o.insiClient.CreateAPIKey(keyName)
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
		err := o.insiClient.DeleteAPIKey(apiKey)
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		return otto.Value{}
	})

	// Get Caller's Limits
	adminObj.Set("getLimits", func(call otto.FunctionCall) otto.Value {
		resp, err := o.insiClient.GetLimits()
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		val, _ := o.vm.ToValue(resp)
		return val
	})

	// Get Limits for a specific Key
	adminObj.Set("getLimitsForKey", func(call otto.FunctionCall) otto.Value {
		apiKey, _ := call.Argument(0).ToString()
		if apiKey == "" {
			panic(o.vm.MakeCustomError("ArgumentError", "apiKey cannot be empty"))
		}
		resp, err := o.insiClient.GetLimitsForKey(apiKey)
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

		err = o.insiClient.SetLimits(apiKey, limits)
		if err != nil {
			panic(o.vm.MakeCustomError("InsiClientError", err.Error()))
		}
		return otto.Value{}
	})

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
