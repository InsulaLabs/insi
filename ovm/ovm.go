package ovm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/robertkrimen/otto"
)

type Config struct {
	Logger     *slog.Logger
	SetupCtx   context.Context
	InsiClient *client.Client
}

type OVM struct {
	logger     *slog.Logger
	vm         *otto.Otto
	insiClient *client.Client
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
	if err := ovm.addConsole(cfg.SetupCtx); err != nil {
		return nil, err
	}
	if err := ovm.addOS(cfg.SetupCtx); err != nil {
		return nil, err
	}
	if err := ovm.addTime(cfg.SetupCtx); err != nil {
		return nil, err
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
