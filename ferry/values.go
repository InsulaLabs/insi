package ferry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"

	"github.com/InsulaLabs/insi/client"
)

type ValueController[T any] interface {
	PushScope(prefix string) error
	PopScope() error
	GetPrefix() string

	Get(ctx context.Context, key string) (T, error)
	Set(ctx context.Context, key string, value T) error
	RawSet(ctx context.Context, key string, value string) error
	SetNX(ctx context.Context, key string, value T) error
	Delete(ctx context.Context, key string) error
	CompareAndSwap(ctx context.Context, key string, oldValue, newValue T) error
	Bump(ctx context.Context, key string, value int) error
	IterateByPrefix(ctx context.Context, prefix string, offset, limit int) ([]string, error)
}

type vcImpl[T any] struct {
	defaultT T
	client   *client.Client
	logger   *slog.Logger

	scopes       []string
	prefix       string
	scopeChanged atomic.Bool
}

func NewValueController[T any](defaultT T, c *client.Client, logger *slog.Logger) ValueController[T] {
	return &vcImpl[T]{
		defaultT: defaultT,
		client:   c,
		logger:   logger.WithGroup("value_controller"),
	}
}

func (vc *vcImpl[T]) buildPrefix(key string) string {
	if len(vc.scopes) == 0 {
		return key
	}
	if !vc.scopeChanged.Load() {
		return fmt.Sprintf("%s:%s", vc.prefix, key)
	}

	vc.prefix = strings.Join(vc.scopes, ":")
	vc.scopeChanged.Store(false)
	return fmt.Sprintf("%s:%s", vc.prefix, key)
}

func (vc *vcImpl[T]) GetPrefix() string {
	return vc.prefix
}

func (vc *vcImpl[T]) PushScope(prefix string) error {
	if prefix == "" {
		return ErrEmptyScopePush
	}

	vc.scopes = append(vc.scopes, prefix)
	vc.scopeChanged.Store(true)
	return nil
}

func (vc *vcImpl[T]) PopScope() error {
	if len(vc.scopes) == 0 {
		return ErrEndScope
	}

	vc.scopes = vc.scopes[:len(vc.scopes)-1]
	vc.scopeChanged.Store(true)
	return nil
}

func (vc *vcImpl[T]) Get(ctx context.Context, key string) (T, error) {
	var result T

	fullKey := vc.buildPrefix(key)

	err := client.WithRetriesVoid(ctx, vc.logger, func() error {
		valueStr, err := vc.client.Get(fullKey)
		if err != nil {
			if errors.Is(err, client.ErrKeyNotFound) {
				result = vc.defaultT
				return nil
			}
			return err
		}

		result = safeUnmarshal(valueStr, vc.defaultT, vc.logger, key)
		return nil
	})

	return result, translateError(err)
}

func (vc *vcImpl[T]) Set(ctx context.Context, key string, value T) error {
	err := client.WithRetriesVoid(ctx, vc.logger, func() error {
		// Marshal the value to JSON
		valueBytes, err := json.Marshal(value)
		if err != nil {
			return err
		}

		fullKey := vc.buildPrefix(key)

		return vc.client.Set(fullKey, string(valueBytes))
	})

	return translateError(err)
}

func (vc *vcImpl[T]) RawSet(ctx context.Context, key string, value string) error {
	err := client.WithRetriesVoid(ctx, vc.logger, func() error {
		fullKey := vc.buildPrefix(key)
		return vc.client.Set(fullKey, value)
	})

	return translateError(err)
}

func (vc *vcImpl[T]) SetNX(ctx context.Context, key string, value T) error {
	err := client.WithRetriesVoid(ctx, vc.logger, func() error {
		valueBytes, err := json.Marshal(value)
		if err != nil {
			return err
		}
		fullKey := vc.buildPrefix(key)
		return vc.client.SetNX(fullKey, string(valueBytes))
	})

	return translateError(err)
}

func (vc *vcImpl[T]) Delete(ctx context.Context, key string) error {
	err := client.WithRetriesVoid(ctx, vc.logger, func() error {
		fullKey := vc.buildPrefix(key)
		return vc.client.Delete(fullKey)
	})

	return translateError(err)
}

func (vc *vcImpl[T]) CompareAndSwap(ctx context.Context, key string, oldValue, newValue T) error {
	err := client.WithRetriesVoid(ctx, vc.logger, func() error {
		// Marshal the old and new values to JSON
		oldValueBytes, err := json.Marshal(oldValue)
		if err != nil {
			return err
		}

		newValueBytes, err := json.Marshal(newValue)
		if err != nil {
			return err
		}

		fullKey := vc.buildPrefix(key)

		return vc.client.CompareAndSwap(fullKey, string(oldValueBytes), string(newValueBytes))
	})

	return translateError(err)
}

func (vc *vcImpl[T]) Bump(ctx context.Context, key string, value int) error {
	err := client.WithRetriesVoid(ctx, vc.logger, func() error {
		fullKey := vc.buildPrefix(key)
		return vc.client.Bump(fullKey, value)
	})

	return translateError(err)
}

func (vc *vcImpl[T]) IterateByPrefix(ctx context.Context, prefix string, offset, limit int) ([]string, error) {
	var result []string

	err := client.WithRetriesVoid(ctx, vc.logger, func() error {
		fullPrefix := vc.buildPrefix(prefix)
		keys, err := vc.client.IterateByPrefix(fullPrefix, offset, limit)
		if err != nil {
			return err
		}

		// Remove the scope prefix from returned keys
		scopePrefix := ""
		if len(vc.scopes) > 0 {
			scopePrefix = strings.Join(vc.scopes, ":") + ":"
		}

		result = make([]string, 0, len(keys))
		for _, key := range keys {
			if strings.HasPrefix(key, scopePrefix) {
				result = append(result, strings.TrimPrefix(key, scopePrefix))
			} else {
				// If key doesn't have expected prefix, include it as-is
				result = append(result, key)
			}
		}
		return nil
	})

	return result, translateError(err)
}
