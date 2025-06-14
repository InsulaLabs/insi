package tkv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/InsulaLabs/insula/security/badge"
	"github.com/dgraph-io/badger/v3"
)

type tkv struct {
	logger   *slog.Logger
	appCtx   context.Context
	db       *data
	identity badge.Badge
}

var _ TKV = &tkv{}

// ErrInvalidState is returned when an operation encounters data in an unexpected format.
type ErrInvalidState struct {
	Key    string
	Reason string
}

func (e *ErrInvalidState) Error() string {
	return fmt.Sprintf("invalid state for key '%s': %s", e.Key, e.Reason)
}

// ErrQueueNotFound is returned when a queue operation is attempted on a non-existent queue.
type ErrQueueNotFound struct {
	Key string
}

func (e *ErrQueueNotFound) Error() string {
	return fmt.Sprintf("queue '%s' not found", e.Key)
}

// ErrQueueEmpty is returned when trying to pop from an empty queue.
type ErrQueueEmpty struct {
	Key string
}

func (e *ErrQueueEmpty) Error() string {
	return fmt.Sprintf("queue '%s' is empty", e.Key)
}

func normalizeDBName(dbName string) string {
	// Replace any characters that are invalid in directory names across platforms
	sanitized := strings.Map(func(r rune) rune {
		switch {
		case r == '/' || r == '\\' || r == ':' || r == '*' || r == '?' ||
			r == '"' || r == '<' || r == '>' || r == '|':
			return '_'
		default:
			return r
		}
	}, dbName)

	// Ensure name doesn't start with . or - which can cause issues
	if len(sanitized) > 0 && (sanitized[0] == '.' || sanitized[0] == '-') {
		sanitized = "_" + sanitized[1:]
	}

	return sanitized
}

func New(config Config) (TKV, error) {

	valuesDir := filepath.Join(config.Directory, "values")

	if err := os.MkdirAll(valuesDir, 0755); err != nil {
		return nil, &ErrInternal{Err: err}
	}

	badgerLogLevel := badger.INFO
	if config.BadgerLogLevel == slog.LevelDebug {
		badgerLogLevel = badger.DEBUG
	} else if config.BadgerLogLevel == slog.LevelInfo {
		badgerLogLevel = badger.INFO
	} else if config.BadgerLogLevel == slog.LevelWarn {
		badgerLogLevel = badger.WARNING
	} else if config.BadgerLogLevel == slog.LevelError {
		badgerLogLevel = badger.ERROR
	} else {
		config.Logger.Warn("Unknown badger log level, defaulting to info", "level", config.BadgerLogLevel)
	}

	dbOpts := badger.DefaultOptions(valuesDir).
		WithLogger(newLogger(config.Logger.WithGroup("store"))).
		WithLoggingLevel(badgerLogLevel).
		WithMemTableSize(16 << 20) // 16MB MemTableSize

	db, err := badger.Open(dbOpts)
	if err != nil {
		return nil, &ErrInternal{Err: err}
	}

	// Configure in-memory Badger for cache
	cacheOpts := badger.DefaultOptions("").
		WithInMemory(true).
		WithLogger(newLogger(config.Logger.WithGroup("cache"))).
		WithLoggingLevel(badgerLogLevel)

	cache, err := badger.Open(cacheOpts)
	if err != nil {
		return nil, &ErrInternal{Err: err}
	}

	tkv := &tkv{
		logger: config.Logger.WithGroup("tkv"),
		appCtx: config.AppCtx,
		db: &data{
			store: db,
			cache: cache,
		},
		identity: config.Identity,
	}

	return tkv, nil
}

func (t *tkv) Close() error {
	var firstErr error

	if t.db.cache != nil {
		if err := t.db.cache.Close(); err != nil {
			t.logger.Error("error closing cache db", "error", err)
			firstErr = &ErrInternal{Err: err}
		} else {
			t.logger.Info("cache db closed")
		}
	}

	if err := t.db.store.Close(); err != nil {
		t.logger.Error("error closing store db", "error", err)
		if firstErr == nil {
			firstErr = &ErrInternal{Err: err}
		}
	}

	return firstErr
}

func (t *tkv) Get(key string) (string, error) {
	var value []byte
	err := t.db.store.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return &ErrKeyNotFound{Key: key}
			}
			return &ErrInternal{Err: err}
		}
		value, err = item.ValueCopy(nil)
		if err != nil {
			return &ErrInternal{Err: err}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (t *tkv) Set(key string, value string) error {
	err := t.db.store.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		if err != nil {
			return &ErrInternal{Err: err}
		}
		return nil
	})
	return err
}

func (t *tkv) Delete(key string) error {
	err := t.db.store.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		if err != nil {
			return &ErrInternal{Err: err}
		}
		return nil
	})
	return err
}

func (t *tkv) Iterate(prefix string, offset int, limit int) ([]string, error) {
	var keys []string
	err := t.db.store.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefixBytes := []byte(prefix)
		skipped := 0
		collected := 0

		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			if skipped < offset {
				skipped++
				continue
			}
			if limit > 0 && collected >= limit {
				break
			}
			item := it.Item()
			keys = append(keys, string(item.Key()))
			collected++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// -------------------------- CACHE

func (t *tkv) CacheGet(key string) (string, error) {
	t.logger.Debug("CacheGet called", "key", key)
	var value []byte
	err := t.db.cache.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				t.logger.Debug("Cache miss", "key", key)
				return &ErrKeyNotFound{Key: key}
			}
			return &ErrInternal{Err: err}
		}
		value, err = item.ValueCopy(nil)
		if err != nil {
			return &ErrInternal{Err: err}
		}
		t.logger.Debug("Cache hit", "key", key)
		return nil
	})
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (t *tkv) CacheSet(key string, value string) error {
	t.logger.Debug("CacheSet called", "key", key, "value", value)
	err := t.db.cache.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		if err != nil {
			return &ErrInternal{Err: err}
		}
		return nil
	})
	return err
}

func (t *tkv) CacheDelete(key string) error {
	t.logger.Debug("CacheDelete called", "key", key)
	err := t.db.cache.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		if err != nil {
			return &ErrInternal{Err: err}
		}
		return nil
	})
	return err
}

func (t *tkv) CacheIterate(prefix string, offset int, limit int) ([]string, error) {
	var keys []string
	err := t.db.cache.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefixBytes := []byte(prefix)
		skipped := 0
		collected := 0

		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			if skipped < offset {
				skipped++
				continue
			}
			if limit > 0 && collected >= limit {
				break
			}
			item := it.Item()
			keys = append(keys, string(item.Key()))
			collected++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func (t *tkv) CacheSetNX(key string, value string) error {
	err := t.db.cache.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == nil {
			return &ErrKeyExists{Key: key}
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return &ErrInternal{Err: err}
		}

		// Key does not exist, safe to set
		if err := txn.Set([]byte(key), []byte(value)); err != nil {
			return &ErrInternal{Err: err}
		}
		return nil
	})
	return err
}

func (t *tkv) CacheCompareAndSwap(key string, oldValue, newValue string) error {
	err := t.db.cache.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return &ErrCASFailed{Key: key}
			}
			return &ErrInternal{Err: err}
		}

		var valBytes []byte
		err = item.Value(func(val []byte) error {
			valBytes = append([]byte{}, val...)
			return nil
		})

		if err != nil {
			return &ErrInternal{Err: err}
		}

		if string(valBytes) != oldValue {
			return &ErrCASFailed{Key: key}
		}

		// Value matches, proceed with setting the new value.
		if err := txn.Set([]byte(key), []byte(newValue)); err != nil {
			return &ErrInternal{Err: err}
		}
		return nil
	})
	return err
}

func (t *tkv) GetDataDB() *badger.DB {
	return t.db.store
}

func (t *tkv) GetCache() *badger.DB {
	return t.db.cache
}

func (t *tkv) BatchSet(entries []TKVBatchEntry) error {
	if len(entries) == 0 {
		return nil // Nothing to do
	}

	wb := t.db.store.NewWriteBatch()
	defer wb.Cancel() // Cancel if not committed

	for _, entry := range entries {
		if entry.Key == "" {
			// Or return an error, depending on desired behavior for invalid entries within a batch
			t.logger.Warn("BatchSet encountered an entry with an empty key, skipping.")
			continue
		}
		// Consider adding validation for value size if necessary, similar to single Set.
		if err := wb.Set([]byte(entry.Key), []byte(entry.Value)); err != nil {
			// This error is typically for when the batch is too large for badger's buffer.
			// It might be better to return it and let the caller handle (e.g., retry with smaller batches).
			return &ErrInternal{Err: fmt.Errorf("failed to add set operation for key '%s' to batch: %w", entry.Key, err)}
		}
	}

	if err := wb.Flush(); err != nil { // Flush commits the batch
		return &ErrInternal{Err: fmt.Errorf("failed to flush batch set: %w", err)}
	}
	return nil
}

func (t *tkv) BatchDelete(keys []string) error {
	if len(keys) == 0 {
		return nil // Nothing to do
	}

	wb := t.db.store.NewWriteBatch()
	defer wb.Cancel()

	for _, key := range keys {
		if key == "" {
			t.logger.Warn("BatchDelete encountered an empty key, skipping.")
			continue
		}
		if err := wb.Delete([]byte(key)); err != nil {
			return &ErrInternal{Err: fmt.Errorf("failed to add delete operation for key '%s' to batch: %w", key, err)}
		}
	}

	if err := wb.Flush(); err != nil {
		return &ErrInternal{Err: fmt.Errorf("failed to flush batch delete: %w", err)}
	}
	return nil
}

func (t *tkv) SetNX(key string, value string) error {
	err := t.db.store.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == nil {
			return &ErrKeyExists{Key: key}
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return &ErrInternal{Err: err}
		}

		// Key does not exist, safe to set
		if err := txn.Set([]byte(key), []byte(value)); err != nil {
			return &ErrInternal{Err: err}
		}
		return nil
	})
	return err
}

func (t *tkv) CompareAndSwap(key string, oldValue, newValue string) error {
	err := t.db.store.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return &ErrCASFailed{Key: key}
			}
			return &ErrInternal{Err: err}
		}

		var valBytes []byte
		err = item.Value(func(val []byte) error {
			valBytes = append([]byte{}, val...)
			return nil
		})

		if err != nil {
			return &ErrInternal{Err: err}
		}

		if string(valBytes) != oldValue {
			return &ErrCASFailed{Key: key}
		}

		// Value matches, proceed with setting the new value.
		if err := txn.Set([]byte(key), []byte(newValue)); err != nil {
			return &ErrInternal{Err: err}
		}
		return nil
	})
	return err
}

func (t *tkv) BumpInteger(key string, delta int64) error {
	err := t.db.store.Update(func(txn *badger.Txn) error {
		var currentVal int64
		item, err := txn.Get([]byte(key))

		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				// Key doesn't exist, we'll create it. Start from 0.
				currentVal = 0
			} else {
				// For any other error, it's internal.
				return &ErrInternal{Err: err}
			}
		} else {
			// Key exists, get its value.
			err = item.Value(func(val []byte) error {
				// If value is empty, treat as 0
				if len(val) == 0 {
					currentVal = 0
					return nil
				}
				// Otherwise, parse it.
				parsedVal, parseErr := strconv.ParseInt(string(val), 10, 64)
				if parseErr != nil {
					// If parsing fails, return an invalid state error.
					return &ErrInvalidState{Key: key, Reason: "value is not a valid integer"}
				}
				currentVal = parsedVal
				return nil
			})
			if err != nil {
				return err // This could be ErrInvalidState or another error from Value().
			}
		}

		newValue := currentVal + delta

		// Explicitly floor the value at 0.
		if newValue < 0 {
			newValue = 0
		}

		// Set the new value.
		if err := txn.Set([]byte(key), []byte(strconv.FormatInt(newValue, 10))); err != nil {
			return &ErrInternal{Err: err}
		}
		return nil
	})
	return err
}
