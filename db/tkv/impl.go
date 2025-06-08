package tkv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/InsulaLabs/insula/security/badge"
	"github.com/dgraph-io/badger/v3"
	"github.com/jellydator/ttlcache/v3"
)

var DefaultCacheTTL = 1 * time.Minute
var DefaultSecureCacheTTL = 1 * time.Minute

type tkv struct {
	logger          *slog.Logger
	appCtx          context.Context
	db              *data
	defaultCacheTTL time.Duration
	identity        badge.Badge
}

var _ TKV = &tkv{}

// ErrKeyExists is returned when trying to create a key that already exists
// and overwrite is false.
type ErrKeyExists struct {
	Key string
}

func (e *ErrKeyExists) Error() string {
	return fmt.Sprintf("key '%s' already exists", e.Key)
}

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

	// in case they set it to 0 for some fuckin reason
	if DefaultCacheTTL == 0 {
		DefaultCacheTTL = 1 * time.Minute
	}

	if config.CacheTTL == 0 {
		config.CacheTTL = DefaultCacheTTL
	}

	cache := ttlcache.New[string, string](
		ttlcache.WithTTL[string, string](config.CacheTTL),

		// If we dont do this then on a multi-node cluster some nodes will expire
		// while others might be "Getting hit" and not expire leading to a stale cache
		// this ensures a fully ephemeral value that is only valid for the duration of the cache
		ttlcache.WithDisableTouchOnHit[string, string](),
	)
	go cache.Start()

	tkv := &tkv{
		logger: config.Logger.WithGroup("tkv"),
		appCtx: config.AppCtx,
		db: &data{
			store: db,
			cache: cache,
		},
		defaultCacheTTL: config.CacheTTL,
		identity:        config.Identity,
	}

	return tkv, nil
}

func (t *tkv) Close() error {
	var firstErr error

	// Stop the cache
	if t.db.cache != nil {
		t.db.cache.Stop()
		t.logger.Info("ttl cache stopped")
	}

	if err := t.db.store.Close(); err != nil {
		t.logger.Error("error closing store db", "error", err)
		firstErr = &ErrInternal{Err: err}
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
	item := t.db.cache.Get(key)
	if item == nil {
		t.logger.Debug("Cache miss", "key", key)
		return "", &ErrKeyNotFound{Key: key}
	}
	if item.IsExpired() {
		t.logger.Debug("Cache item expired", "key", key)
		t.db.cache.Delete(key)
		return "", &ErrKeyNotFound{Key: key}
	}
	t.logger.Debug("Cache hit", "key", key, "value", item.Value())
	return item.Value(), nil
}

func (t *tkv) CacheSet(key string, value string, ttl time.Duration) error {
	t.logger.Debug("CacheSet called", "key", key, "value", value, "ttl", ttl)
	if ttl == 0 {
		t.logger.Debug("CacheSet ttl is 0, using default ttl", "default ttl", t.defaultCacheTTL)
		ttl = t.defaultCacheTTL
	}
	t.db.cache.Set(key, value, ttl)
	return nil
}

func (t *tkv) CacheDelete(key string) error {
	t.logger.Debug("CacheDelete called", "key", key)
	t.db.cache.Delete(key)
	return nil
}

func (t *tkv) GetDataDB() *badger.DB {
	return t.db.store
}

func (t *tkv) GetCache() *ttlcache.Cache[string, string] {
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
