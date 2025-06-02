package tkv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
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

func New(config Config) (TKV, error) {

	valuesDir := filepath.Join(config.Directory, "values")
	objectsDir := filepath.Join(config.Directory, "objects")

	if err := os.MkdirAll(valuesDir, 0755); err != nil {
		return nil, &ErrInternal{Err: err}
	}

	if err := os.MkdirAll(objectsDir, 0755); err != nil {
		return nil, &ErrInternal{Err: err}
	}

	dbOpts := badger.DefaultOptions(valuesDir).
		WithLogger(newLogger(config.Logger.WithGroup("store"))).
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
			store:  db,
			cache:  cache,
			queues: make(map[string][]string), // Initialize queues map
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

// -------------------------- ATOMIC OPERATIONS

// AtomicNew creates a new key for atomic operations, initializing its value to "0".
// If overwrite is true and the key exists, it will be reset to "0".
// If overwrite is false and the key exists, ErrKeyExists is returned.
func (t *tkv) AtomicNew(key string, overwrite bool) error {
	return t.db.store.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		keyExists := err == nil

		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return &ErrInternal{Err: fmt.Errorf("failed to check existence for key %s: %w", key, err)}
		}

		if keyExists && !overwrite {
			return &ErrKeyExists{Key: key}
		}

		// If key exists and overwrite is true, or if key doesn't exist, set to "0".
		// Badger's Set will handle overwriting if the key exists.
		return txn.Set([]byte(key), []byte("0"))
	})
}

// AtomicGet retrieves the int64 value of an atomic key.
// Returns 0 if the key does not exist (as per interface spec).
// Returns ErrInvalidState if the key exists but its value is not a valid int64.
func (t *tkv) AtomicGet(key string) (int64, error) {
	var value int64
	errView := t.db.store.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			value = 0 // Key doesn't exist, return 0 and no error for AtomicGet
			return nil
		}
		if err != nil {
			return &ErrInternal{Err: fmt.Errorf("failed to get key %s: %w", key, err)}
		}

		valBytes, err := item.ValueCopy(nil)
		if err != nil {
			return &ErrInternal{Err: fmt.Errorf("failed to copy value for key %s: %w", key, err)}
		}

		parsedValue, err := strconv.ParseInt(string(valBytes), 10, 64)
		if err != nil {
			value = 0 // Set to 0 on parse error as well
			return &ErrInvalidState{Key: key, Reason: fmt.Sprintf("value not a valid int64: '%s'", string(valBytes))}
		}
		value = parsedValue
		return nil
	})

	if errView != nil {
		return value, errView // Return the value (which might be 0 if parsing failed) and the error
	}
	return value, nil
}

// AtomicAdd adds a delta to an atomic key's int64 value.
// If the key does not exist, it's treated as starting from 0.
// The value is floored at 0 (cannot go negative).
// Returns the new value after the addition.
func (t *tkv) AtomicAdd(key string, delta int64) (int64, error) {
	var newValue int64
	errUpdate := t.db.store.Update(func(txn *badger.Txn) error {
		item, errGet := txn.Get([]byte(key))
		var currentValue int64

		if errors.Is(errGet, badger.ErrKeyNotFound) {
			currentValue = 0 // Key doesn't exist, start from 0
		} else if errGet != nil {
			return &ErrInternal{Err: fmt.Errorf("failed to get key %s for add: %w", key, errGet)}
		} else {
			valBytes, errCopy := item.ValueCopy(nil)
			if errCopy != nil {
				return &ErrInternal{Err: fmt.Errorf("failed to copy value for key %s for add: %w", key, errCopy)}
			}
			parsedVal, errParse := strconv.ParseInt(string(valBytes), 10, 64)
			if errParse != nil {
				// If current value is not a number, it's an invalid state.
				// Consider if this should default to 0 and add, or error out.
				// Erroring out seems safer for "atomic" operations.
				return &ErrInvalidState{Key: key, Reason: fmt.Sprintf("existing value not a valid int64: '%s'", string(valBytes))}
			}
			currentValue = parsedVal
		}

		newValue = currentValue + delta
		if newValue < 0 {
			newValue = 0 // Floor at 0
		}

		return txn.Set([]byte(key), []byte(strconv.FormatInt(newValue, 10)))
	})

	if errUpdate != nil {
		return 0, errUpdate // Return 0 for value if the update failed
	}
	return newValue, nil
}

// AtomicDelete deletes an atomic key.
// No error is returned if the key does not exist.
func (t *tkv) AtomicDelete(key string) error {
	return t.db.store.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		if err != nil {
			// Badger's Delete can return an error for reasons other than not found (though typically not for a simple Delete)
			// We wrap it to conform to our error handling.
			return &ErrInternal{Err: fmt.Errorf("failed to delete key %s: %w", key, err)}
		}
		return nil // Badger's Delete is idempotent; no error if key doesn't exist.
	})
}

// -------------------------- QUEUE OPERATIONS (In-Memory)

// QueueNew creates a new in-memory queue.
// If the queue already exists, no error is returned and the existing queue is unchanged.
func (t *tkv) QueueNew(key string) error {
	t.db.qLock.Lock()
	defer t.db.qLock.Unlock()

	if _, exists := t.db.queues[key]; !exists {
		t.db.queues[key] = make([]string, 0)
		t.logger.Debug("QueueNew: created new queue", "key", key)
	} else {
		t.logger.Debug("QueueNew: queue already exists", "key", key)
	}
	return nil
}

// QueuePush pushes a value onto the end of an in-memory queue.
// Returns the new length of the queue.
// If the queue does not exist, it returns ErrQueueNotFound.
func (t *tkv) QueuePush(key string, value string) (int, error) {
	t.db.qLock.Lock()
	defer t.db.qLock.Unlock()

	if queue, exists := t.db.queues[key]; exists {
		t.db.queues[key] = append(queue, value)
		newLength := len(t.db.queues[key])
		t.logger.Debug("QueuePush: pushed value to queue", "key", key, "value", value, "new_length", newLength)
		return newLength, nil
	}
	t.logger.Warn("QueuePush: queue not found", "key", key)
	return 0, &ErrQueueNotFound{Key: key}
}

// QueuePop removes and returns the first value from an in-memory queue (FIFO).
// Returns the value.
// If the queue does not exist, it returns ErrQueueNotFound.
// If the queue is empty, it returns ErrQueueEmpty.
func (t *tkv) QueuePop(key string) (string, error) {
	t.db.qLock.Lock()
	defer t.db.qLock.Unlock()

	if queue, exists := t.db.queues[key]; exists {
		if len(queue) == 0 {
			t.logger.Warn("QueuePop: queue is empty", "key", key)
			return "", &ErrQueueEmpty{Key: key}
		}
		value := queue[0]
		t.db.queues[key] = queue[1:]
		t.logger.Debug("QueuePop: popped value from queue", "key", key, "value", value, "new_length", len(t.db.queues[key]))
		return value, nil
	}
	t.logger.Warn("QueuePop: queue not found", "key", key)
	return "", &ErrQueueNotFound{Key: key}
}

// QueueDelete deletes an in-memory queue.
// If the queue does not exist, no error is returned.
func (t *tkv) QueueDelete(key string) error {
	t.db.qLock.Lock()
	defer t.db.qLock.Unlock()

	if _, exists := t.db.queues[key]; exists {
		delete(t.db.queues, key)
		t.logger.Debug("QueueDelete: deleted queue", "key", key)
	} else {
		t.logger.Debug("QueueDelete: queue not found, no action needed", "key", key)
	}
	return nil
}
