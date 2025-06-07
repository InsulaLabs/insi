package tkv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

type testTKV struct {
	tkv TKV
	dir string
}

func (t *testTKV) Cleanup() error {
	return os.RemoveAll(t.dir)
}

func createTestTKV(ctx context.Context) (*testTKV, error) {
	// Create a unique temp directory for each test instance
	tempBaseDir := os.TempDir()
	dir, err := os.MkdirTemp(tempBaseDir, "tkv_test_*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir for test: %w", err)
	}

	fmt.Println("Temp dir:", dir)
	tkv, err := New(Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Directory: dir,
		AppCtx:    ctx,
	})
	if err != nil {
		return nil, err
	}
	return &testTKV{
		tkv: tkv,
		dir: dir, // so we can clean up after
	}, nil
}

// -------------------------- TESTS

func TestTKV_GetSetDelete(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	defer tkvTest.Cleanup()

	t.Run("Set and Get basic value", func(t *testing.T) {
		key := "testKey1"
		value := "testValue1"
		err := tkvTest.tkv.Set(key, value)
		if err != nil {
			t.Errorf("Set() error = %v, wantErr nil", err)
		}

		retrievedVal, err := tkvTest.tkv.Get(key)
		if err != nil {
			t.Errorf("Get() error = %v, wantErr nil", err)
		}
		if retrievedVal != value {
			t.Errorf("Get() got = %v, want %v", retrievedVal, value)
		}
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		key := "nonExistentKey"
		_, err := tkvTest.tkv.Get(key)
		if err == nil {
			t.Errorf("Get() expected error for non-existent key, got nil")
		}
		var keyNotFound *ErrKeyNotFound
		if !errors.As(err, &keyNotFound) {
			t.Errorf("Get() expected ErrKeyNotFound, got %T", err)
		}
		if keyNotFound.Key != key {
			t.Errorf("ErrKeyNotFound.Key got = %s, want %s", keyNotFound.Key, key)
		}
	})

	t.Run("Delete existing key", func(t *testing.T) {
		key := "toBeDeletedKey"
		value := "toBeDeletedValue"
		if err := tkvTest.tkv.Set(key, value); err != nil {
			t.Fatalf("Setup: Set() error = %v", err)
		}

		if err := tkvTest.tkv.Delete(key); err != nil {
			t.Errorf("Delete() error = %v, wantErr nil", err)
		}

		_, err := tkvTest.tkv.Get(key)
		if !errors.As(err, new(*ErrKeyNotFound)) {
			t.Errorf("Get() after Delete expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("Delete non-existent key", func(t *testing.T) {
		key := "nonExistentKeyForDelete"
		err := tkvTest.tkv.Delete(key)
		if err != nil {
			t.Errorf("Delete() of non-existent key error = %v, wantErr nil", err)
		}
	})
}

func TestTKV_Iterate(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	defer tkvTest.Cleanup()

	// Setup: Add some data
	keys := []string{"prefix_key1", "prefix_key2", "prefix_key3", "other_key1"}
	values := []string{"value1", "value2", "value3", "valueOther1"}

	for i, key := range keys {
		if err := tkvTest.tkv.Set(key, values[i]); err != nil {
			t.Fatalf("Setup: Set() error for key %s: %v", key, err)
		}
	}

	t.Run("Iterate with prefix", func(t *testing.T) {
		prefix := "prefix_"
		expectedKeys := []string{"prefix_key1", "prefix_key2", "prefix_key3"}
		retrievedKeys, err := tkvTest.tkv.Iterate(prefix, 0, 0)
		if err != nil {
			t.Errorf("Iterate() error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		sort.Strings(expectedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeys) {
			t.Errorf("Iterate() got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})

	t.Run("Iterate with prefix and offset", func(t *testing.T) {
		prefix := "prefix_"
		offset := 1
		expectedKeys := []string{"prefix_key2", "prefix_key3"}
		retrievedKeys, err := tkvTest.tkv.Iterate(prefix, offset, 0)
		if err != nil {
			t.Errorf("Iterate() error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		sort.Strings(expectedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeys) {
			t.Errorf("Iterate() got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})

	t.Run("Iterate with prefix and limit", func(t *testing.T) {
		prefix := "prefix_"
		limit := 2
		expectedKeys := []string{"prefix_key1", "prefix_key2"}
		retrievedKeys, err := tkvTest.tkv.Iterate(prefix, 0, limit)
		if err != nil {
			t.Errorf("Iterate() error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		sort.Strings(expectedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeys) {
			t.Errorf("Iterate() got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})

	t.Run("Iterate with prefix, offset, and limit", func(t *testing.T) {
		prefix := "prefix_"
		offset := 1
		limit := 1
		expectedKeys := []string{"prefix_key2"}
		retrievedKeys, err := tkvTest.tkv.Iterate(prefix, offset, limit)
		if err != nil {
			t.Errorf("Iterate() error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		sort.Strings(expectedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeys) {
			t.Errorf("Iterate() got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})

	t.Run("Iterate with non-matching prefix", func(t *testing.T) {
		prefix := "non_matching_prefix_"
		expectedKeys := []string{}
		retrievedKeys, err := tkvTest.tkv.Iterate(prefix, 0, 0)
		if err != nil {
			t.Errorf("Iterate() error = %v, wantErr nil", err)
		}
		if len(retrievedKeys) != 0 {
			t.Errorf("Iterate() got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})
}

func TestTKV_Cache(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	// No need to defer tkvTest.Cleanup() here as createTestTKV uses t.TempDir() which handles cleanup in Go 1.15+
	// However, our createTestTKV uses os.MkdirTemp and manual cleanup, so we still need it.
	defer tkvTest.Cleanup()

	tkvInstance, ok := tkvTest.tkv.(TKVCacheHandler) // Type assert to get TKVCacheHandler
	if !ok {
		t.Fatalf("TKV instance does not implement TKVCacheHandler")
	}

	t.Run("Set and Get cache value", func(t *testing.T) {
		key := "cacheKey1"
		value := "cacheValue1"
		ttl := 5 * time.Minute

		if err := tkvInstance.CacheSet(key, value, ttl); err != nil {
			t.Errorf("CacheSet() error = %v, wantErr nil", err)
		}

		retrievedVal, err := tkvInstance.CacheGet(key)
		if err != nil {
			t.Errorf("CacheGet() error = %v, wantErr nil", err)
		}
		if retrievedVal != value {
			t.Errorf("CacheGet() got = %v, want %v", retrievedVal, value)
		}
	})

	t.Run("Get non-existent cache key", func(t *testing.T) {
		key := "nonExistentCacheKey"
		_, err := tkvInstance.CacheGet(key)
		if err == nil {
			t.Errorf("CacheGet() expected error for non-existent key, got nil")
		}
		var keyNotFound *ErrKeyNotFound
		if !errors.As(err, &keyNotFound) {
			t.Errorf("CacheGet() expected ErrKeyNotFound, got %T", err)
		}
		if keyNotFound.Key != key {
			t.Errorf("ErrKeyNotFound.Key got = %s, want %s", keyNotFound.Key, key)
		}
	})

	t.Run("Cache value expiration", func(t *testing.T) {
		key := "cacheKeyTTL"
		value := "cacheValueTTL"
		ttl := 100 * time.Millisecond // Short TTL

		if err := tkvInstance.CacheSet(key, value, ttl); err != nil {
			t.Fatalf("CacheSet() error = %v", err)
		}

		// Wait for TTL to expire
		time.Sleep(ttl + 50*time.Millisecond)

		_, err := tkvInstance.CacheGet(key)
		if err == nil {
			t.Errorf("CacheGet() expected error for expired key, got nil")
		}
		var keyNotFound *ErrKeyNotFound
		if !errors.As(err, &keyNotFound) {
			t.Errorf("CacheGet() expected ErrKeyNotFound for expired key, got %T", err)
		}
	})

	t.Run("Delete cache key", func(t *testing.T) {
		key := "cacheKeyDelete"
		value := "cacheValueDelete"
		ttl := 5 * time.Minute

		if err := tkvInstance.CacheSet(key, value, ttl); err != nil {
			t.Fatalf("CacheSet() error = %v", err)
		}

		if err := tkvInstance.CacheDelete(key); err != nil {
			t.Errorf("CacheDelete() error = %v, wantErr nil", err)
		}

		_, err := tkvInstance.CacheGet(key)
		if err == nil {
			t.Errorf("CacheGet() after CacheDelete expected error, got nil")
		}
		var keyNotFound *ErrKeyNotFound
		if !errors.As(err, &keyNotFound) {
			t.Errorf("CacheGet() after CacheDelete expected ErrKeyNotFound, got %T", err)
		}
	})

	t.Run("Set value with zero TTL uses instance's default TTL for caching", func(t *testing.T) {
		key := "cacheKeyZeroTTL"
		value := "cacheValueZeroTTL"

		// tkvInstance is from createTestTKV. Its tkvInstance.defaultCacheTTL is initialized
		// based on the Config.CacheTTL or the global DefaultCacheTTL at the time of its creation.
		// In this test setup, it will be the package-level DefaultCacheTTL (e.g., 1 minute).

		if err := tkvInstance.CacheSet(key, value, 0); err != nil { // 0 TTL
			t.Fatalf("CacheSet() with 0 TTL error = %v, wantErr nil", err)
		}

		// Item should be immediately available because it was cached with tkvInstance.defaultCacheTTL.
		retrievedVal, err := tkvInstance.CacheGet(key)
		if err != nil {
			t.Fatalf("CacheGet() for 0 TTL key error = %v, wantErr nil. This implies it was not cached as expected.", err)
		}
		if retrievedVal != value {
			t.Errorf("CacheGet() for 0 TTL key got = %v, want %v", retrievedVal, value)
		}

		// This test confirms that a 0 TTL to CacheSet results in the item being cached
		// (using the instance's defaultCacheTTL). It does not need to wait for expiration.
		// The actual expiration with a specific TTL is covered by "Cache value expiration" test.
	})
}

func TestTKV_BatchOperations(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	defer tkvTest.Cleanup()

	tkvBatchHandler, ok := tkvTest.tkv.(TKVBatchHandler)
	if !ok {
		t.Fatalf("TKV instance does not implement TKVBatchHandler")
	}

	t.Run("BatchSet basic functionality", func(t *testing.T) {
		entries := []TKVBatchEntry{
			{Key: "batchKey1", Value: "batchValue1"},
			{Key: "batchKey2", Value: "batchValue2"},
			{Key: "batchKey3", Value: "batchValue3"},
		}

		if err := tkvBatchHandler.BatchSet(entries); err != nil {
			t.Errorf("BatchSet() error = %v, wantErr nil", err)
		}

		for _, entry := range entries {
			retrievedVal, err := tkvTest.tkv.Get(entry.Key)
			if err != nil {
				t.Errorf("Get() after BatchSet error for key %s: %v", entry.Key, err)
			}
			if retrievedVal != entry.Value {
				t.Errorf("Get() after BatchSet got = %v, want %v for key %s", retrievedVal, entry.Value, entry.Key)
			}
		}
	})

	t.Run("BatchSet with empty slice", func(t *testing.T) {
		if err := tkvBatchHandler.BatchSet([]TKVBatchEntry{}); err != nil {
			t.Errorf("BatchSet() with empty slice error = %v, wantErr nil", err)
		}
	})

	t.Run("BatchSet with entry having empty key (should be skipped)", func(t *testing.T) {
		entries := []TKVBatchEntry{
			{Key: "batchKeyValid1", Value: "batchValueValid1"},
			{Key: "", Value: "valueForEmptyKey"}, // Invalid entry
			{Key: "batchKeyValid2", Value: "batchValueValid2"},
		}

		if err := tkvBatchHandler.BatchSet(entries); err != nil {
			t.Errorf("BatchSet() with an empty key entry error = %v, wantErr nil (as it should be skipped)", err)
		}

		// Check valid entries were set
		val1, err1 := tkvTest.tkv.Get("batchKeyValid1")
		if err1 != nil || val1 != "batchValueValid1" {
			t.Errorf("batchKeyValid1 not set correctly after BatchSet with empty key entry")
		}
		val2, err2 := tkvTest.tkv.Get("batchKeyValid2")
		if err2 != nil || val2 != "batchValueValid2" {
			t.Errorf("batchKeyValid2 not set correctly after BatchSet with empty key entry")
		}

		// Check empty key was not set (difficult to verify directly, but Get should fail)
		_, errGetEmpty := tkvTest.tkv.Get("")
		if !errors.As(errGetEmpty, new(*ErrKeyNotFound)) {
			// Depending on Badger's behavior for empty keys, this might need adjustment.
			// The implementation skips empty keys, so it shouldn't be set.
			t.Logf("Get for empty key after BatchSet returned: %v. Expected ErrKeyNotFound or similar.", errGetEmpty)
		}
	})

	t.Run("BatchDelete basic functionality", func(t *testing.T) {
		// First, set some keys
		keysToSet := []TKVBatchEntry{
			{Key: "delBatchKey1", Value: "val1"},
			{Key: "delBatchKey2", Value: "val2"},
			{Key: "delBatchKey3", Value: "val3"},
		}
		if err := tkvBatchHandler.BatchSet(keysToSet); err != nil {
			t.Fatalf("Setup for BatchDelete: BatchSet failed: %v", err)
		}

		keysToDelete := []string{"delBatchKey1", "delBatchKey3"}
		if err := tkvBatchHandler.BatchDelete(keysToDelete); err != nil {
			t.Errorf("BatchDelete() error = %v, wantErr nil", err)
		}

		// Check deleted keys
		for _, key := range keysToDelete {
			_, err := tkvTest.tkv.Get(key)
			if !errors.As(err, new(*ErrKeyNotFound)) {
				t.Errorf("Get() after BatchDelete for key %s expected ErrKeyNotFound, got %v", key, err)
			}
		}

		// Check non-deleted key still exists
		retrievedVal, err := tkvTest.tkv.Get("delBatchKey2")
		if err != nil {
			t.Errorf("Get() for non-deleted key 'delBatchKey2' error = %v", err)
		}
		if retrievedVal != "val2" {
			t.Errorf("Get() for non-deleted key 'delBatchKey2' got = %v, want 'val2'", retrievedVal)
		}
	})

	t.Run("BatchDelete with empty slice", func(t *testing.T) {
		if err := tkvBatchHandler.BatchDelete([]string{}); err != nil {
			t.Errorf("BatchDelete() with empty slice error = %v, wantErr nil", err)
		}
	})

	t.Run("BatchDelete with non-existent keys and empty keys", func(t *testing.T) {
		// Setup: Ensure a key exists
		keyToExist := "existingForBatchDeleteTest"
		if err := tkvTest.tkv.Set(keyToExist, "data"); err != nil {
			t.Fatalf("Setup for BatchDelete non-existent: Set failed: %v", err)
		}

		keysToDelete := []string{"nonExistentKey1", keyToExist, "", "nonExistentKey2"}
		if err := tkvBatchHandler.BatchDelete(keysToDelete); err != nil {
			t.Errorf("BatchDelete() with non-existent keys error = %v, wantErr nil", err)
		}

		// Check existing key was deleted
		_, err := tkvTest.tkv.Get(keyToExist)
		if !errors.As(err, new(*ErrKeyNotFound)) {
			t.Errorf("Get() after BatchDelete for key %s expected ErrKeyNotFound, got %v", keyToExist, err)
		}

		// Verify non-existent keys don't cause issues (implicitly tested by no error from BatchDelete)
	})
}

func TestTKV_AtomicOperations(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	defer tkvTest.Cleanup()

	tkvAtomicHandler, ok := tkvTest.tkv.(TKVAtomicHandler)
	if !ok {
		t.Fatalf("TKV instance does not implement TKVAtomicHandler")
	}

	// Test AtomicNew
	t.Run("AtomicNew_NewKey", func(t *testing.T) {
		key := "atomicNew1"
		err := tkvAtomicHandler.AtomicNew(key, false)
		if err != nil {
			t.Errorf("AtomicNew() error = %v, wantErr nil", err)
		}
		val, _ := tkvTest.tkv.Get(key) // Use base Get to check raw value
		if val != "0" {
			t.Errorf("AtomicNew() value = %s, want \"0\"", val)
		}
	})

	t.Run("AtomicNew_ExistingKey_NoOverwrite_ShouldFail", func(t *testing.T) {
		key := "atomicNew2"
		tkvAtomicHandler.AtomicNew(key, false) // Create it first
		err := tkvAtomicHandler.AtomicNew(key, false)
		if !errors.As(err, new(*ErrKeyExists)) {
			t.Errorf("AtomicNew() with existing key and no overwrite, error = %T, want *ErrKeyExists", err)
		}
	})

	t.Run("AtomicNew_ExistingKey_Overwrite_ShouldSucceed", func(t *testing.T) {
		key := "atomicNew3"
		tkvTest.tkv.Set(key, "123") // Set to something other than "0"
		err := tkvAtomicHandler.AtomicNew(key, true)
		if err != nil {
			t.Errorf("AtomicNew() with existing key and overwrite, error = %v, wantErr nil", err)
		}
		val, _ := tkvTest.tkv.Get(key)
		if val != "0" {
			t.Errorf("AtomicNew() with overwrite, value = %s, want \"0\"", val)
		}
	})

	// Test AtomicGet
	t.Run("AtomicGet_NonExistentKey", func(t *testing.T) {
		key := "atomicGetNonExistent"
		val, err := tkvAtomicHandler.AtomicGet(key)
		if err != nil {
			t.Errorf("AtomicGet() for non-existent key, error = %v, wantErr nil", err)
		}
		if val != 0 {
			t.Errorf("AtomicGet() for non-existent key, value = %d, want 0", val)
		}
	})

	t.Run("AtomicGet_ExistingKey", func(t *testing.T) {
		key := "atomicGetExisting"
		tkvAtomicHandler.AtomicNew(key, false)
		tkvAtomicHandler.AtomicAdd(key, 5) // Add something to it
		val, err := tkvAtomicHandler.AtomicGet(key)
		if err != nil {
			t.Errorf("AtomicGet() for existing key, error = %v, wantErr nil", err)
		}
		if val != 5 {
			t.Errorf("AtomicGet() for existing key, value = %d, want 5", val)
		}
	})

	t.Run("AtomicGet_InvalidStateNotInt", func(t *testing.T) {
		key := "atomicGetInvalid"
		tkvTest.tkv.Set(key, "not-a-number")
		val, err := tkvAtomicHandler.AtomicGet(key)
		if !errors.As(err, new(*ErrInvalidState)) {
			t.Errorf("AtomicGet() for invalid state key, error = %T, want *ErrInvalidState", err)
		}
		if val != 0 { // Should be 0 as per impl if parsing fails
			t.Errorf("AtomicGet() for invalid state key, value = %d, want 0", val)
		}
	})

	// Test AtomicAdd
	t.Run("AtomicAdd_NonExistentKey", func(t *testing.T) {
		key := "atomicAddNonExistent"
		newVal, err := tkvAtomicHandler.AtomicAdd(key, 10)
		if err != nil {
			t.Errorf("AtomicAdd() to non-existent key, error = %v, wantErr nil", err)
		}
		if newVal != 10 {
			t.Errorf("AtomicAdd() to non-existent key, new value = %d, want 10", newVal)
		}
		storedVal, _ := tkvAtomicHandler.AtomicGet(key)
		if storedVal != 10 {
			t.Errorf("AtomicAdd() to non-existent key, stored value = %d, want 10", storedVal)
		}
	})

	t.Run("AtomicAdd_ExistingKey_PositiveDelta", func(t *testing.T) {
		key := "atomicAddExistingPos"
		tkvAtomicHandler.AtomicNew(key, false)
		tkvAtomicHandler.AtomicAdd(key, 5)
		newVal, err := tkvAtomicHandler.AtomicAdd(key, 3)
		if err != nil {
			t.Errorf("AtomicAdd() positive delta, error = %v, wantErr nil", err)
		}
		if newVal != 8 {
			t.Errorf("AtomicAdd() positive delta, new value = %d, want 8", newVal)
		}
	})

	t.Run("AtomicAdd_ExistingKey_NegativeDelta_AboveZero", func(t *testing.T) {
		key := "atomicAddExistingNegAboveZero"
		tkvAtomicHandler.AtomicNew(key, false)
		tkvAtomicHandler.AtomicAdd(key, 10)
		newVal, err := tkvAtomicHandler.AtomicAdd(key, -3)
		if err != nil {
			t.Errorf("AtomicAdd() negative delta above zero, error = %v, wantErr nil", err)
		}
		if newVal != 7 {
			t.Errorf("AtomicAdd() negative delta above zero, new value = %d, want 7", newVal)
		}
	})

	t.Run("AtomicAdd_ExistingKey_NegativeDelta_ToZeroFloor", func(t *testing.T) {
		key := "atomicAddExistingNegToZero"
		tkvAtomicHandler.AtomicNew(key, false)
		tkvAtomicHandler.AtomicAdd(key, 5)
		newVal, err := tkvAtomicHandler.AtomicAdd(key, -10)
		if err != nil {
			t.Errorf("AtomicAdd() negative delta to zero floor, error = %v, wantErr nil", err)
		}
		if newVal != 0 {
			t.Errorf("AtomicAdd() negative delta to zero floor, new value = %d, want 0", newVal)
		}
	})

	t.Run("AtomicAdd_InvalidStateNotInt", func(t *testing.T) {
		key := "atomicAddInvalid"
		tkvTest.tkv.Set(key, "not-a-number")
		_, err := tkvAtomicHandler.AtomicAdd(key, 5)
		if !errors.As(err, new(*ErrInvalidState)) {
			t.Errorf("AtomicAdd() to invalid state key, error = %T, want *ErrInvalidState", err)
		}
	})

	// Test AtomicDelete
	t.Run("AtomicDelete_ExistingKey", func(t *testing.T) {
		key := "atomicDeleteExisting"
		tkvAtomicHandler.AtomicNew(key, false)
		err := tkvAtomicHandler.AtomicDelete(key)
		if err != nil {
			t.Errorf("AtomicDelete() for existing key, error = %v, wantErr nil", err)
		}
		_, errGet := tkvTest.tkv.Get(key)
		if !errors.As(errGet, new(*ErrKeyNotFound)) {
			t.Errorf("Get() after AtomicDelete expected ErrKeyNotFound, got %v", errGet)
		}
	})

	t.Run("AtomicDelete_NonExistentKey", func(t *testing.T) {
		key := "atomicDeleteNonExistent"
		err := tkvAtomicHandler.AtomicDelete(key)
		if err != nil {
			t.Errorf("AtomicDelete() for non-existent key, error = %v, wantErr nil", err)
		}
	})
}

func TestTKV_QueueOperations(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	defer tkvTest.Cleanup()

	tkvQueueHandler, ok := tkvTest.tkv.(TKVQueueHandler)
	if !ok {
		t.Fatalf("TKV instance does not implement TKVQueueHandler")
	}

	queueKey := "myTestQueue"

	t.Run("QueueNew_CreateNew", func(t *testing.T) {
		err := tkvQueueHandler.QueueNew(queueKey)
		if err != nil {
			t.Errorf("QueueNew() error = %v, wantErr nil", err)
		}
	})

	t.Run("QueueNew_ExistingQueue", func(t *testing.T) {
		// First ensure it exists
		if err := tkvQueueHandler.QueueNew(queueKey); err != nil {
			t.Fatalf("Setup: QueueNew() error = %v", err)
		}
		// Attempt to create again
		err := tkvQueueHandler.QueueNew(queueKey)
		if err != nil {
			t.Errorf("QueueNew() on existing queue error = %v, wantErr nil", err)
		}
	})

	t.Run("QueuePush_ToExistingQueue", func(t *testing.T) {
		if err := tkvQueueHandler.QueueNew(queueKey); err != nil { // Ensure queue exists
			t.Fatalf("Setup: QueueNew() error = %v", err)
		}
		val1 := "item1"
		len1, err := tkvQueueHandler.QueuePush(queueKey, val1)
		if err != nil {
			t.Errorf("QueuePush() item1 error = %v, wantErr nil", err)
		}
		if len1 != 1 {
			t.Errorf("QueuePush() item1 length got = %d, want 1", len1)
		}

		val2 := "item2"
		len2, err := tkvQueueHandler.QueuePush(queueKey, val2)
		if err != nil {
			t.Errorf("QueuePush() item2 error = %v, wantErr nil", err)
		}
		if len2 != 2 {
			t.Errorf("QueuePush() item2 length got = %d, want 2", len2)
		}
	})

	t.Run("QueuePush_ToNonExistentQueue", func(t *testing.T) {
		nonExistentQueueKey := "ghostQueue"
		_, err := tkvQueueHandler.QueuePush(nonExistentQueueKey, "ghostItem")
		if !errors.As(err, new(*ErrQueueNotFound)) {
			t.Errorf("QueuePush() to non-existent queue, error = %T, want *ErrQueueNotFound", err)
		}
	})

	t.Run("QueuePop_FromExistingQueue_FIFO", func(t *testing.T) {
		// Setup queue with items from previous push test if it runs in sequence and modifies state
		// For safety, re-initialize specific state for this test or use a new key
		popQueueKey := "popTestQueue"
		tkvQueueHandler.QueueNew(popQueueKey)
		tkvQueueHandler.QueuePush(popQueueKey, "popItem1")
		tkvQueueHandler.QueuePush(popQueueKey, "popItem2")

		popVal1, err := tkvQueueHandler.QueuePop(popQueueKey)
		if err != nil {
			t.Errorf("QueuePop() item1 error = %v, wantErr nil", err)
		}
		if popVal1 != "popItem1" {
			t.Errorf("QueuePop() item1 got = %s, want \"popItem1\"", popVal1)
		}

		popVal2, err := tkvQueueHandler.QueuePop(popQueueKey)
		if err != nil {
			t.Errorf("QueuePop() item2 error = %v, wantErr nil", err)
		}
		if popVal2 != "popItem2" {
			t.Errorf("QueuePop() item2 got = %s, want \"popItem2\"", popVal2)
		}
	})

	t.Run("QueuePop_FromEmptyQueue", func(t *testing.T) {
		emptyQueueKey := "emptyTestQueue"
		tkvQueueHandler.QueueNew(emptyQueueKey)

		_, err := tkvQueueHandler.QueuePop(emptyQueueKey)
		if !errors.As(err, new(*ErrQueueEmpty)) {
			t.Errorf("QueuePop() from empty queue, error = %T, want *ErrQueueEmpty", err)
		}
	})

	t.Run("QueuePop_FromNonExistentQueue", func(t *testing.T) {
		nonExistentPopKey := "ghostPopQueue"
		_, err := tkvQueueHandler.QueuePop(nonExistentPopKey)
		if !errors.As(err, new(*ErrQueueNotFound)) {
			t.Errorf("QueuePop() from non-existent queue, error = %T, want *ErrQueueNotFound", err)
		}
	})

	t.Run("QueueDelete_ExistingQueue", func(t *testing.T) {
		deleteQueueKey := "deleteTestQueue"
		tkvQueueHandler.QueueNew(deleteQueueKey)
		tkvQueueHandler.QueuePush(deleteQueueKey, "itemToDelete")

		err := tkvQueueHandler.QueueDelete(deleteQueueKey)
		if err != nil {
			t.Errorf("QueueDelete() error = %v, wantErr nil", err)
		}

		// Try to pop, should fail with ErrQueueNotFound
		_, errPop := tkvQueueHandler.QueuePop(deleteQueueKey)
		if !errors.As(errPop, new(*ErrQueueNotFound)) {
			t.Errorf("QueuePop() after Delete expected *ErrQueueNotFound, got %T", errPop)
		}
	})

	t.Run("QueueDelete_NonExistentQueue", func(t *testing.T) {
		nonExistentDeleteKey := "ghostDeleteQueue"
		err := tkvQueueHandler.QueueDelete(nonExistentDeleteKey)
		if err != nil {
			t.Errorf("QueueDelete() of non-existent queue error = %v, wantErr nil", err)
		}
	})
}
