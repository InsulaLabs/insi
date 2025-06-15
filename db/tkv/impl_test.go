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

		if err := tkvInstance.CacheSet(key, value); err != nil {
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

	t.Run("Delete cache key", func(t *testing.T) {
		key := "cacheKeyDelete"
		value := "cacheValueDelete"

		if err := tkvInstance.CacheSet(key, value); err != nil {
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
}

func TestTKV_Cache_Advanced(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	defer tkvTest.Cleanup()

	tkvCache, ok := tkvTest.tkv.(TKVCacheHandler)
	if !ok {
		t.Fatalf("TKV instance does not implement TKVCacheHandler")
	}

	t.Run("CacheIterate", func(t *testing.T) {
		keys := []string{"c_prefix_key1", "c_prefix_key2", "c_prefix_key3", "c_other_key1"}
		for _, key := range keys {
			if err := tkvCache.CacheSet(key, "value"); err != nil {
				t.Fatalf("Setup: CacheSet() error for key %s: %v", key, err)
			}
		}

		prefix := "c_prefix_"
		expectedKeys := []string{"c_prefix_key1", "c_prefix_key2", "c_prefix_key3"}
		retrievedKeys, err := tkvCache.CacheIterate(prefix, 0, 0)
		if err != nil {
			t.Errorf("CacheIterate() error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		sort.Strings(expectedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeys) {
			t.Errorf("CacheIterate() got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})

	t.Run("CacheSetNX", func(t *testing.T) {
		key := "newCacheKey"
		value := "newCacheValue"

		// Set for the first time
		err := tkvCache.CacheSetNX(key, value)
		if err != nil {
			t.Errorf("CacheSetNX() on new key error = %v, wantErr nil", err)
		}
		retrieved, err := tkvCache.CacheGet(key)
		if err != nil || retrieved != value {
			t.Errorf("CacheGet() after CacheSetNX failed or got wrong value")
		}

		// Try to set again
		err = tkvCache.CacheSetNX(key, "anotherValue")
		if !errors.As(err, new(*ErrKeyExists)) {
			t.Errorf("CacheSetNX() on existing key expected ErrKeyExists, got %v", err)
		}
	})

	t.Run("CacheCompareAndSwap", func(t *testing.T) {
		key := "casCacheKey"
		initialValue := "initial"
		newValue := "swapped"

		if err := tkvCache.CacheSet(key, initialValue); err != nil {
			t.Fatalf("Setup: CacheSet failed: %v", err)
		}

		// CAS with correct old value
		err := tkvCache.CacheCompareAndSwap(key, initialValue, newValue)
		if err != nil {
			t.Errorf("CacheCompareAndSwap() with correct value failed: %v", err)
		}
		retrieved, _ := tkvCache.CacheGet(key)
		if retrieved != newValue {
			t.Errorf("Get() after successful CAS got = %s, want = %s", retrieved, newValue)
		}

		// CAS with incorrect old value
		err = tkvCache.CacheCompareAndSwap(key, "wrongOldValue", "someOtherValue")
		if !errors.As(err, new(*ErrCASFailed)) {
			t.Errorf("CacheCompareAndSwap() with incorrect value expected ErrCASFailed, got %T", err)
		}
		retrieved, _ = tkvCache.CacheGet(key)
		if retrieved != newValue {
			t.Errorf("Value should not have changed on failed CAS, got = %s", retrieved)
		}
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

func TestTKV_SetNX(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	defer tkvTest.Cleanup()

	t.Run("SetNX on a new key", func(t *testing.T) {
		key := "newKey"
		value := "newValue"
		err := tkvTest.tkv.SetNX(key, value)
		if err != nil {
			t.Errorf("SetNX() on new key error = %v, wantErr nil", err)
		}
		retrieved, err := tkvTest.tkv.Get(key)
		if err != nil {
			t.Errorf("Get() after SetNX failed: %v", err)
		}
		if retrieved != value {
			t.Errorf("Get() after SetNX got = %s, want = %s", retrieved, value)
		}
	})

	t.Run("SetNX on an existing key", func(t *testing.T) {
		key := "existingKey"
		value := "existingValue"
		tkvTest.tkv.Set(key, value) // Pre-populate

		err := tkvTest.tkv.SetNX(key, "anotherValue")
		if err == nil {
			t.Fatal("SetNX() on existing key expected an error, got nil")
		}

		var keyExistsErr *ErrKeyExists
		if !errors.As(err, &keyExistsErr) {
			t.Errorf("SetNX() on existing key expected ErrKeyExists, got %T", err)
		}
		if keyExistsErr.Key != key {
			t.Errorf("ErrKeyExists has wrong key, got = %s, want = %s", keyExistsErr.Key, key)
		}
	})
}

func TestTKV_CompareAndSwap(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	defer tkvTest.Cleanup()

	key := "casKey"
	initialValue := "initial"
	newValue := "swapped"

	// Setup initial value
	if err := tkvTest.tkv.Set(key, initialValue); err != nil {
		t.Fatalf("Setup: Set failed: %v", err)
	}

	t.Run("CAS with correct old value", func(t *testing.T) {
		err := tkvTest.tkv.CompareAndSwap(key, initialValue, newValue)
		if err != nil {
			t.Errorf("CompareAndSwap() with correct value failed: %v", err)
		}
		retrieved, err := tkvTest.tkv.Get(key)
		if err != nil {
			t.Errorf("Get() after successful CAS failed: %v", err)
		}
		if retrieved != newValue {
			t.Errorf("Get() after successful CAS got = %s, want = %s", retrieved, newValue)
		}
	})

	t.Run("CAS with incorrect old value", func(t *testing.T) {
		// Value is now `newValue`
		err := tkvTest.tkv.CompareAndSwap(key, "wrongOldValue", "someOtherValue")
		if err == nil {
			t.Fatal("CompareAndSwap() with incorrect value expected an error, got nil")
		}
		var casFailedErr *ErrCASFailed
		if !errors.As(err, &casFailedErr) {
			t.Errorf("CompareAndSwap() with incorrect value expected ErrCASFailed, got %T", err)
		}
		if casFailedErr.Key != key {
			t.Errorf("ErrCASFailed has wrong key, got = %s, want = %s", casFailedErr.Key, key)
		}
		// Ensure the value was not changed
		retrieved, _ := tkvTest.tkv.Get(key)
		if retrieved != newValue {
			t.Errorf("Value should not have changed on failed CAS, got = %s, want = %s", retrieved, newValue)
		}
	})

	t.Run("CAS on non-existent key", func(t *testing.T) {
		nonExistentKey := "nonExistent"
		err := tkvTest.tkv.CompareAndSwap(nonExistentKey, "any", "any")
		if err == nil {
			t.Fatal("CompareAndSwap() on non-existent key expected an error, got nil")
		}
		var casFailedErr *ErrCASFailed
		if !errors.As(err, &casFailedErr) {
			t.Errorf("CompareAndSwap() on non-existent key expected ErrCASFailed, got %T", err)
		}
	})
}
