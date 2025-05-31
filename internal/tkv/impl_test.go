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

	"bytes"

	"github.com/dgraph-io/badger/v3"
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
		expectedValues := []string{"value1", "value2", "value3"}
		retrievedValues, err := tkvTest.tkv.Iterate(prefix, 0, 0) // No offset, no limit
		if err != nil {
			t.Errorf("Iterate() error = %v, wantErr nil", err)
		}
		// Sort slices because iteration order is not guaranteed for all values with the same prefix but different full keys
		sort.Strings(retrievedValues)
		sort.Strings(expectedValues)
		if !reflect.DeepEqual(retrievedValues, expectedValues) {
			t.Errorf("Iterate() got = %v, want %v", retrievedValues, expectedValues)
		}
	})

	t.Run("Iterate with prefix and offset", func(t *testing.T) {
		prefix := "prefix_"
		offset := 1
		expectedValues := []string{"value2", "value3"} // key1 is skipped
		retrievedValues, err := tkvTest.tkv.Iterate(prefix, offset, 0)
		if err != nil {
			t.Errorf("Iterate() error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedValues)
		sort.Strings(expectedValues)
		if !reflect.DeepEqual(retrievedValues, expectedValues) {
			t.Errorf("Iterate() got = %v, want %v", retrievedValues, expectedValues)
		}
	})

	t.Run("Iterate with prefix and limit", func(t *testing.T) {
		prefix := "prefix_"
		limit := 2
		expectedValues := []string{"value1", "value2"} // only first two
		retrievedValues, err := tkvTest.tkv.Iterate(prefix, 0, limit)
		if err != nil {
			t.Errorf("Iterate() error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedValues)
		sort.Strings(expectedValues)
		if !reflect.DeepEqual(retrievedValues, expectedValues) {
			t.Errorf("Iterate() got = %v, want %v", retrievedValues, expectedValues)
		}
	})

	t.Run("Iterate with prefix, offset, and limit", func(t *testing.T) {
		prefix := "prefix_"
		offset := 1
		limit := 1
		expectedValues := []string{"value2"} // skip 1, take 1
		retrievedValues, err := tkvTest.tkv.Iterate(prefix, offset, limit)
		if err != nil {
			t.Errorf("Iterate() error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedValues)
		sort.Strings(expectedValues)
		if !reflect.DeepEqual(retrievedValues, expectedValues) {
			t.Errorf("Iterate() got = %v, want %v", retrievedValues, expectedValues)
		}
	})

	t.Run("Iterate with non-matching prefix", func(t *testing.T) {
		prefix := "non_matching_prefix_"
		expectedValues := []string{}
		retrievedValues, err := tkvTest.tkv.Iterate(prefix, 0, 0)
		if err != nil {
			t.Errorf("Iterate() error = %v, wantErr nil", err)
		}
		if len(retrievedValues) != 0 {
			t.Errorf("Iterate() got = %v, want %v", retrievedValues, expectedValues)
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

func TestTKV_ObjectHandler(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	defer tkvTest.Cleanup()

	tkvObjectHandler, ok := tkvTest.tkv.(TKVObjectHandler)
	if !ok {
		t.Fatalf("TKV instance does not implement TKVObjectHandler")
	}

	t.Run("Set and Get small object", func(t *testing.T) {
		key := "objectKeySmall"
		data := []byte("this is a small object")

		if err := tkvObjectHandler.SetObject(key, data); err != nil {
			t.Errorf("SetObject() error = %v, wantErr nil", err)
		}

		retrievedData, err := tkvObjectHandler.GetObject(key)
		if err != nil {
			t.Errorf("GetObject() error = %v, wantErr nil", err)
		}
		if !reflect.DeepEqual(retrievedData, data) {
			t.Errorf("GetObject() got = %s, want %s", string(retrievedData), string(data))
		}
	})

	t.Run("Set and Get large object (multi-chunk)", func(t *testing.T) {
		key := "objectKeyLarge"
		// Create data larger than 1MB to ensure chunking
		// chunkSize in impl is 1024 * 1024
		largeDataSize := (1024 * 1024 * 2) + 100 // একটু বেশি 2MB
		data := make([]byte, largeDataSize)
		for i := 0; i < largeDataSize; i++ {
			data[i] = byte(i % 256)
		}

		if err := tkvObjectHandler.SetObject(key, data); err != nil {
			t.Errorf("SetObject() for large object error = %v, wantErr nil", err)
		}

		retrievedData, err := tkvObjectHandler.GetObject(key)
		if err != nil {
			t.Errorf("GetObject() for large object error = %v, wantErr nil", err)
		}
		if len(retrievedData) != len(data) {
			t.Errorf("GetObject() for large object length got = %d, want %d", len(retrievedData), len(data))
		}
		if !reflect.DeepEqual(retrievedData, data) {
			t.Errorf("GetObject() for large object data mismatch")
		}
	})

	t.Run("Set and Get empty object", func(t *testing.T) {
		key := "objectKeyEmpty"
		data := []byte{}

		if err := tkvObjectHandler.SetObject(key, data); err != nil {
			t.Errorf("SetObject() for empty object error = %v, wantErr nil", err)
		}

		retrievedData, err := tkvObjectHandler.GetObject(key)
		if err != nil {
			t.Errorf("GetObject() for empty object error = %v, wantErr nil", err)
		}
		// Check if both are nil or empty, DeepEqual might be tricky with nil vs empty slices
		if !((retrievedData == nil && data == nil) || (len(retrievedData) == 0 && len(data) == 0)) {
			if !reflect.DeepEqual(retrievedData, data) { // Fallback to DeepEqual which should handle empty slices correctly
				t.Errorf("GetObject() for empty object got = %v, want %v", retrievedData, data)
			}
		}
	})

	t.Run("Get non-existent object", func(t *testing.T) {
		key := "nonExistentObjectKey"
		_, err := tkvObjectHandler.GetObject(key)
		if err == nil {
			t.Errorf("GetObject() expected error for non-existent key, got nil")
		}
		var keyNotFound *ErrKeyNotFound
		if !errors.As(err, &keyNotFound) {
			t.Errorf("GetObject() expected ErrKeyNotFound, got %T (%v)", err, err)
		}
		if keyNotFound.Key != key {
			t.Errorf("ErrKeyNotFound.Key got = %s, want %s", keyNotFound.Key, key)
		}
	})

	t.Run("Delete object and verify", func(t *testing.T) {
		key := "objectKeyToBeDeleted"
		data := []byte("some data to delete")

		if err := tkvObjectHandler.SetObject(key, data); err != nil {
			t.Fatalf("Setup: SetObject() error = %v", err)
		}

		if err := tkvObjectHandler.DeleteObject(key); err != nil {
			t.Errorf("DeleteObject() error = %v, wantErr nil", err)
		}

		_, err := tkvObjectHandler.GetObject(key)
		var keyNotFound *ErrKeyNotFound
		if !errors.As(err, &keyNotFound) {
			t.Errorf("GetObject() after DeleteObject expected ErrKeyNotFound, got %T (%v)", err, err)
		}
	})

	t.Run("Delete non-existent object", func(t *testing.T) {
		key := "nonExistentObjectKeyForDelete"
		if err := tkvObjectHandler.DeleteObject(key); err != nil {
			t.Errorf("DeleteObject() of non-existent key error = %v, wantErr nil", err)
		}
	})

	// Test for potential data corruption if a chunk is missing
	t.Run("Get object with missing chunk (simulated corruption)", func(t *testing.T) {
		key := "corruptedObjectKey"
		data := make([]byte, (1024*1024)+10) // Ensure at least two chunks
		for i := range data {
			data[i] = byte(i)
		}

		if err := tkvObjectHandler.SetObject(key, data); err != nil {
			t.Fatalf("SetObject for corruption test failed: %v", err)
		}

		// Manually delete a chunk (this requires knowing the chunk key format)
		// The format used in impl.go is fmt.Sprintf("%s:chunk:%d", key, i)
		chunkToDeleteKey := fmt.Sprintf("%s:chunk:%d", key, 0) // Delete the first chunk

		db := tkvTest.tkv.GetObjectsDB() // Use GetObjectsDB() here
		err = db.Update(func(txn *badger.Txn) error {
			return txn.Delete([]byte(chunkToDeleteKey))
		})
		if err != nil {
			t.Fatalf("Failed to manually delete chunk for corruption test: %v", err)
		}

		_, err = tkvObjectHandler.GetObject(key)
		if err == nil {
			t.Errorf("GetObject() expected error for corrupted data, got nil")
		}
		var dataCorruptionErr *ErrDataCorruption
		if !errors.As(err, &dataCorruptionErr) {
			t.Errorf("GetObject() expected ErrDataCorruption, got %T (%v)", err, err)
		} else {
			if dataCorruptionErr.Key != key {
				t.Errorf("ErrDataCorruption.Key got = %s, want %s", dataCorruptionErr.Key, key)
			}
			t.Logf("Successfully caught expected data corruption: %v", dataCorruptionErr)
		}
	})

	t.Run("Set and Get very large object (600MB)", func(t *testing.T) {
		key := "objectKeyVeryLarge"
		// Create data of 600MB
		veryLargeDataSize := 600 * 1024 * 1024
		data := make([]byte, veryLargeDataSize)
		// Fill with some pattern to ensure content is checked, not just size
		for i := 0; i < veryLargeDataSize; i++ {
			data[i] = byte(i % 256)
		}

		t.Logf("Attempting to set object of size: %d bytes", veryLargeDataSize)
		if err := tkvObjectHandler.SetObject(key, data); err != nil {
			t.Errorf("SetObject() for very large object error = %v, wantErr nil", err)
			return
		}
		t.Log("Successfully set very large object")

		t.Log("Attempting to get very large object")
		retrievedData, err := tkvObjectHandler.GetObject(key)
		if err != nil {
			t.Errorf("GetObject() for very large object error = %v, wantErr nil", err)
			return
		}
		t.Log("Successfully retrieved very large object")

		if len(retrievedData) != len(data) {
			t.Errorf("GetObject() for very large object length got = %d, want %d", len(retrievedData), len(data))
			return // Important to return if lengths don't match before byte comparison
		}

		t.Log("Comparing retrieved data with original data (this might take a moment for 600MB)")
		if !bytes.Equal(retrievedData, data) { // bytes.Equal is more efficient for large slices
			t.Errorf("GetObject() for very large object data mismatch")
		}
		t.Log("Successfully compared very large object data")
	})

}

func TestTKV_ObjectList(t *testing.T) {
	ctx := context.Background()
	tkvTest, err := createTestTKV(ctx)
	if err != nil {
		t.Fatalf("Failed to create test TKV: %v", err)
	}
	defer tkvTest.Cleanup()

	tkvObjectHandler, ok := tkvTest.tkv.(TKVObjectHandler)
	if !ok {
		t.Fatalf("TKV instance does not implement TKVObjectHandler")
	}

	// Setup: Add some objects
	objectKeys := map[string][]byte{
		"list_obj_aaa":   []byte("data_aaa"),
		"list_obj_aab":   []byte("data_aab"),
		"list_obj_abb":   []byte("data_abb"),
		"list_other_ccc": []byte("data_ccc"),
	}

	expectedAllObjectKeys := make([]string, 0, len(objectKeys))
	for k, v := range objectKeys {
		if err := tkvObjectHandler.SetObject(k, v); err != nil {
			t.Fatalf("Setup: SetObject() error for key %s: %v", k, err)
		}
		expectedAllObjectKeys = append(expectedAllObjectKeys, k)
	}
	sort.Strings(expectedAllObjectKeys)

	t.Run("List all objects", func(t *testing.T) {
		retrievedKeys, err := tkvObjectHandler.GetObjectList("", 0, 0) // No prefix, no offset, no limit
		if err != nil {
			t.Errorf("GetObjectList() error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedAllObjectKeys) {
			t.Errorf("GetObjectList() got = %v, want %v", retrievedKeys, expectedAllObjectKeys)
		}
	})

	t.Run("List objects with prefix", func(t *testing.T) {
		prefix := "list_obj_a"
		expectedKeys := []string{"list_obj_aaa", "list_obj_aab", "list_obj_abb"} // abb should also be here
		retrievedKeys, err := tkvObjectHandler.GetObjectList(prefix, 0, 0)
		if err != nil {
			t.Errorf("GetObjectList() with prefix error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		sort.Strings(expectedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeys) {
			t.Errorf("GetObjectList() with prefix got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})

	t.Run("List objects with prefix and offset", func(t *testing.T) {
		prefix := "list_obj_"
		offset := 1
		// expectedAllObjectKeys sorted: [list_obj_aaa, list_obj_aab, list_obj_abb, list_other_ccc]
		// with prefix list_obj_ : [list_obj_aaa, list_obj_aab, list_obj_abb]
		// with offset 1: [list_obj_aab, list_obj_abb]
		expectedKeys := []string{"list_obj_aab", "list_obj_abb"}
		retrievedKeys, err := tkvObjectHandler.GetObjectList(prefix, offset, 0)
		if err != nil {
			t.Errorf("GetObjectList() with prefix and offset error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		sort.Strings(expectedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeys) {
			t.Errorf("GetObjectList() with prefix and offset got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})

	t.Run("List objects with prefix and limit", func(t *testing.T) {
		prefix := "list_obj_"
		limit := 2
		// with prefix list_obj_ : [list_obj_aaa, list_obj_aab, list_obj_abb]
		// with limit 2: [list_obj_aaa, list_obj_aab]
		expectedKeys := []string{"list_obj_aaa", "list_obj_aab"}
		retrievedKeys, err := tkvObjectHandler.GetObjectList(prefix, 0, limit)
		if err != nil {
			t.Errorf("GetObjectList() with prefix and limit error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		sort.Strings(expectedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeys) {
			t.Errorf("GetObjectList() with prefix and limit got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})

	t.Run("List objects with prefix, offset, and limit", func(t *testing.T) {
		prefix := "list_obj_"
		offset := 1
		limit := 1
		// with prefix list_obj_ : [list_obj_aaa, list_obj_aab, list_obj_abb]
		// with offset 1: [list_obj_aab, list_obj_abb]
		// with limit 1: [list_obj_aab]
		expectedKeys := []string{"list_obj_aab"}
		retrievedKeys, err := tkvObjectHandler.GetObjectList(prefix, offset, limit)
		if err != nil {
			t.Errorf("GetObjectList() with prefix, offset, and limit error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		sort.Strings(expectedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeys) {
			t.Errorf("GetObjectList() with prefix, offset, and limit got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})

	t.Run("List with non-matching prefix", func(t *testing.T) {
		prefix := "non_matching_prefix_"
		expectedKeys := []string{}
		retrievedKeys, err := tkvObjectHandler.GetObjectList(prefix, 0, 0)
		if err != nil {
			t.Errorf("GetObjectList() error = %v, wantErr nil", err)
		}
		if len(retrievedKeys) != 0 {
			t.Errorf("GetObjectList() got = %v, want %v", retrievedKeys, expectedKeys)
		}
	})

	t.Run("List after deleting an object", func(t *testing.T) {
		keyToDelete := "list_obj_aab"
		if err := tkvObjectHandler.DeleteObject(keyToDelete); err != nil {
			t.Fatalf("DeleteObject() error for key %s: %v", keyToDelete, err)
		}

		expectedKeysAfterDelete := []string{"list_obj_aaa", "list_obj_abb", "list_other_ccc"}
		sort.Strings(expectedKeysAfterDelete)

		retrievedKeys, err := tkvObjectHandler.GetObjectList("", 0, 0) // List all
		if err != nil {
			t.Errorf("GetObjectList() after delete error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeysAfterDelete) {
			t.Errorf("GetObjectList() after delete got = %v, want %v", retrievedKeys, expectedKeysAfterDelete)
		}
	})

	t.Run("List after deleting all objects with a prefix", func(t *testing.T) {
		// Delete remaining objects starting with "list_obj_"
		keysToDelete := []string{"list_obj_aaa", "list_obj_abb"}
		for _, key := range keysToDelete {
			if err := tkvObjectHandler.DeleteObject(key); err != nil {
				t.Fatalf("DeleteObject() error for key %s: %v", key, err)
			}
		}

		expectedKeysAfterPrefixDelete := []string{"list_other_ccc"}
		sort.Strings(expectedKeysAfterPrefixDelete)

		retrievedKeys, err := tkvObjectHandler.GetObjectList("", 0, 0) // List all
		if err != nil {
			t.Errorf("GetObjectList() after prefix delete error = %v, wantErr nil", err)
		}
		sort.Strings(retrievedKeys)
		if !reflect.DeepEqual(retrievedKeys, expectedKeysAfterPrefixDelete) {
			t.Errorf("GetObjectList() after prefix delete got = %v, want %v", retrievedKeys, expectedKeysAfterPrefixDelete)
		}

		// Ensure listing with the deleted prefix yields no results
		retrievedKeysPrefix, err := tkvObjectHandler.GetObjectList("list_obj_", 0, 0)
		if err != nil {
			t.Errorf("GetObjectList() for deleted prefix error = %v, wantErr nil", err)
		}
		if len(retrievedKeysPrefix) != 0 {
			t.Errorf("GetObjectList() for deleted prefix got = %v, want empty", retrievedKeysPrefix)
		}
	})
}
