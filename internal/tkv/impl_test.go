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
