package tkv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/InsulaLabs/insula/security/badge"
	"github.com/dgraph-io/badger/v3"
	"github.com/jellydator/ttlcache/v3"
)

type objectManifest struct {
	SizeBytes int       `json:"size_bytes"`
	Chunks    int       `json:"chunks"`
	UpdatedAt time.Time `json:"updated_at"`
	Indicies  []string  `json:"indicies"` // the indicies of the chunks in the db (keys)
}

var DefaultCacheTTL = 1 * time.Minute
var DefaultSecureCacheTTL = 1 * time.Minute

type tkv struct {
	logger          *slog.Logger
	appCtx          context.Context // if this is cancelled we need to prepare to receive Close() [but WE dont call Close() - Owner does]
	db              *data
	defaultCacheTTL time.Duration
	identity        badge.Badge
}

var _ TKV = &tkv{}

func New(config Config) (TKV, error) {

	valuesDir := filepath.Join(config.Directory, "values")
	objectsDir := filepath.Join(config.Directory, "objects")

	if err := os.MkdirAll(valuesDir, 0755); err != nil {
		return nil, &ErrInternal{Err: err}
	}

	if err := os.MkdirAll(objectsDir, 0755); err != nil {
		return nil, &ErrInternal{Err: err}
	}

	db, err := badger.Open(badger.DefaultOptions(valuesDir).WithLogger(newLogger(config.Logger.WithGroup("store"))))
	if err != nil {
		return nil, &ErrInternal{Err: err}
	}

	objects, err := badger.Open(badger.DefaultOptions(objectsDir).WithLogger(newLogger(config.Logger.WithGroup("objects"))))
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
			store:   db,
			cache:   cache,
			objects: objects,
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

	if t.db.objects != nil {
		t.logger.Info("objects db stopped")
		if err := t.db.objects.Close(); err != nil {
			t.logger.Error("error closing objects db", "error", err)
			firstErr = &ErrInternal{Err: err}
		}
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
	var values []string
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
			val, err := item.ValueCopy(nil)
			if err != nil {
				return &ErrInternal{Err: err}
			}
			values = append(values, string(val))
			collected++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, nil
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

func (t *tkv) GetObjectsDB() *badger.DB {
	return t.db.objects
}

func (t *tkv) SetObject(key string, data []byte) error {
	manifest := objectManifest{
		SizeBytes: len(data),
		UpdatedAt: time.Now(),
	}

	chunkSize := 1024 * 1024 // 1MB
	numChunks := (len(data) + chunkSize - 1) / chunkSize
	manifest.Chunks = numChunks
	manifest.Indicies = make([]string, numChunks)

	err := t.db.objects.Update(func(txn *badger.Txn) error {
		for i := 0; i < numChunks; i++ {
			start := i * chunkSize
			end := (i + 1) * chunkSize
			if end > len(data) {
				end = len(data)
			}
			chunk := data[start:end]
			chunkKey := fmt.Sprintf("%s:chunk:%d", key, i)
			manifest.Indicies[i] = chunkKey

			if err := txn.Set([]byte(chunkKey), chunk); err != nil {
				return fmt.Errorf("failed to set chunk %s: %w", chunkKey, err)
			}
		}

		manifestBytes, err := json.Marshal(manifest)
		if err != nil {
			return fmt.Errorf("failed to marshal object manifest for key %s: %w", key, err)
		}

		if err := txn.Set([]byte(key), manifestBytes); err != nil {
			return fmt.Errorf("failed to set object manifest for key %s: %w", key, err)
		}

		// Add the object index key
		indexKey := fmt.Sprintf("objidx::%s", key)
		if err := txn.Set([]byte(indexKey), []byte{}); err != nil { // Store empty value for the index
			return fmt.Errorf("failed to set object index key %s: %w", indexKey, err)
		}
		return nil
	})

	if err != nil {
		return &ErrInternal{Err: err}
	}
	return nil
}

func (t *tkv) GetObject(key string) ([]byte, error) {
	var assembledData []byte
	err := t.db.objects.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return &ErrKeyNotFound{Key: key}
			}
			return fmt.Errorf("failed to get object manifest for key %s: %w", key, err)
		}

		manifestBytes, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to copy object manifest value for key %s: %w", key, err)
		}

		var manifest objectManifest
		if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
			return fmt.Errorf("failed to unmarshal object manifest for key %s: %w", key, err)
		}

		var buffer bytes.Buffer
		buffer.Grow(manifest.SizeBytes)

		for _, chunkKey := range manifest.Indicies {
			chunkItem, err := txn.Get([]byte(chunkKey))
			if err != nil {
				// If a chunk is not found, it indicates data corruption or an incomplete write.
				if errors.Is(err, badger.ErrKeyNotFound) {
					return &ErrDataCorruption{Key: key, Reason: fmt.Sprintf("chunk %s not found", chunkKey)}
				}
				return fmt.Errorf("failed to get chunk %s for object %s: %w", chunkKey, key, err)
			}
			chunkData, err := chunkItem.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("failed to copy chunk %s value for object %s: %w", chunkKey, key, err)
			}
			buffer.Write(chunkData)
		}

		if buffer.Len() != manifest.SizeBytes {
			return &ErrDataCorruption{Key: key, Reason: fmt.Sprintf("reconstructed size %d does not match manifest size %d", buffer.Len(), manifest.SizeBytes)}
		}
		assembledData = buffer.Bytes()
		return nil
	})

	if err != nil {
		// Wrap specific errors if they are not already wrapped, otherwise return as is or wrap with ErrInternal
		switch err.(type) {
		case *ErrKeyNotFound, *ErrDataCorruption:
			return nil, err
		default:
			return nil, &ErrInternal{Err: err}
		}
	}
	return assembledData, nil
}

func (t *tkv) DeleteObject(key string) error {
	err := t.db.objects.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				// Object doesn't exist, consider deletion successful (idempotent)
				return nil
			}
			return fmt.Errorf("failed to get object manifest for key %s during deletion: %w", key, err)
		}

		manifestBytes, err := item.ValueCopy(nil)
		if err != nil {
			// If we can't copy the manifest, we can't find the chunks to delete.
			return fmt.Errorf("failed to copy object manifest value for key %s during deletion: %w", key, err)
		}

		var manifest objectManifest
		if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
			// If manifest is corrupt, we can still delete the main key, but chunks might be orphaned.
			// Log this, then proceed to delete the main key.
			t.logger.Error("failed to unmarshal object manifest during deletion, chunks may be orphaned", "key", key, "error", err)
		} else {
			for _, chunkKey := range manifest.Indicies {
				if err := txn.Delete([]byte(chunkKey)); err != nil {
					// Log error but continue trying to delete other chunks and the manifest.
					t.logger.Error("failed to delete chunk during object deletion", "chunkKey", chunkKey, "objectKey", key, "error", err)
				}
			}
		}

		// Delete the object index key
		indexKey := fmt.Sprintf("objidx::%s", key)
		if err := txn.Delete([]byte(indexKey)); err != nil {
			// Log error but continue, as the primary goal is to delete the object data
			t.logger.Error("failed to delete object index key during object deletion", "indexKey", indexKey, "objectKey", key, "error", err)
		}

		if err := txn.Delete([]byte(key)); err != nil {
			return fmt.Errorf("failed to delete object manifest key %s: %w", key, err)
		}
		return nil
	})

	if err != nil {
		return &ErrInternal{Err: err}
	}
	return nil
}

const objectIndexPrefix = "objidx::"

func (t *tkv) GetObjectList(prefix string, offset int, limit int) ([]string, error) {

	var keys []string
	err := t.db.objects.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		searchPrefix := []byte(objectIndexPrefix + prefix)
		skipped := 0
		collected := 0

		for it.Seek(searchPrefix); it.ValidForPrefix(searchPrefix); it.Next() {
			if skipped < offset {
				skipped++
				continue
			}
			if limit > 0 && collected >= limit {
				break
			}
			item := it.Item()
			keyBytes := item.Key()
			// Remove the "objidx::" prefix to get the actual object key
			actualKey := string(bytes.TrimPrefix(keyBytes, []byte(objectIndexPrefix)))
			keys = append(keys, actualKey)
			collected++
		}
		return nil
	})

	if err != nil {
		return nil, &ErrInternal{Err: err}
	}
	return keys, nil
}
