package tkv

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/InsulaLabs/insula/security/badge"
	"github.com/dgraph-io/badger/v3"
	"github.com/jellydator/ttlcache/v3"
)

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

	db, err := badger.Open(badger.DefaultOptions(config.Directory).WithLogger(newLogger(config.Logger.WithGroup("store"))))
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

const (
	tagToKeyPrefix = "t2k:"
	keyToTagPrefix = "k2t:"
)

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
