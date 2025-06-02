package tkv

import (
	"context"
	"log/slog"
	"time"

	"github.com/InsulaLabs/insula/security/badge"
	"github.com/dgraph-io/badger/v3"
	"github.com/jellydator/ttlcache/v3"
)

type Config struct {
	Identity  badge.Badge
	Logger    *slog.Logger
	Directory string
	AppCtx    context.Context
	CacheTTL  time.Duration
}

type data struct {
	store   *badger.DB
	cache   *ttlcache.Cache[string, string]
	objects *badger.DB
}

/*
// TODO:
*/
type TKVBatchEntry struct {
	Key   string
	Value string
}

// These are meant to happen "atomically" so that when fsm applies we dont have to worry about
// race conditions involving writes
type TKVAtomicHandler interface {
	AtomicNew(key string, overwrite bool) error       // if overwrite is true, the key will be deleted if it exists, else an error if it exists already
	AtomicGet(key string) (int64, error)              // get the value of the key, 0 if it doesn't exist
	AtomicAdd(key string, delta int64) (int64, error) // sub by adding negative delta. Atomics floor at 0
	AtomicDelete(key string) error                    // delete the key if it exists, no error if it doesn't
}

type TKVBatchHandler interface {
	BatchSet(entries []TKVBatchEntry) error
	BatchDelete(keys []string) error
}

type TKVDataHandler interface {
	Get(key string) (string, error)
	Iterate(prefix string, offset int, limit int) ([]string, error)
	Set(key string, value string) error
	Delete(key string) error
}

type TKVCacheHandler interface {
	CacheGet(key string) (string, error)
	CacheSet(key string, value string, ttl time.Duration) error
	CacheDelete(key string) error
}

type TKV interface {
	TKVDataHandler
	TKVCacheHandler
	TKVBatchHandler
	TKVAtomicHandler

	Close() error

	GetDataDB() *badger.DB
	GetCache() *ttlcache.Cache[string, string]
	GetObjectsDB() *badger.DB
}
