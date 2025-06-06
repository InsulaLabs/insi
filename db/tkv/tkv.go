package tkv

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/InsulaLabs/insula/security/badge"
	"github.com/dgraph-io/badger/v3"
	"github.com/jellydator/ttlcache/v3"
)

type Config struct {
	Identity       badge.Badge
	Logger         *slog.Logger
	BadgerLogLevel slog.Level
	Directory      string
	AppCtx         context.Context
	CacheTTL       time.Duration
}

type data struct {
	store  *badger.DB
	cache  *ttlcache.Cache[string, string]
	queues map[string][]string
	qLock  sync.RWMutex
}

type TKVBatchEntry struct {
	Key   string
	Value string
}

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

type TKVQueueHandler interface {
	QueueNew(key string) error                       // create a new queue, no error if it already exists
	QueuePush(key string, value string) (int, error) // push the value to the queue, return the new length of the queue, error if the queue is full
	QueuePop(key string) (string, error)             // pop the first item from the queue, return the value, error if the queue is empty
	QueueDelete(key string) error                    // delete the queue if it exists, no error if it doesn't
}

type TKV interface {
	TKVDataHandler
	TKVCacheHandler
	TKVBatchHandler
	TKVAtomicHandler
	TKVQueueHandler

	Close() error

	GetDataDB() *badger.DB
	GetCache() *ttlcache.Cache[string, string]
}
