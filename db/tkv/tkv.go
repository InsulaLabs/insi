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
	Identity       badge.Badge
	Logger         *slog.Logger
	BadgerLogLevel slog.Level
	Directory      string
	AppCtx         context.Context
	CacheTTL       time.Duration
}

type data struct {
	store *badger.DB
	cache *ttlcache.Cache[string, string]
}

type TKVBatchEntry struct {
	Key   string
	Value string
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

	Close() error

	GetDataDB() *badger.DB
	GetCache() *ttlcache.Cache[string, string]
}
