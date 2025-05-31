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
	store *badger.DB
	cache *ttlcache.Cache[string, string]
}

/*

// TODO:


type TKVBatchEntry struct {
	Key   string
	Value string
}

type TKVBatchHandler interface {
	BatchSet(entries []TKVBatchEntry) error
	BatchDelete(keys []string) error
}
*/

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

	Close() error

	GetDataDB() *badger.DB
	GetCache() *ttlcache.Cache[string, string]
}
