package tkv

import (
	"context"
	"log/slog"

	"github.com/InsulaLabs/insula/security/badge"
	"github.com/dgraph-io/badger/v3"
)

type Config struct {
	Identity       badge.Badge
	Logger         *slog.Logger
	BadgerLogLevel slog.Level
	Directory      string
	AppCtx         context.Context
}

type data struct {
	store *badger.DB
	cache *badger.DB
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
	SetNX(key string, value string) error
	CompareAndSwap(key string, oldValue, newValue string) error

	BumpInteger(key string, delta int64) error
}

type TKVCacheHandler interface {
	CacheGet(key string) (string, error)
	CacheSet(key string, value string) error
	CacheDelete(key string) error

	CacheSetNX(key string, value string) error
	CacheCompareAndSwap(key string, oldValue, newValue string) error
	CacheIterate(prefix string, offset int, limit int) ([]string, error)
}

type TKV interface {
	TKVDataHandler
	TKVCacheHandler
	TKVBatchHandler

	Close() error

	GetDataDB() *badger.DB
	GetCache() *badger.DB
}
