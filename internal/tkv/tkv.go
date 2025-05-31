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

/*

// TODO: OPTIMIZATION:

	If we hash the object data and store it into the objectManifest
	then we optimize the loading of a stored object by
	dedicating a "cache" size for disk - then dump uploads
	to the cache under the name of the key its stored as.
	We keep a ttl map with a file handle to it and as long
	as the hash doesn't change we can use the cached object
	rather than loading all of the chunks from the db.

	The ttl handle is just for os reading. when a read comes in
	and a cache hit happens (Request comes in and the hash  in
	the manifest is the same as the hash of the cached object)
	we can load that ttl map and grab a file handle to return to
	the user. This means we can avoid having large objects in memory
	for prolonged periods of time [crucial for this type of caching]

	If we have a cache miss and an object that is mapped to that key,
	then we can delete the cache file from disk.
*/

// Objects are meant to store "larger" values by splitting them
// up into smaller (<1mb) chunks and storing them in multiple keys,
// with the mapped value being a structure to reconstruct the object.
type TKVObjectHandler interface {
	SetObject(key string, object []byte) error
	GetObject(key string) ([]byte, error)
	DeleteObject(key string) error
	GetObjectList(prefix string, offset int, limit int) ([]string, error)
}

type TKV interface {
	TKVDataHandler
	TKVCacheHandler
	TKVObjectHandler

	Close() error

	GetDataDB() *badger.DB
	GetCache() *ttlcache.Cache[string, string]
	GetObjectsDB() *badger.DB
}
