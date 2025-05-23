package rft

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"github.com/jellydator/ttlcache/v3"
)

const (
	dbTypeValues = "values"
	dbTypeTags   = "tags"
	cacheType    = "cache"
)

// snapshotEntry is used to store entries in the snapshot with a DB type.
type snapshotEntry struct {
	DBType string // "values" or "tags"
	Key    string
	Value  string

	TTL         float64 // not present for std db entries, only for caches
	TimeEncoded int64   // not present for std db entries, only for caches - so we dont reconstruct dead entries
}

type badgerFSMSnapshot struct {
	valuesDb *badger.DB
	tagsDb   *badger.DB

	stdCache *ttlcache.Cache[string, string]
}

func (b *badgerFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	encoder := json.NewEncoder(sink)

	persistDb := func(db *badger.DB, dbType string) error {
		return db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := item.KeyCopy(nil)
				err := item.Value(func(val []byte) error {
					valCopy := make([]byte, len(val))
					copy(valCopy, val)
					entry := snapshotEntry{
						DBType: dbType,
						Key:    string(key),
						Value:  string(valCopy),
					}
					if errEnc := encoder.Encode(entry); errEnc != nil {
						return fmt.Errorf(
							"failed to encode snapshot entry for %s (key: %s): %w",
							dbType,
							string(key),
							errEnc,
						)
					}
					return nil
				})
				if err != nil {
					return fmt.Errorf(
						"failed to get value for snapshot from %s (key: %s): %w",
						dbType,
						string(key),
						err,
					)
				}
			}
			return nil
		})
	}

	// Persist valuesDb
	if err := persistDb(b.valuesDb, dbTypeValues); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to persist snapshot for valuesDb: %w", err)
	}

	// Persist tagsDb
	if err := persistDb(b.tagsDb, dbTypeTags); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to persist snapshot for tagsDb: %w", err)
	}

	persistStdCache := func(cache *ttlcache.Cache[string, string], cacheType string) error {
		for _, item := range cache.Items() {
			entry := snapshotEntry{
				DBType:      cacheType,
				Key:         item.Key(),
				Value:       item.Value(),
				TTL:         item.TTL().Seconds(),
				TimeEncoded: time.Now().Unix(),
			}
			if errEnc := encoder.Encode(entry); errEnc != nil {
				return fmt.Errorf(
					"failed to encode snapshot entry for %s (key: %s): %w",
					cacheType,
					item.Key(),
					errEnc,
				)
			}
			return nil
		}
		return nil
	}

	if err := persistStdCache(b.stdCache, cacheType); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to persist snapshot for stdCache: %w", err)
	}

	if errClose := sink.Close(); errClose != nil {
		return fmt.Errorf("failed to close snapshot sink: %w", errClose)
	}
	return nil
}

func (b *badgerFSMSnapshot) Release() {}
