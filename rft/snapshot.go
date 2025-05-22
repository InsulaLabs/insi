package rft

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

const (
	dbTypeValues = "values"
	dbTypeTags   = "tags"
)

// snapshotEntry is used to store entries in the snapshot with a DB type.
type snapshotEntry struct {
	DBType string // "values" or "tags"
	Key    string
	Value  string
}

type badgerFSMSnapshot struct {
	valuesDb *badger.DB
	tagsDb   *badger.DB
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
						return fmt.Errorf("failed to encode snapshot entry for %s (key: %s): %w", dbType, string(key), errEnc)
					}
					return nil
				})
				if err != nil {
					return fmt.Errorf("failed to get value for snapshot from %s (key: %s): %w", dbType, string(key), err)
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

	if errClose := sink.Close(); errClose != nil {
		return fmt.Errorf("failed to close snapshot sink: %w", errClose)
	}
	return nil
}

func (b *badgerFSMSnapshot) Release() {}
