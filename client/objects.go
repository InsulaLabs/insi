package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

var (
	ErrFieldAlreadyExists = errors.New("field already exists")
	ErrFieldNotFound      = errors.New("field not found")
)

/*

	A remote map of objects

	Each object will be grouped into its own unique key prefix (most likely a uuid, but thats up to the user)
	then each field is an extension of that key prefix
	   uuid:field_name => value

		so, for example:

		123e4567-e89b-12d3-a456-426614174000:name => "John Doe"
		123e4567-e89b-12d3-a456-426614174000:age => "30"
		123e4567-e89b-12d3-a456-426614174000:email => "john.doe@example.com"

	This means that we can set

*/

var (
	ErrObjectInvalidMaxSize = errors.New("invalid max size")
)

/*
RemoteMapFromPrefix

	Creates a new remote map from a given prefix

	For any valid prefix given, a local map representation is made of the remote data.
	This may or may not represent an object. The commentary describes it as so, but its
	a simple "range" map of keys on the remote server.

	The remote object map provides a window to work with these ranges of key
	data to facilitate easy operations on the data locally
*/
func RemoteMapFromPrefix(ctx context.Context, prefix string, logger *slog.Logger, client *Client, maxSize int) (*RemoteMap, error) {

	if maxSize <= 0 {
		return nil, ErrObjectInvalidMaxSize
	}

	// remove any trailing * from the prefix to permit common patterns of usage
	// that are invalid, but handleable
	prefix = strings.TrimSuffix(prefix, "*")

	keysInMap, err := client.IterateByPrefix(prefix, 0, maxSize)
	if err != nil {
		return nil, err
	}

	fields := make(map[string]string, len(keysInMap))

	for _, key := range keysInMap {
		var value string
		if err := WithRetriesVoid(ctx, logger, func() error {
			value, err = client.Get(key)
			return err
		}); err != nil {
			return nil, err
		}
		fields[key] = value
	}

	return &RemoteMap{
		fields:  fields,
		mu:      sync.RWMutex{},
		ctx:     ctx,
		client:  client,
		logger:  logger,
		prefix:  prefix,
		maxSize: maxSize,
	}, nil
}

type RemoteMap struct {
	fields  map[string]string
	mu      sync.RWMutex
	ctx     context.Context
	client  *Client
	logger  *slog.Logger
	prefix  string
	maxSize int
}

/*
AutoSync

Starts a goroutine that will periodically sync the remote map with the local map
using the updateInterval.

# The go routine will automatically stop when the context is completed

This permits the user to have a local in-memory representation of the remote map
that stays consistently up to date with the remote database and provides
a simple means of updating the remote map without the direct usage of a db client
and its overhead semantics
*/
func (x *RemoteMap) AutoSync(ctx context.Context, updateInterval time.Duration) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(updateInterval):
				if err := x.SyncPull(ctx, false); err != nil {
					x.logger.Error("Error syncing remote map", "error", err)
				}
			}
		}
	}()
	return nil
}

/*
RemoteFromSubset

In an instance where a selected field for a map has multiple members of a sub field:

UUID:INSTANCE_ID:FIELD_NAME:ITEM_ONE
UUID:INSTANCE_ID:FIELD_NAME:ITEM_TWO
UUID:INSTANCE_ID:FIELD_NAME:ITEM_THREE

Say you have a remote map on UUID, you can get a subset map:

	   RemoteFromSubset("INSTANCE_ID")

		 Theat will then be scoped to UUID:INSTANCE_ID and all of its entries
		 will be that of the FIELD_NAME

The internal representatioan of the maps do not consider the existence of other remote
maps, and as such, if a subset map is created it is up to the caller to ensure that the
data integrity is maintained (i.e don't remove data from the parent that would upset
the child map and lead to weird data juggling)
*/
func (x *RemoteMap) RemoteFromSubset(subsetField string) (*RemoteMap, error) {

	properSubsetField := fmt.Sprintf("%s:%s", x.prefix, subsetField)

	keysInMap, err := x.client.IterateByPrefix(properSubsetField, 0, x.maxSize)
	if err != nil {
		return nil, err
	}

	subsetFields := make(map[string]string, len(keysInMap))

	for _, key := range keysInMap {
		var value string
		if err := WithRetriesVoid(x.ctx, x.logger, func() error {
			value, err = x.client.Get(key)
			return err
		}); err != nil {
			return nil, err
		}
		subsetFields[key] = value
	}

	return &RemoteMap{
		fields:  subsetFields,
		mu:      sync.RWMutex{},
		ctx:     x.ctx,
		client:  x.client,
		logger:  x.logger.With("subset", subsetField),
		prefix:  properSubsetField,
		maxSize: x.maxSize,
	}, nil
}

/*
CreateUniqueEntry

	Creates a new entry in the remote map, but only if the key does not already exist.
	Returns an error if the key already exists.
	The key is the unprefixed field name to create onto the object instance
*/
func (x *RemoteMap) CreateUniqueEntry(key, value string) error {

	fullKey := fmt.Sprintf("%s:%s", x.prefix, key)

	if err := WithRetriesVoid(x.ctx, x.logger, func() error {
		return x.client.SetNX(fullKey, value)
	}); err != nil {
		return err
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	x.fields[fullKey] = value
	return nil
}

func (x *RemoteMap) getCurrentExistingEntryRemote(prefixedKey string) (string, error) {
	var err error
	var value string
	if err = WithRetriesVoid(x.ctx, x.logger, func() error {
		// note: it must have the prefix already
		value, err = x.client.Get(prefixedKey)
		return err
	}); err != nil {
		return "", err
	}
	return value, nil
}

/*
UpdateExistingEntry

	Updates an existing entry in the remote map.
	The key is the unprefixed field name on the object instance
*/
func (x *RemoteMap) UpdateExistingEntry(key, value string) error {

	fullKey := fmt.Sprintf("%s:%s", x.prefix, key)
	existingValue, err := x.getCurrentExistingEntryRemote(fullKey)
	if err != nil {
		return err
	}

	// since we can, we might as well check to see if its really an update
	if existingValue == value {
		return nil
	}

	if err := WithRetriesVoid(x.ctx, x.logger, func() error {
		x.mu.Lock()
		defer x.mu.Unlock()
		if _, ok := x.fields[fullKey]; !ok {
			return ErrFieldNotFound
		}
		return x.client.CompareAndSwap(fullKey, existingValue, value)
	}); err != nil {
		return err
	}

	// update local representation
	x.mu.Lock()
	defer x.mu.Unlock()
	x.fields[fullKey] = value
	return nil
}

/*
DeleteExistingEntry

	Deletes an existing entry from the remote map.
	The key is the unprefixed field name on the object instance
*/
func (x *RemoteMap) DeleteExistingEntry(key string) error {

	fullKey := fmt.Sprintf("%s:%s", x.prefix, key)

	if err := WithRetriesVoid(x.ctx, x.logger, func() error {
		x.mu.Lock()
		defer x.mu.Unlock()
		if _, ok := x.fields[fullKey]; !ok {
			return ErrFieldNotFound
		}
		return x.client.Delete(fullKey)
	}); err != nil {
		return err
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	delete(x.fields, fullKey)
	return nil
}

/*
GetEntry

	Returns the value of the entry for the given key (unprefixed)
	The key is the unprefixed field name to create onto the object instance
*/
func (x *RemoteMap) GetEntry(key string) (string, error) {
	fullKey := fmt.Sprintf("%s:%s", x.prefix, key)
	x.mu.RLock()
	defer x.mu.RUnlock()
	value, ok := x.fields[fullKey]
	if !ok {
		return "", ErrFieldNotFound
	}
	return value, nil
}

/*
GetAllEntries

	Returns a map of all the entries in the remote map.
	If withPrefix is true, then the object instance prefix will be included in the key
*/
func (x *RemoteMap) GetAllEntries(withPrefix bool) map[string]string {
	x.mu.RLock()
	defer x.mu.RUnlock()

	fullEntries := x.fields
	entries := map[string]string{}
	for key, value := range fullEntries {
		if withPrefix {
			entries[key] = value
		} else {
			actual := strings.TrimPrefix(key, x.prefix+":")
			entries[actual] = value
		}
	}
	return entries
}

/*
	   this implementation will create a list of keys that it has to pull
		 we then create a new map to build a local representation of the remote map
		 while outside the mutex lock to ensure we dont have network activity behing
		 the map lock

		 once the new map is complete, we iterate over it and update the local representation
		 key by key

		 this means we will briefly have two copies of the map in memory during the update
		 but the alternative would be have impact on lock contention
*/
var CONFIG_OBJECT_LOW_MEMORY = false

func (x *RemoteMap) SyncPull(ctx context.Context, quick bool) error {

	entries := make([]string, len(x.fields))
	x.mu.RLock()
	idx := 0
	for key := range x.fields {
		entries[idx] = key
		idx++
	}
	x.mu.RUnlock()

	/*
		quick updates the items in-place without potentially removing any stale items
		which ideally wouldnt happen, but since networks might flake if not "quick"
		we can pause operations potentially very briefly with a full map lock
	*/
	if quick {
		for _, fullKey := range entries {
			currentValue, err := x.getCurrentExistingEntryRemote(fullKey)
			if err != nil {
				return err
			}
			x.mu.Lock()
			x.fields[fullKey] = currentValue
			x.mu.Unlock()
		}
		return nil
	}

	/*
		If the client is configured to be low memory
		we lock the map and free it immediately

		Then, we update the local representation behind that lock,
		potentially causing lock contention across the users system
		as a series of network operations are now operating behind the lock
	*/
	if CONFIG_OBJECT_LOW_MEMORY {

		x.mu.Lock()
		defer x.mu.Unlock()

		x.fields = make(map[string]string)
		for _, fullKey := range entries {
			currentValue, err := x.getCurrentExistingEntryRemote(fullKey)
			if err != nil {
				return err
			}
			x.fields[fullKey] = currentValue
		}
		return nil
	}

	/*
		In the general case the records will be manageable within the memory of the system
		and the user really doesn't need to care about having a brief moment when there are
		two copies of the record in memory (just during the full update)

		So, in that case we create a new map pulling all of the entries from the associated
		prefix and then swap the map in place.
	*/

	keysInMap, err := x.client.IterateByPrefix(x.prefix, 0, x.maxSize)
	if err != nil {
		return err
	}

	fields := make(map[string]string, len(keysInMap))

	for _, key := range keysInMap {
		var value string
		if err := WithRetriesVoid(ctx, x.logger, func() error {
			value, err = x.client.Get(key)
			return err
		}); err != nil {
			return err
		}
		fields[key] = value
	}

	// update the local representation
	x.mu.Lock()
	defer x.mu.Unlock()

	x.fields = fields
	return nil
}
