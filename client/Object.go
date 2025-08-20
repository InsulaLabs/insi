package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
)

type ClientObjectManager struct {
	client *Client
}

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

func RemoteMapFromPrefix(ctx context.Context, prefix string, logger *slog.Logger, client *Client, maxSize int) (*RemoteMap, error) {

	if maxSize <= 0 {
		return nil, ErrObjectInvalidMaxSize
	}

	/*


		TODO:

			Ensure that all local key map entries are prefixed with the prefix

			Ensure that when we hand them to callers we do so without the prefix so the
			caller doesn't need to recall the prefix, and they can generalize their usage
			of the object easily


	*/

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
		fields: fields,
		mu:     sync.RWMutex{},
		ctx:    ctx,
		client: client,
		logger: logger,
		prefix: prefix,
	}, nil
}

type RemoteMap struct {
	fields map[string]string
	mu     sync.RWMutex
	ctx    context.Context
	client *Client
	logger *slog.Logger
	prefix string
}

func (x *RemoteMap) CreateUniqueEntry(key, value string) error {

	fullKey := fmt.Sprintf("%s:%s", x.prefix, key)

	x.mu.Lock()
	if _, ok := x.fields[fullKey]; ok {
		x.mu.Unlock()
		return ErrFieldAlreadyExists
	}
	x.mu.Unlock()

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

func (x *RemoteMap) GetEntry(key string) (string, error) {
	fullKey := fmt.Sprintf("%s:%s", x.prefix, key)
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.fields[fullKey], nil
}

func (x *RemoteMap) GetAllEntries() map[string]string {
	x.mu.RLock()
	defer x.mu.RUnlock()

	fullEntries := x.fields
	entries := map[string]string{}
	for key, value := range fullEntries {
		actual := strings.TrimPrefix(key, x.prefix+":")
		entries[actual] = value
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
var CFG_OBJECT_LOW_MEMORY = false

func (x *RemoteMap) SyncPull(quick bool) error {

	entries := make([]string, len(x.fields))
	x.mu.RLock()
	for key := range x.fields {
		entries = append(entries, key)
	}
	x.mu.RUnlock()

	/*
		quick updates the items in-place without potentially removing any stale items
		which ideally wouldnt happen, but since networks might flake if not "quick"
		we can pause operations potentially very briefly with a full map lock
	*/
	if quick {
		for _, key := range entries {
			fullKey := fmt.Sprintf("%s:%s", x.prefix, key)
			currentValue, err := x.getCurrentExistingEntryRemote(fullKey)
			if err != nil {
				return err
			}
			x.mu.Lock()
			x.fields[key] = currentValue
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
	if CFG_OBJECT_LOW_MEMORY {

		x.mu.Lock()
		defer x.mu.Unlock()

		x.fields = make(map[string]string)
		for _, key := range entries {
			fullKey := fmt.Sprintf("%s:%s", x.prefix, key)
			currentValue, err := x.getCurrentExistingEntryRemote(fullKey)
			if err != nil {
				return err
			}
			x.fields[key] = currentValue
		}
		return nil
	}

	/*
		In the general case the records will be manageable within the memory of the system
		and the user really doesn't need to care about having a brief moment when there are
		two copies of the record in memory (just during the full update)

		So, in that case we create a new map. This allows us to let other threads read from the
		old map while we update in their periphery.

		Once the new map is built, we then lock and swap the map in place
	*/
	newMap := make(map[string]string)
	for _, key := range entries {
		fullKey := fmt.Sprintf("%s:%s", x.prefix, key)
		currentValue, err := x.getCurrentExistingEntryRemote(fullKey)
		if err != nil {
			return err
		}
		newMap[key] = currentValue
	}

	// update the local representation
	x.mu.Lock()
	defer x.mu.Unlock()

	x.fields = newMap
	return nil
}
