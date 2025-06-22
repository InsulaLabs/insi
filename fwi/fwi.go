package fwi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/db/models"
	"github.com/fatih/color"
	"github.com/google/uuid"
)

const (
	EntityPrefix = "entity_record:"
)

var (
	// errEntityNotFound is returned when an entity cannot be found by name after a full scan.
	errEntityNotFound = errors.New("entity not found")
)

func withRetries[R any](ctx context.Context, logger *slog.Logger, fn func() (R, error)) (R, error) {
	for {
		result, err := fn()
		if err == nil {
			return result, nil // Success
		}

		var rateLimitErr *client.ErrRateLimited
		if errors.As(err, &rateLimitErr) {
			logger.Warn("FWI operation rate limited, sleeping", "duration", rateLimitErr.RetryAfter)
			select {
			case <-time.After(rateLimitErr.RetryAfter):
				logger.Debug("Finished rate limit sleep, retrying operation.")
				continue // Slept, continue to retry
			case <-ctx.Done():
				logger.Error("Context cancelled during rate limit sleep", "error", ctx.Err())
				var zero R
				return zero, fmt.Errorf("operation cancelled during rate limit sleep: %w", ctx.Err())
			}
		}

		// It's some other error, just return it. FWI is a library, so it should propagate them.
		var zero R
		return zero, err
	}
}

func withRetriesVoid(ctx context.Context, logger *slog.Logger, fn func() error) error {
	_, err := withRetries(ctx, logger, func() (any, error) {
		return nil, fn()
	})
	return err
}

// Abstracts
type KV interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string) error
	IterateKeys(ctx context.Context, prefix string, offset, limit int) ([]string, error)
	Delete(ctx context.Context, key string) error

	CompareAndSwap(ctx context.Context, key string, oldValue, newValue string) error
	SetNX(ctx context.Context, key string, value string) error

	// Adds a scope to the key to scope the data operations to a sub-scope
	// When the system scopes data to an entity (internally) it prefixes by a magic hidden string,
	// we will call "U" All items on the entity are scoped as:
	// U:values U:cache U:events in their respective stores
	// Push-scope is an abstraction layer specific to the fwi package that adds another scope to the kv
	// prefix on the underlying KV storage (cache or values)
	// i.e:			U.scope.scope0.scope1.scope2.key etc
	PushScope(scope string)

	PopScope() // if scope prefix is empty its a no-op
}

type Events interface {
	Subscribe(ctx context.Context, topic string, onEvent func(data any)) error
	Publish(ctx context.Context, topic string, data any) error

	PushScope(scope string)
	PopScope()
}

type Blobs interface {
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Set(ctx context.Context, key, value string) error
	SetReader(ctx context.Context, key string, r io.Reader) error
	Delete(ctx context.Context, key string) error
	IterateKeys(ctx context.Context, prefix string, offset, limit int) ([]string, error)

	PushScope(scope string)
	PopScope()
}

type Aliases interface {
	MakeAlias(ctx context.Context) (string, error)
	DeleteAlias(ctx context.Context, alias string) error
	ListAliases(ctx context.Context) ([]string, error)
}

type Entity interface {
	GetName() string
	GetKey() string
	GetUUID() string

	// Use the insi client to bump an integer value
	Bump(ctx context.Context, key string, value int) error
	GetValueStore() KV // Get the value store for the entity
	GetCacheStore() KV // Get the cache store for the entity
	GetEvents() Events // Get the event pub/sub for the entity
	GetBlobs() Blobs   // Get the blob store for the entity
	GetFS() FS         // Get the virtual file system for the entity

	GetAliasManager() Aliases

	// GetUsageInfo retrieves the current usage and limits for the entity.
	GetUsageInfo(ctx context.Context) (*models.LimitsResponse, error)
}

// FWI is the instance that contains a client with the root key
// When we want to have a user or service delineated, we create an entity
// and it creates an api key for that entity that seperates that data internally
// We store the entities and load them by name so we can re-access their data scope
// The FWI interace is
type FWI interface {
	// Creates an api key as an admin
	CreateEntity(
		ctx context.Context,
		name string, // how caller refers to the entity
		maxlimits models.Limits,
	) (Entity, error)

	// Create or load an entity. If the entity already exists, it will be loaded.
	// If the entity does not exist, it will be created.
	// If the entity is created, the maxlimits will be set.
	// If the entity is loaded, the maxlimits will be ignored.
	CreateOrLoadEntity(
		ctx context.Context,
		name string,
		maxlimits models.Limits,
	) (Entity, error)

	// Load all created entities (we wont have thousands so no need for pagination)
	GetAllEntities(ctx context.Context) ([]Entity, error)

	// Load an entity by name
	GetEntity(ctx context.Context, name string) (Entity, error)

	// Delete an entity
	DeleteEntity(ctx context.Context, name string) error

	// Update the limits for an entity.
	UpdateEntityLimits(ctx context.Context, name string, newLimits models.Limits) error

	// GetOpsPerSecond retrieves the current operations-per-second metrics for the node.
	GetOpsPerSecond(ctx context.Context) (*models.OpsPerSecondCounters, error)
}

// ------------------------------------------------------------------------------------------------

type entityImpl struct {
	name       string
	insiClient *client.Client
	record     *entityRecord
	logger     *slog.Logger
}

type valueStoreImpl struct {
	insiClient *client.Client
	scope      string
	logger     *slog.Logger
}

type cacheStoreImpl struct {
	insiClient *client.Client
	scope      string
	logger     *slog.Logger
}

type eventsImpl struct {
	insiClient *client.Client
	scope      string
	logger     *slog.Logger
}

type blobsImpl struct {
	insiClient *client.Client
	scope      string
	logger     *slog.Logger
}

type aliasesImpl struct {
	insiClient *client.Client
	logger     *slog.Logger
}

type fwiImpl struct {
	insiCfg        *client.Config
	rootInsiClient *client.Client
	entities       map[string]Entity
	mu             sync.RWMutex
	logger         *slog.Logger
}

type entityRecord struct {
	UUID string `json:"uuid"` // unique id for the entity
	Name string `json:"name"` // the name of the entity as seen by the caller
	Key  string `json:"key"`  // the api key for the entity
	// Limits stored and manuouakted internally
	// this is a meta-meta record for the entity
}

var _ Entity = &entityImpl{}
var _ KV = &valueStoreImpl{}
var _ KV = &cacheStoreImpl{}
var _ Events = &eventsImpl{}
var _ Blobs = &blobsImpl{}
var _ Aliases = &aliasesImpl{}

func (e *entityImpl) GetName() string {
	return e.name
}

func (e *entityImpl) GetKey() string {
	return e.insiClient.GetApiKey()
}

func (e *entityImpl) Bump(ctx context.Context, key string, value int) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.Bump(key, value)
	})
}

func (e *entityImpl) GetUUID() string {
	return e.record.UUID
}

func (e *entityImpl) GetValueStore() KV {
	return &valueStoreImpl{
		insiClient: e.insiClient,
		logger:     e.logger.WithGroup("vs"),
	}
}

func (e *entityImpl) GetCacheStore() KV {
	return &cacheStoreImpl{
		insiClient: e.insiClient,
		logger:     e.logger.WithGroup("cache"),
	}
}

func (e *entityImpl) GetEvents() Events {
	return &eventsImpl{
		insiClient: e.insiClient,
		logger:     e.logger.WithGroup("events"),
	}
}

func (e *entityImpl) GetBlobs() Blobs {
	return &blobsImpl{
		insiClient: e.insiClient,
		logger:     e.logger.WithGroup("blobs"),
	}
}

func (e *entityImpl) GetFS() FS {
	return NewVFS(e.GetValueStore(), e.GetBlobs(), e.logger)
}

func (e *entityImpl) GetAliasManager() Aliases {
	return &aliasesImpl{
		insiClient: e.insiClient,
		logger:     e.logger.WithGroup("aliases"),
	}
}

func (e *entityImpl) GetUsageInfo(ctx context.Context) (*models.LimitsResponse, error) {
	return withRetries(ctx, e.logger, func() (*models.LimitsResponse, error) {
		return e.insiClient.GetLimits()
	})
}

func assembleKey(scope, key string) string {
	if scope == "" {
		return key
	}
	return fmt.Sprintf("%s.%s", scope, key)
}

func (e *valueStoreImpl) Get(ctx context.Context, key string) (string, error) {
	return withRetries(ctx, e.logger, func() (string, error) {
		return e.insiClient.Get(assembleKey(e.scope, key))
	})
}

func (e *valueStoreImpl) Set(ctx context.Context, key string, value string) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.Set(assembleKey(e.scope, key), value)
	})
}

func (e *valueStoreImpl) IterateKeys(ctx context.Context, prefix string, offset, limit int) ([]string, error) {
	return withRetries(ctx, e.logger, func() ([]string, error) {
		return e.insiClient.IterateByPrefix(assembleKey(e.scope, prefix), offset, limit)
	})
}

func (e *valueStoreImpl) Delete(ctx context.Context, key string) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.Delete(assembleKey(e.scope, key))
	})
}

func (e *valueStoreImpl) CompareAndSwap(ctx context.Context, key string, oldValue, newValue string) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.CompareAndSwap(assembleKey(e.scope, key), oldValue, newValue)
	})
}

func (e *valueStoreImpl) SetNX(ctx context.Context, key string, value string) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.SetNX(assembleKey(e.scope, key), value)
	})
}

func (e *valueStoreImpl) PushScope(scope string) {
	if e.scope == "" {
		e.scope = scope
	} else {
		e.scope = e.scope + "." + scope
	}
}

func (e *valueStoreImpl) PopScope() {
	if i := strings.LastIndex(e.scope, "."); i != -1 {
		e.scope = e.scope[:i]
	} else {
		e.scope = ""
	}
}

func (e *cacheStoreImpl) Get(ctx context.Context, key string) (string, error) {
	return withRetries(ctx, e.logger, func() (string, error) {
		return e.insiClient.GetCache(assembleKey(e.scope, key))
	})
}

func (e *cacheStoreImpl) Set(ctx context.Context, key string, value string) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.SetCache(assembleKey(e.scope, key), value)
	})
}

func (e *cacheStoreImpl) IterateKeys(ctx context.Context, prefix string, offset, limit int) ([]string, error) {
	return withRetries(ctx, e.logger, func() ([]string, error) {
		return e.insiClient.IterateCacheByPrefix(assembleKey(e.scope, prefix), offset, limit)
	})
}

func (e *cacheStoreImpl) Delete(ctx context.Context, key string) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.DeleteCache(assembleKey(e.scope, key))
	})
}

func (e *cacheStoreImpl) CompareAndSwap(ctx context.Context, key string, oldValue, newValue string) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.CompareAndSwapCache(assembleKey(e.scope, key), oldValue, newValue)
	})
}

func (e *cacheStoreImpl) SetNX(ctx context.Context, key string, value string) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.SetCacheNX(assembleKey(e.scope, key), value)
	})
}

func (e *cacheStoreImpl) PushScope(scope string) {
	if e.scope == "" {
		e.scope = scope
	} else {
		e.scope = e.scope + "." + scope
	}
}

func (e *cacheStoreImpl) PopScope() {
	if i := strings.LastIndex(e.scope, "."); i != -1 {
		e.scope = e.scope[:i]
	} else {
		e.scope = ""
	}
}

func (e *eventsImpl) Subscribe(
	ctx context.Context,
	topic string,
	onEvent func(data any),
) error {
	return e.insiClient.SubscribeToEvents(assembleKey(e.scope, topic), ctx, onEvent)
}

func (e *eventsImpl) Publish(ctx context.Context, topic string, data any) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.PublishEvent(assembleKey(e.scope, topic), data)
	})
}

func (e *eventsImpl) PushScope(scope string) {
	if e.scope == "" {
		e.scope = scope
	} else {
		e.scope = e.scope + "." + scope
	}
}

func (e *eventsImpl) PopScope() {
	if i := strings.LastIndex(e.scope, "."); i != -1 {
		e.scope = e.scope[:i]
	} else {
		e.scope = ""
	}
}

func (e *blobsImpl) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return withRetries(ctx, e.logger, func() (io.ReadCloser, error) {
		return e.insiClient.GetBlob(ctx, assembleKey(e.scope, key))
	})
}

func (e *blobsImpl) Set(ctx context.Context, key string, value string) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.UploadBlob(ctx, assembleKey(e.scope, key), strings.NewReader(value), key)
	})
}

func (e *blobsImpl) SetReader(ctx context.Context, key string, r io.Reader) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.UploadBlob(ctx, assembleKey(e.scope, key), r, key)
	})
}

func (e *blobsImpl) Delete(ctx context.Context, key string) error {
	return withRetriesVoid(ctx, e.logger, func() error {
		return e.insiClient.DeleteBlob(assembleKey(e.scope, key))
	})
}

func (e *blobsImpl) IterateKeys(ctx context.Context, prefix string, offset, limit int) ([]string, error) {
	return withRetries(ctx, e.logger, func() ([]string, error) {
		return e.insiClient.IterateBlobKeysByPrefix(assembleKey(e.scope, prefix), offset, limit)
	})
}

func (e *blobsImpl) PushScope(scope string) {
	if e.scope == "" {
		e.scope = scope
	} else {
		e.scope = e.scope + "." + scope
	}
}

func (e *blobsImpl) PopScope() {
	if i := strings.LastIndex(e.scope, "."); i != -1 {
		e.scope = e.scope[:i]
	} else {
		e.scope = ""
	}
}

func (a *aliasesImpl) MakeAlias(ctx context.Context) (string, error) {
	return withRetries(ctx, a.logger, func() (string, error) {
		resp, err := a.insiClient.SetAlias()
		if err != nil {
			return "", err
		}
		if resp == nil {
			return "", fmt.Errorf("server did not return an alias")
		}
		return resp.Alias, nil
	})
}

func (a *aliasesImpl) DeleteAlias(ctx context.Context, alias string) error {
	return withRetriesVoid(ctx, a.logger, func() error {
		return a.insiClient.DeleteAlias(alias)
	})
}

func (a *aliasesImpl) ListAliases(ctx context.Context) ([]string, error) {
	return withRetries(ctx, a.logger, func() ([]string, error) {
		resp, err := a.insiClient.ListAliases()
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return []string{}, nil
		}
		return resp.Aliases, nil
	})
}

func NewFWI(insiCfg *client.Config, rootInsiClient *client.Client, logger *slog.Logger) (FWI, error) {
	l := logger.WithGroup("fwi")
	f := &fwiImpl{
		rootInsiClient: rootInsiClient,
		entities:       make(map[string]Entity),
		logger:         l,
		insiCfg:        insiCfg,
	}

	if _, err := f.rootInsiClient.Ping(); err != nil {
		return nil, err
	}

	// load the entites that are already created
	entities, err := f.GetAllEntities(context.Background())
	if err != nil {
		return nil, err
	}

	for _, entity := range entities {
		f.mu.Lock()
		f.entities[entity.GetName()] = entity
		f.mu.Unlock()
	}

	l.Info("FWI initialized", "loaded_entities", len(entities))
	return f, nil
}

func (f *fwiImpl) CreateEntity(
	ctx context.Context,
	name string,
	maxlimits models.Limits,
) (Entity, error) {
	// To enforce name uniqueness without using it as a key, we must check all existing entities.
	existingEntity, err := f.findEntityByName(ctx, name)
	if err != nil && !errors.Is(err, errEntityNotFound) {
		return nil, fmt.Errorf("error checking for existing entity: %w", err)
	}
	if existingEntity != nil {
		return nil, fmt.Errorf("entity with name '%s' already exists", name)
	}

	key, err := withRetries(ctx, f.logger, func() (*models.ApiKeyCreateResponse, error) {
		return f.rootInsiClient.CreateAPIKey(name)
	})
	if err != nil {
		return nil, err
	}

	// create the entity record
	entityRecord := &entityRecord{
		UUID: uuid.New().String(),
		Name: name,
		Key:  key.Key,
	}

	entityKey := fmt.Sprintf("%s%s", EntityPrefix, entityRecord.UUID)

	entityEncoded, err := json.Marshal(entityRecord)
	if err != nil {
		return nil, err
	}

	if err := withRetriesVoid(ctx, f.logger, func() error {
		return f.rootInsiClient.Set(entityKey, string(entityEncoded))
	}); err != nil {
		return nil, err
	}

	if maxlimits.BytesInMemory == nil {
		maxlimits.BytesInMemory = new(int64)
		*maxlimits.BytesInMemory = 1024 * 1024 * 1024 // 1GB
	}

	if maxlimits.BytesOnDisk == nil {
		maxlimits.BytesOnDisk = new(int64)
		*maxlimits.BytesOnDisk = 1024 * 1024 * 1024 // 1GB
	}

	if *maxlimits.BytesInMemory > *maxlimits.BytesOnDisk {
		return nil, fmt.Errorf("bytes in memory cannot be greater than bytes on disk")
	}

	if maxlimits.EventsEmitted == nil {
		maxlimits.EventsEmitted = new(int64)
		*maxlimits.EventsEmitted = 1000
	}

	if maxlimits.Subscribers == nil {
		maxlimits.Subscribers = new(int64)
		*maxlimits.Subscribers = 100
	}

	// set the limits on the entity key
	if err := withRetriesVoid(ctx, f.logger, func() error {
		return f.rootInsiClient.SetLimits(key.Key, maxlimits)
	}); err != nil {
		color.HiRed("Failed to set the limits on new entity: %s %v", key.Key, err)
		return nil, err
	}

	cfgc := client.Config{
		ApiKey:         key.Key,
		Endpoints:      f.insiCfg.Endpoints,
		Logger:         f.insiCfg.Logger.WithGroup(name),
		ConnectionType: f.insiCfg.ConnectionType,
		SkipVerify:     f.insiCfg.SkipVerify,
	}

	entityClient, err := client.NewClient(&cfgc)
	if err != nil {
		return nil, err
	}

	entity := &entityImpl{
		name:       name,
		insiClient: entityClient,
		record:     entityRecord,
		logger:     f.logger.WithGroup("entity").WithGroup(name),
	}

	f.mu.Lock()
	f.entities[name] = entity
	f.mu.Unlock()

	return entity, nil
}

func (f *fwiImpl) CreateOrLoadEntity(
	ctx context.Context,
	name string,
	maxlimits models.Limits,
) (Entity, error) {
	// First, check the in-memory cache for the entity.
	f.mu.RLock()
	entity, ok := f.entities[name]
	f.mu.RUnlock()
	if ok {
		return entity, nil
	}

	// If not in cache, try to load from the database by iterating and checking the name field.
	entity, err := f.findEntityByName(ctx, name)
	if err == nil {
		// Found it in the DB. Cache it locally and return.
		f.mu.Lock()
		f.entities[name] = entity
		f.mu.Unlock()
		return entity, nil
	}

	if !errors.Is(err, errEntityNotFound) {
		// An unexpected error occurred during the search.
		return nil, err
	}

	// Entity does not exist, so create it.
	return f.CreateEntity(ctx, name, maxlimits)
}

func (f *fwiImpl) GetAllEntities(ctx context.Context) ([]Entity, error) {
	keys, err := withRetries(ctx, f.logger, func() ([]string, error) {
		return f.rootInsiClient.IterateByPrefix(EntityPrefix, 0, 2048)
	})
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			return []Entity{}, nil // No entities found is not an error.
		}
		return nil, err
	}

	entities := make([]Entity, 0, len(keys))

	for _, key := range keys {
		entityRecordStr, err := withRetries(ctx, f.logger, func() (string, error) {
			return f.rootInsiClient.Get(key)
		})
		if err != nil {
			f.logger.Warn("Failed to get entity record during GetAll, skipping", "key", key, "error", err)
			continue
		}

		var er entityRecord
		if err := json.Unmarshal([]byte(entityRecordStr), &er); err != nil {
			f.logger.Warn("Failed to unmarshal entity record during GetAll, skipping", "key", key, "error", err)
			continue
		}

		entityClient := f.rootInsiClient.DeriveWithApiKey(er.Name, er.Key)
		entity := &entityImpl{
			name:       er.Name,
			insiClient: entityClient,
			record:     &er,
			logger:     f.logger.WithGroup("entity").WithGroup(er.Name),
		}

		entities = append(entities, entity)
	}

	return entities, nil
}

func (f *fwiImpl) GetEntity(ctx context.Context, name string) (Entity, error) {
	// Check the local in-memory map first for performance.
	f.mu.RLock()
	entity, ok := f.entities[name]
	f.mu.RUnlock()
	if ok {
		return entity, nil
	}

	// If not found in cache, it may have been created by another process.
	// Fall back to searching the database by iterating.
	f.logger.Info("Entity not in local cache, searching database", "name", name)
	entity, err := f.findEntityByName(ctx, name)
	if err != nil {
		// This will be errEntityNotFound if it's not in the DB.
		return nil, err
	}

	// Found it in the DB, so we cache it for future requests.
	f.mu.Lock()
	f.entities[name] = entity
	f.mu.Unlock()

	return entity, nil
}

func (f *fwiImpl) DeleteEntity(ctx context.Context, name string) error {
	// To delete an entity by name, we first have to find it to get its UUID.
	entity, err := f.findEntityByName(ctx, name)
	if err != nil {
		return err // Will be errEntityNotFound if it doesn't exist.
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	entityUUID := entity.GetUUID()
	entityKeyInDB := fmt.Sprintf("%s%s", EntityPrefix, entityUUID)

	if err := withRetriesVoid(ctx, f.logger, func() error {
		return f.rootInsiClient.Delete(entityKeyInDB)
	}); err != nil {
		return fmt.Errorf("failed to delete entity record from db: %w", err)
	}

	if err := withRetriesVoid(ctx, f.logger, func() error {
		return f.rootInsiClient.DeleteAPIKey(entity.GetKey())
	}); err != nil {
		f.logger.Error("Failed to delete API key for entity, but entity record was deleted. Manual cleanup of API key may be required.",
			"entityName", name,
			"apiKey", entity.GetKey(),
			"error", err)
		return fmt.Errorf("failed to delete entity api key: %w", err)
	}

	delete(f.entities, name)
	f.logger.Info("Successfully deleted entity", "name", name, "uuid", entityUUID)

	return nil
}

func (f *fwiImpl) UpdateEntityLimits(
	ctx context.Context,
	name string,
	newLimits models.Limits,
) error {
	entity, err := f.GetEntity(ctx, name)
	if err != nil {
		return err // Will be errEntityNotFound if it doesn't exist.
	}

	apiKey := entity.GetKey()

	// Get current limits to merge with the new limits and validate.
	currentLimitsResp, err := withRetries(ctx, f.logger, func() (*models.LimitsResponse, error) {
		return f.rootInsiClient.GetLimitsForKey(apiKey)
	})
	if err != nil {
		return fmt.Errorf("failed to get current limits for entity '%s': %w", name, err)
	}

	// Start with existing limits, or a fresh struct if none are set.
	mergedLimits := models.Limits{}
	if currentLimitsResp.MaxLimits != nil {
		mergedLimits = *currentLimitsResp.MaxLimits
	}

	if newLimits.BytesInMemory != nil {
		mergedLimits.BytesInMemory = newLimits.BytesInMemory
	}
	if newLimits.BytesOnDisk != nil {
		mergedLimits.BytesOnDisk = newLimits.BytesOnDisk
	}
	if newLimits.EventsEmitted != nil {
		mergedLimits.EventsEmitted = newLimits.EventsEmitted
	}
	if newLimits.Subscribers != nil {
		mergedLimits.Subscribers = newLimits.Subscribers
	}

	// Validate the merged limits.
	if mergedLimits.BytesInMemory != nil && mergedLimits.BytesOnDisk != nil {
		if *mergedLimits.BytesInMemory > *mergedLimits.BytesOnDisk {
			return fmt.Errorf("validation failed: bytes in memory (%d) cannot be greater than bytes on disk (%d)",
				*mergedLimits.BytesInMemory, *mergedLimits.BytesOnDisk)
		}
	}

	// Now set the updated and validated limits.
	if err := withRetriesVoid(ctx, f.logger, func() error {
		return f.rootInsiClient.SetLimits(apiKey, mergedLimits)
	}); err != nil {
		return fmt.Errorf("failed to update limits for entity '%s': %w", name, err)
	}

	f.logger.Info("Successfully updated limits for entity", "name", name)
	return nil
}

func (f *fwiImpl) GetOpsPerSecond(ctx context.Context) (*models.OpsPerSecondCounters, error) {
	return withRetries(ctx, f.logger, func() (*models.OpsPerSecondCounters, error) {
		return f.rootInsiClient.GetOpsPerSecond()
	})
}

func (f *fwiImpl) findEntityByName(ctx context.Context, name string) (Entity, error) {
	keys, err := withRetries(ctx, f.logger, func() ([]string, error) {
		return f.rootInsiClient.IterateByPrefix(EntityPrefix, 0, 2048)
	})
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			return nil, errEntityNotFound
		}
		return nil, err
	}

	for _, key := range keys {
		entityRecordStr, err := withRetries(ctx, f.logger, func() (string, error) {
			return f.rootInsiClient.Get(key)
		})
		if err != nil {
			f.logger.Warn("Failed to get entity record during name search, skipping", "key", key, "error", err)
			continue
		}

		var er entityRecord
		if err := json.Unmarshal([]byte(entityRecordStr), &er); err != nil {
			f.logger.Warn(
				"Failed to unmarshal entity record during name search, skipping", "key", key, "error", err)
			continue
		}

		if er.Name == name {
			// When finding an entity, we must create a new client for it
			// with the correct API key and configuration inherited from the FWI instance.
			cfgc := client.Config{
				ApiKey:         er.Key,
				Endpoints:      f.insiCfg.Endpoints,
				Logger:         f.insiCfg.Logger.WithGroup(er.Name),
				ConnectionType: f.insiCfg.ConnectionType,
				SkipVerify:     f.insiCfg.SkipVerify,
			}
			entityClient, err := client.NewClient(&cfgc)
			if err != nil {
				// Log the error but continue, as other entities might be valid.
				f.logger.Error("Failed to create client for found entity", "name", er.Name, "error", err)
				continue
			}

			entity := &entityImpl{
				name:       er.Name,
				insiClient: entityClient,
				record:     &er,
				logger:     f.logger.WithGroup("entity").WithGroup(er.Name),
			}
			return entity, nil
		}
	}

	return nil, errEntityNotFound
}
