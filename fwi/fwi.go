package fwi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/db/models"
	"github.com/google/uuid"
)

const (
	EntityPrefix = "entity_record:"
)

var (
	// errEntityNotFound is returned when an entity cannot be found by name after a full scan.
	errEntityNotFound = errors.New("entity not found")
)

// Abstracts
type KV interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	IterateKeys(prefix string, offset, limit int) ([]string, error)
	Delete(key string) error

	CompareAndSwap(key string, oldValue, newValue string) error
	SetNX(key string, value string) error
}

type Events interface {
	Subscribe(ctx context.Context, topic string, onEvent func(data any)) error
	Publish(topic string, data any) error
}

type Entity interface {
	GetName() string
	GetKey() string
	GetUUID() string

	// Use the insi client to bump an integer value
	Bump(key string, value int) error
	GetValueStore() KV // Get the value store for the entity
	GetCacheStore() KV // Get the cache store for the entity
	GetEvents() Events // Get the event pub/sub for the entity
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
}

// ------------------------------------------------------------------------------------------------

type entityImpl struct {
	name       string
	insiClient *client.Client
	record     *entityRecord
}

type valueStoreImpl struct {
	insiClient *client.Client
}

type cacheStoreImpl struct {
	insiClient *client.Client
}

type eventsImpl struct {
	insiClient *client.Client
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

func (e *entityImpl) GetName() string {
	return e.name
}

func (e *entityImpl) GetKey() string {
	return e.insiClient.GetApiKey()
}

func (e *entityImpl) Bump(key string, value int) error {
	return e.insiClient.Bump(key, value)
}

func (e *entityImpl) GetUUID() string {
	return e.record.UUID
}

func (e *entityImpl) GetValueStore() KV {
	return &valueStoreImpl{
		insiClient: e.insiClient,
	}
}

func (e *entityImpl) GetCacheStore() KV {
	return &cacheStoreImpl{
		insiClient: e.insiClient,
	}
}

func (e *entityImpl) GetEvents() Events {
	return &eventsImpl{
		insiClient: e.insiClient,
	}
}

func (e *valueStoreImpl) Get(key string) (string, error) {
	return e.insiClient.Get(key)
}

func (e *valueStoreImpl) Set(key string, value string) error {
	return e.insiClient.Set(key, value)
}

func (e *valueStoreImpl) IterateKeys(prefix string, offset, limit int) ([]string, error) {
	return e.insiClient.IterateByPrefix(prefix, offset, limit)
}

func (e *valueStoreImpl) Delete(key string) error {
	return e.insiClient.Delete(key)
}

func (e *valueStoreImpl) CompareAndSwap(key string, oldValue, newValue string) error {
	return e.insiClient.CompareAndSwap(key, oldValue, newValue)
}

func (e *valueStoreImpl) SetNX(key string, value string) error {
	return e.insiClient.SetNX(key, value)
}

func (e *cacheStoreImpl) Get(key string) (string, error) {
	return e.insiClient.GetCache(key)
}

func (e *cacheStoreImpl) Set(key string, value string) error {
	return e.insiClient.SetCache(key, value)
}

func (e *cacheStoreImpl) IterateKeys(prefix string, offset, limit int) ([]string, error) {
	return e.insiClient.IterateCacheByPrefix(prefix, offset, limit)
}

func (e *cacheStoreImpl) Delete(key string) error {
	return e.insiClient.DeleteCache(key)
}

func (e *cacheStoreImpl) CompareAndSwap(key string, oldValue, newValue string) error {
	return e.insiClient.CompareAndSwapCache(key, oldValue, newValue)
}

func (e *cacheStoreImpl) SetNX(key string, value string) error {
	return e.insiClient.SetCacheNX(key, value)
}

func (e *eventsImpl) Subscribe(
	ctx context.Context,
	topic string,
	onEvent func(data any),
) error {
	return e.insiClient.SubscribeToEvents(topic, ctx, onEvent)
}

func (e *eventsImpl) Publish(topic string, data any) error {
	return e.insiClient.PublishEvent(topic, data)
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

	key, err := f.rootInsiClient.CreateAPIKey(name)
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

	if err := f.rootInsiClient.Set(entityKey, string(entityEncoded)); err != nil {
		return nil, err
	}

	cfgc := client.Config{
		ApiKey:    key.Key,
		Endpoints: f.insiCfg.Endpoints,
		Logger:    f.insiCfg.Logger.WithGroup(name),
	}

	entityClient, err := client.NewClient(&cfgc)
	if err != nil {
		return nil, err
	}

	entity := &entityImpl{
		name:       name,
		insiClient: entityClient,
		record:     entityRecord,
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
	keys, err := f.rootInsiClient.IterateByPrefix(EntityPrefix, 0, 2048)
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			return []Entity{}, nil // No entities found is not an error.
		}
		return nil, err
	}

	entities := make([]Entity, 0, len(keys))

	for _, key := range keys {
		entityRecordStr, err := f.rootInsiClient.Get(key)
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

	if err := f.rootInsiClient.Delete(entityKeyInDB); err != nil {
		return fmt.Errorf("failed to delete entity record from db: %w", err)
	}

	if err := f.rootInsiClient.DeleteAPIKey(entity.GetKey()); err != nil {
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

func (f *fwiImpl) findEntityByName(ctx context.Context, name string) (Entity, error) {
	keys, err := f.rootInsiClient.IterateByPrefix(EntityPrefix, 0, 2048)
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			return nil, errEntityNotFound
		}
		return nil, err
	}

	for _, key := range keys {
		entityRecordStr, err := f.rootInsiClient.Get(key)
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
			entityClient := f.rootInsiClient.DeriveWithApiKey(er.Name, er.Key)
			entity := &entityImpl{
				name:       er.Name,
				insiClient: entityClient,
				record:     &er,
			}
			return entity, nil
		}
	}

	return nil, errEntityNotFound
}
