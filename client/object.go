package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
)

type StoreType string

const (
	StoreTypeValue StoreType = "value"
	StoreTypeCache StoreType = "cache"
)

var (
	ErrUniqueConstraintConflict = errors.New("conflict: unique constraint failed")
	ErrObjectNotFound           = errors.New("object not found")
)

// TransformationFunc is a function that transforms a value before it's stored.
type TransformationFunc func(ctx context.Context, input any) (any, error)

// --- Struct tag parsing metadata ---

type fieldInfo struct {
	Name          string
	Index         int
	Key           string // storage key name
	IsPrimary     bool
	IsUnique      bool
	TransformName string
	NoRead        bool
}

type typeInfo struct {
	Type           reflect.Type
	Fields         map[string]fieldInfo // map from struct field name to info
	PrimaryKeyName string               // struct field name of the primary key
	ObjectPrefix   string
}

// --- ObjectManager ---

// ObjectManager is a generic controller for a specific Go struct type.
type ObjectManager[T any] struct {
	logger         *slog.Logger
	insiClient     *Client
	typeInfo       *typeInfo
	transformFuncs map[string]TransformationFunc
	storeType      StoreType
}

// ObjectInstance represents a single instance of a managed object.
type ObjectInstance[T any] struct {
	manager *ObjectManager[T]
	data    *T
}

func (i *ObjectInstance[T]) Data() *T {
	return i.data
}

// --- ObjectManagerBuilder ---

// ObjectManagerBuilder is used to construct an ObjectManager.
type ObjectManagerBuilder[T any] struct {
	logger         *slog.Logger
	insiClient     *Client
	objectPrefix   string
	transformFuncs map[string]TransformationFunc
	storeType      StoreType
}

// NewObjectManagerBuilder creates a new builder for an ObjectManager.
func NewObjectManagerBuilder[T any](logger *slog.Logger, insiClient *Client, objectPrefix string, storeType StoreType) *ObjectManagerBuilder[T] {
	if storeType == "" {
		storeType = StoreTypeValue
	}
	return &ObjectManagerBuilder[T]{
		logger:         logger.WithGroup("object_manager_builder"),
		insiClient:     insiClient,
		objectPrefix:   objectPrefix,
		transformFuncs: make(map[string]TransformationFunc),
		storeType:      storeType,
	}
}

// WithTransformation registers a named transformation function.
func (b *ObjectManagerBuilder[T]) WithTransformation(name string, fn TransformationFunc) *ObjectManagerBuilder[T] {
	b.transformFuncs[name] = fn
	return b
}

// Build creates and initializes the ObjectManager.
func (b *ObjectManagerBuilder[T]) Build() (*ObjectManager[T], error) {
	var t T
	typ := reflect.TypeOf(t)
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("type must be a struct, got %s", typ.Kind())
	}

	info := &typeInfo{
		Type:         typ,
		Fields:       make(map[string]fieldInfo),
		ObjectPrefix: b.objectPrefix,
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("insi")
		if tag == "" || tag == "-" {
			continue
		}

		fi := fieldInfo{
			Name:  field.Name,
			Index: i,
		}

		parts := strings.Split(tag, ",")
		for _, part := range parts {
			kv := strings.SplitN(part, ":", 2)
			switch kv[0] {
			case "key":
				if len(kv) > 1 {
					fi.Key = kv[1]
				}
			case "primary":
				if info.PrimaryKeyName != "" {
					return nil, fmt.Errorf("multiple primary keys defined for type %s", typ.Name())
				}
				info.PrimaryKeyName = field.Name
				fi.IsPrimary = true
			case "unique":
				fi.IsUnique = true
			case "transform":
				if len(kv) > 1 {
					fi.TransformName = kv[1]
				}
			case "noread":
				fi.NoRead = true
			}
		}

		if fi.Key == "" {
			fi.Key = strings.ToLower(field.Name)
		}

		info.Fields[field.Name] = fi
	}

	if info.PrimaryKeyName == "" {
		return nil, fmt.Errorf("no primary key defined for type %s", typ.Name())
	}

	manager := &ObjectManager[T]{
		logger:         b.logger.With("object_type", typ.Name()),
		insiClient:     b.insiClient,
		typeInfo:       info,
		transformFuncs: b.transformFuncs,
		storeType:      b.storeType,
	}

	return manager, nil
}

// --- Key Generation ---

func (m *ObjectManager[T]) getObjectKeyPrefix(primaryKeyValue string) string {
	return fmt.Sprintf("%s:%s:", m.typeInfo.ObjectPrefix, primaryKeyValue)
}

func (m *ObjectManager[T]) GetFieldKey(primaryKeyValue, fieldKey string) string {
	return fmt.Sprintf("%s%s", m.getObjectKeyPrefix(primaryKeyValue), fieldKey)
}

func (m *ObjectManager[T]) GetUniqueLookupKey(fieldKey string, fieldValue any) string {
	return fmt.Sprintf("%s:%s:%v", m.typeInfo.ObjectPrefix, fieldKey, fieldValue)
}

// --- Datastore Abstractions ---

func (m *ObjectManager[T]) get(key string) (string, error) {
	if m.storeType == StoreTypeCache {
		return m.insiClient.GetCache(key)
	}
	return m.insiClient.Get(key)
}

func (m *ObjectManager[T]) set(key, value string) error {
	if m.storeType == StoreTypeCache {
		return m.insiClient.SetCache(key, value)
	}
	return m.insiClient.Set(key, value)
}

func (m *ObjectManager[T]) setNX(key, value string) error {
	if m.storeType == StoreTypeCache {
		return m.insiClient.SetCacheNX(key, value)
	}
	return m.insiClient.SetNX(key, value)
}

func (m *ObjectManager[T]) compareAndSwap(key, oldValue, newValue string) error {
	if m.storeType == StoreTypeCache {
		return m.insiClient.CompareAndSwapCache(key, oldValue, newValue)
	}
	return m.insiClient.CompareAndSwap(key, oldValue, newValue)
}

func (m *ObjectManager[T]) delete(key string) error {
	if m.storeType == StoreTypeCache {
		return m.insiClient.DeleteCache(key)
	}
	return m.insiClient.Delete(key)
}

func (m *ObjectManager[T]) iterateByPrefix(prefix string, offset, limit int) ([]string, error) {
	if m.storeType == StoreTypeCache {
		return m.insiClient.IterateCacheByPrefix(prefix, offset, limit)
	}
	return m.insiClient.IterateByPrefix(prefix, offset, limit)
}

// --- CRUD Operations ---

// New creates a new object in the data store.
func (m *ObjectManager[T]) New(ctx context.Context, data *T) (*ObjectInstance[T], error) {
	val := reflect.ValueOf(data).Elem()
	pkField := val.FieldByName(m.typeInfo.PrimaryKeyName)

	if !pkField.IsValid() || !pkField.CanSet() {
		return nil, fmt.Errorf("primary key field %s is not valid or settable", m.typeInfo.PrimaryKeyName)
	}

	newUUID := uuid.NewString()
	if pkField.Kind() != reflect.String {
		return nil, fmt.Errorf("primary key must be a string to hold a UUID")
	}
	pkField.SetString(newUUID)

	createdUniqueKeys := []string{}
	var creationErr error

	defer func() {
		if creationErr != nil {
			m.logger.Error("creation failed, cleaning up unique keys", "error", creationErr)
			for _, key := range createdUniqueKeys {
				err := WithRetriesVoid(ctx, m.logger, func() error {
					return m.delete(key)
				})
				if err != nil {
					m.logger.Error("failed to cleanup unique key", "key", key, "error", err)
				}
			}
		}
	}()

	fieldsToSet := make(map[string]string)

	for _, fi := range m.typeInfo.Fields {
		if fi.IsPrimary {
			continue
		}

		fieldVal := val.Field(fi.Index)
		var finalValue any = fieldVal.Interface()

		if fi.TransformName != "" {
			transformFn, ok := m.transformFuncs[fi.TransformName]
			if !ok {
				creationErr = fmt.Errorf("transformation function '%s' not found", fi.TransformName)
				return nil, creationErr
			}
			transformed, err := transformFn(ctx, finalValue)
			if err != nil {
				creationErr = fmt.Errorf("failed to transform field %s: %w", fi.Name, err)
				return nil, creationErr
			}
			finalValue = transformed
		}

		if fi.IsUnique {
			uniqueKey := m.GetUniqueLookupKey(fi.Key, finalValue)
			err := WithRetriesVoid(ctx, m.logger, func() error {
				return m.setNX(uniqueKey, newUUID)
			})
			if err != nil {
				if errors.Is(err, ErrConflict) {
					creationErr = fmt.Errorf("field %s with value %v already exists: %w", fi.Name, finalValue, ErrUniqueConstraintConflict)
				} else {
					creationErr = fmt.Errorf("failed to set unique key for %s: %w", fi.Name, err)
				}
				return nil, creationErr
			}
			createdUniqueKeys = append(createdUniqueKeys, uniqueKey)
		}

		storageValue := m.formatValueForStorage(finalValue)
		fieldsToSet[m.GetFieldKey(newUUID, fi.Key)] = storageValue
	}

	now := time.Now().UTC().Format(time.RFC3339)
	fieldsToSet[m.GetFieldKey(newUUID, "created_at")] = now
	fieldsToSet[m.GetFieldKey(newUUID, "updated_at")] = now

	for key, value := range fieldsToSet {
		err := WithRetriesVoid(ctx, m.logger, func() error {
			return m.set(key, value)
		})
		if err != nil {
			creationErr = fmt.Errorf("failed to set field for key %s: %w", key, err)
			// Attempt to delete the main object record as well
			_ = m.Delete(ctx, newUUID)
			return nil, creationErr
		}
	}

	return &ObjectInstance[T]{
		manager: m,
		data:    data,
	}, nil
}

// GetByUUID retrieves an object by its primary key (UUID).
func (m *ObjectManager[T]) GetByUUID(ctx context.Context, uuid string) (*ObjectInstance[T], error) {
	instance := new(T)
	val := reflect.ValueOf(instance).Elem()

	for _, fi := range m.typeInfo.Fields {
		if fi.NoRead || fi.IsPrimary {
			continue
		}
		fieldKey := m.GetFieldKey(uuid, fi.Key)
		valueStr, err := WithRetries(ctx, m.logger, func() (string, error) {
			return m.get(fieldKey)
		})

		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return nil, ErrObjectNotFound
			}
			return nil, fmt.Errorf("failed to get field %s: %w", fi.Name, err)
		}

		field := val.Field(fi.Index)
		if err := m.setFieldFromString(field, valueStr); err != nil {
			return nil, fmt.Errorf("failed to set field %s from string: %w", fi.Name, err)
		}
	}
	val.FieldByName(m.typeInfo.PrimaryKeyName).SetString(uuid)

	return &ObjectInstance[T]{
		manager: m,
		data:    instance,
	}, nil
}

// GetByUniqueField retrieves an object by a unique field.
func (m *ObjectManager[T]) GetByUniqueField(ctx context.Context, fieldName string, fieldValue any) (*ObjectInstance[T], error) {
	fi, ok := m.typeInfo.Fields[fieldName]
	if !ok {
		return nil, fmt.Errorf("field %s not defined in type info for %s", fieldName, m.typeInfo.Type.Name())
	}
	if !fi.IsUnique {
		return nil, fmt.Errorf("field %s is not a unique field", fieldName)
	}

	uniqueKey := m.GetUniqueLookupKey(fi.Key, fieldValue)
	uuid, err := WithRetries(ctx, m.logger, func() (string, error) {
		return m.get(uniqueKey)
	})

	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("failed to look up uuid by unique key %s: %w", uniqueKey, err)
	}

	return m.GetByUUID(ctx, uuid)
}

// SetField sets a single field for an object.
func (m *ObjectManager[T]) SetField(ctx context.Context, uuid, fieldName, value string) error {
	fi, ok := m.typeInfo.Fields[fieldName]
	if !ok {
		return fmt.Errorf("field %s not defined in type info for %s", fieldName, m.typeInfo.Type.Name())
	}
	key := m.GetFieldKey(uuid, fi.Key)
	return WithRetriesVoid(ctx, m.logger, func() error {
		return m.set(key, value)
	})
}

// GetField gets a single raw field for an object.
func (m *ObjectManager[T]) GetField(ctx context.Context, uuid, fieldName string) (string, error) {
	fi, ok := m.typeInfo.Fields[fieldName]
	if !ok {
		return "", fmt.Errorf("field %s not defined in type info for %s", fieldName, m.typeInfo.Type.Name())
	}
	key := m.GetFieldKey(uuid, fi.Key)
	return WithRetries(ctx, m.logger, func() (string, error) {
		return m.get(key)
	})
}

// CompareAndSwapField performs a CAS operation on a single field.
func (m *ObjectManager[T]) CompareAndSwapField(ctx context.Context, uuid, fieldName, oldVal, newVal string) error {
	fi, ok := m.typeInfo.Fields[fieldName]
	if !ok {
		return fmt.Errorf("field %s not defined in type info for %s", fieldName, m.typeInfo.Type.Name())
	}
	key := m.GetFieldKey(uuid, fi.Key)
	return WithRetriesVoid(ctx, m.logger, func() error {
		return m.compareAndSwap(key, oldVal, newVal)
	})
}

// Delete removes an object and its associated unique keys.
func (m *ObjectManager[T]) Delete(ctx context.Context, uuid string) error {
	keysToDelete := []string{}
	instanceVal := reflect.ValueOf(new(T)).Elem()

	for _, fi := range m.typeInfo.Fields {
		fieldKey := m.GetFieldKey(uuid, fi.Key)
		keysToDelete = append(keysToDelete, fieldKey)

		if fi.IsUnique {
			valueStr, err := WithRetries(ctx, m.logger, func() (string, error) {
				return m.get(fieldKey)
			})
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				m.logger.Warn("could not retrieve unique field value for deletion", "field", fi.Name, "error", err)
				continue
			}
			if err == nil {
				field := instanceVal.Field(fi.Index)
				if err := m.setFieldFromString(field, valueStr); err == nil {
					uniqueKey := m.GetUniqueLookupKey(fi.Key, field.Interface())
					keysToDelete = append(keysToDelete, uniqueKey)
				}
			}
		}
	}
	keysToDelete = append(keysToDelete, m.GetFieldKey(uuid, "created_at"), m.GetFieldKey(uuid, "updated_at"))

	for _, key := range keysToDelete {
		err := WithRetriesVoid(ctx, m.logger, func() error {
			return m.delete(key)
		})
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			m.logger.Error("failed to delete key", "key", key, "error", err)
		}
	}

	return nil
}

// SetUniqueNX sets a unique key, but only if it does not already exist.
func (m *ObjectManager[T]) SetUniqueNX(ctx context.Context, uuid, fieldName, fieldValue string) error {
	fi, ok := m.typeInfo.Fields[fieldName]
	if !ok {
		return fmt.Errorf("field %s not defined for type %s", fieldName, m.typeInfo.Type.Name())
	}
	if !fi.IsUnique {
		return fmt.Errorf("field %s is not a unique field", fieldName)
	}

	uniqueKey := m.GetUniqueLookupKey(fi.Key, fieldValue)
	return WithRetriesVoid(ctx, m.logger, func() error {
		return m.setNX(uniqueKey, uuid)
	})
}

// DeleteUnique removes a unique key.
func (m *ObjectManager[T]) DeleteUnique(ctx context.Context, fieldName, fieldValue string) error {
	fi, ok := m.typeInfo.Fields[fieldName]
	if !ok {
		return fmt.Errorf("field %s not defined for type %s", fieldName, m.typeInfo.Type.Name())
	}
	if !fi.IsUnique {
		return fmt.Errorf("field %s is not a unique field", fieldName)
	}

	uniqueKey := m.GetUniqueLookupKey(fi.Key, fieldValue)
	return WithRetriesVoid(ctx, m.logger, func() error {
		// We ignore "not found" errors on delete for idempotency.
		err := m.delete(uniqueKey)
		if errors.Is(err, ErrKeyNotFound) {
			return nil
		}
		return err
	})
}

// List retrieves all objects of a given type.
// It iterates over a unique field to discover all objects.
func (m *ObjectManager[T]) List(ctx context.Context, uniqueFieldForDiscovery string) ([]*ObjectInstance[T], error) {
	fi, ok := m.typeInfo.Fields[uniqueFieldForDiscovery]
	if !ok {
		return nil, fmt.Errorf("field %s not defined for type %s", uniqueFieldForDiscovery, m.typeInfo.Type.Name())
	}
	if !fi.IsUnique {
		return nil, fmt.Errorf("field %s must be unique for discovery", uniqueFieldForDiscovery)
	}

	scanPrefix := m.GetUniqueLookupKey(fi.Key, "")
	var instances []*ObjectInstance[T]
	offset := 0
	limit := 100 // Process in batches of 100

	for {
		keys, err := WithRetries(ctx, m.logger, func() ([]string, error) {
			return m.iterateByPrefix(scanPrefix, offset, limit)
		})

		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				break
			}
			m.logger.Error("failed to iterate for object keys", "prefix", scanPrefix, "error", err)
			return nil, fmt.Errorf("could not list objects: %w", err)
		}

		if len(keys) == 0 {
			break
		}

		for _, key := range keys {
			uuid, getErr := WithRetries(ctx, m.logger, func() (string, error) {
				return m.get(key)
			})
			if getErr != nil {
				m.logger.Warn("failed to get uuid for key while listing", "key", key, "error", getErr)
				continue
			}

			instance, getErr := m.GetByUUID(ctx, uuid)
			if getErr != nil {
				m.logger.Warn("failed to get object by uuid while listing", "uuid", uuid, "error", getErr)
				continue
			}
			instances = append(instances, instance)
		}

		if len(keys) < limit {
			break
		}
		offset += len(keys)
	}

	return instances, nil
}

// --- Helpers ---

func (m *ObjectManager[T]) formatValueForStorage(v any) string {
	if t, ok := v.(time.Time); ok {
		return t.Format(time.RFC3339)
	}
	return fmt.Sprintf("%v", v)
}

func (m *ObjectManager[T]) setFieldFromString(field reflect.Value, valueStr string) error {
	if !field.CanSet() {
		return fmt.Errorf("field cannot be set")
	}

	switch field.Kind() {
	case reflect.String:
		field.SetString(valueStr)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Simplified, add proper parsing if needed
		var i int64
		fmt.Sscanf(valueStr, "%d", &i)
		field.SetInt(i)
	// Add other types as needed
	default:
		// Handle time.Time specifically
		if field.Type() == reflect.TypeOf(time.Time{}) {
			t, err := time.Parse(time.RFC3339, valueStr)
			if err != nil {
				return err
			}
			field.Set(reflect.ValueOf(t))
		} else {
			return fmt.Errorf("unsupported field type for setting from string: %s", field.Kind())
		}
	}
	return nil
}
