package ferry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"
)

// RecordManager handles structured records stored as individual fields in KV backend
type RecordManager interface {
	// Register a record type with a name
	Register(recordType string, example interface{}) error

	// Create a new instance of a registered record type
	NewInstance(ctx context.Context, recordType, instanceName string, data interface{}) error

	// Get an entire record
	GetRecord(ctx context.Context, recordType, instanceName string) (interface{}, error)

	// Get a specific field from a record
	GetRecordField(ctx context.Context, recordType, instanceName, fieldName string) (interface{}, error)

	// Update an entire record
	SetRecord(ctx context.Context, recordType, instanceName string, data interface{}) error

	// SetRecordIfMatch updates a record only if it matches the expected state
	// Uses Compare-And-Swap to prevent race conditions
	SetRecordIfMatch(ctx context.Context, recordType, instanceName string, expected, data interface{}) error

	// Update a specific field in a record
	SetRecordField(ctx context.Context, recordType, instanceName, fieldName string, value interface{}) error

	// Delete an entire record
	DeleteRecord(ctx context.Context, recordType, instanceName string) error

	// Sync methods for reading from backend
	SyncRecord(ctx context.Context, recordType, instanceName string) (interface{}, error)
	SyncRecordField(ctx context.Context, recordType, instanceName, fieldName string) (interface{}, error)

	// Upgrade all instances of a record type
	UpgradeRecord(ctx context.Context, recordType string, upgrader interface{}) error

	// List all instances of a record type
	ListInstances(ctx context.Context, recordType string, offset, limit int) ([]string, error)

	// List all registered record types
	ListRecordTypes(ctx context.Context) ([]string, error)

	// List all instances across all record types
	ListAllInstances(ctx context.Context, offset, limit int) (map[string][]string, error)

	// CleanupOldFields removes fields that are no longer in the record type definition
	// Useful after upgrades that remove fields
	CleanupOldFields(ctx context.Context, recordType, instanceName string) error
}

type recordManagerImpl struct {
	ferry    *Ferry
	logger   *slog.Logger
	inProd   bool
	useCache bool

	// Registry of record types
	registry   map[string]reflect.Type
	registryMu sync.RWMutex

	// Controllers
	valueCtrl ValueController[string]
	cacheCtrl CacheController[string]
}

// RecordManagerOption configures the RecordManager
type RecordManagerOption func(*recordManagerImpl)

// WithCache uses cache backend instead of value backend
func WithCache() RecordManagerOption {
	return func(rm *recordManagerImpl) {
		rm.useCache = true
	}
}

// WithProduction sets production mode
func WithProduction() RecordManagerOption {
	return func(rm *recordManagerImpl) {
		rm.inProd = true
	}
}

// NewRecordManager creates a new record manager
func NewRecordManager(ferry *Ferry, logger *slog.Logger, opts ...RecordManagerOption) RecordManager {
	rm := &recordManagerImpl{
		ferry:    ferry,
		logger:   logger.WithGroup("record_manager"),
		registry: make(map[string]reflect.Type),
		inProd:   false,
		useCache: false,
	}

	// Apply options
	for _, opt := range opts {
		opt(rm)
	}

	// Initialize controllers
	rm.valueCtrl = GetValueController(ferry, "")
	rm.cacheCtrl = GetCacheController(ferry, "")

	// Set up the base scope with records prefix
	scope := "dev"
	if rm.inProd {
		scope = "prod"
	}

	if rm.useCache {
		rm.cacheCtrl.PushScope(scope)
		rm.cacheCtrl.PushScope("records")
	} else {
		rm.valueCtrl.PushScope(scope)
		rm.valueCtrl.PushScope("records")
	}

	return rm
}

func (rm *recordManagerImpl) Register(recordType string, example interface{}) error {
	if recordType == "" {
		return errors.New("record type cannot be empty")
	}

	// Prevent colons in record type to avoid key parsing issues
	if strings.Contains(recordType, ":") {
		return errors.New("record type cannot contain colons (:)")
	}

	typ := reflect.TypeOf(example)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return errors.New("record type must be a struct")
	}

	// Validate field names don't conflict with reserved names
	reservedNames := map[string]bool{
		"__meta__":  true,
		"__index__": true,
		"__type__":  true,
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.IsExported() {
			if reservedNames[field.Name] {
				return fmt.Errorf("field name %q is reserved and cannot be used", field.Name)
			}

			// Also check for fields that could cause parsing issues
			if strings.Contains(field.Name, ":") {
				return fmt.Errorf("field name %q cannot contain colons (:)", field.Name)
			}

			if strings.HasPrefix(field.Name, "__") && strings.HasSuffix(field.Name, "__") {
				return fmt.Errorf("field name %q uses reserved pattern __name__", field.Name)
			}
		}
	}

	rm.registryMu.Lock()
	rm.registry[recordType] = typ
	rm.registryMu.Unlock()

	rm.logger.Info("Registered record type", "type", recordType, "struct", typ.Name())
	return nil
}

func (rm *recordManagerImpl) buildKey(recordType, instanceName, fieldName string) string {
	parts := []string{recordType, instanceName}
	if fieldName != "" {
		parts = append(parts, fieldName)
	}
	return strings.Join(parts, ":")
}

// buildIndexKey creates a key for the instance index
func (rm *recordManagerImpl) buildIndexKey(recordType, instanceName string) string {
	return fmt.Sprintf("%s:%s:__meta__", recordType, instanceName)
}

// buildInstanceIndexKey creates a key for the instance index
func (rm *recordManagerImpl) buildInstanceIndexKey(recordType, instanceName string) string {
	return fmt.Sprintf("__instances__:%s:%s", recordType, instanceName)
}

// addToInstanceIndex adds an instance to the index
func (rm *recordManagerImpl) addToInstanceIndex(ctx context.Context, recordType, instanceName string) error {
	indexKey := rm.buildInstanceIndexKey(recordType, instanceName)

	// Store with timestamp for potential future sorting
	timestamp := time.Now().Unix()
	value := fmt.Sprintf("%d", timestamp)

	if rm.useCache {
		return rm.cacheCtrl.Set(ctx, indexKey, value)
	}
	return rm.valueCtrl.Set(ctx, indexKey, value)
}

// removeFromInstanceIndex removes an instance from the index
func (rm *recordManagerImpl) removeFromInstanceIndex(ctx context.Context, recordType, instanceName string) error {
	indexKey := rm.buildInstanceIndexKey(recordType, instanceName)

	if rm.useCache {
		return rm.cacheCtrl.Delete(ctx, indexKey)
	}
	return rm.valueCtrl.Delete(ctx, indexKey)
}

func (rm *recordManagerImpl) NewInstance(ctx context.Context, recordType, instanceName string, data interface{}) error {
	// FIX: Validate instance name to prevent key parsing issues
	if instanceName == "" {
		return errors.New("instance name cannot be empty")
	}
	if strings.Contains(instanceName, ":") {
		return errors.New("instance name cannot contain colons (:)")
	}

	rm.registryMu.RLock()
	registeredType, exists := rm.registry[recordType]
	rm.registryMu.RUnlock()

	if !exists {
		return fmt.Errorf("record type %s not registered", recordType)
	}

	// Validate that the data matches the registered type
	dataType := reflect.TypeOf(data)
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
	}

	if dataType != registeredType {
		return fmt.Errorf("data type %s does not match registered type %s", dataType.Name(), registeredType.Name())
	}

	// Compute checksum for the new record
	checksum, err := rm.computeRecordChecksum(data)
	if err != nil {
		return fmt.Errorf("failed to compute record checksum: %w", err)
	}

	// Try to create the instance atomically using SetNX on the metadata key
	indexKey := rm.buildIndexKey(recordType, instanceName)
	var setNXErr error

	if rm.useCache {
		// For cache, we need to check if key exists first since we might not have SetNX
		existing, getErr := rm.cacheCtrl.Get(ctx, indexKey)
		if getErr == nil && existing != "" {
			return fmt.Errorf("instance %s already exists", instanceName)
		}
		// If key doesn't exist, set it
		setNXErr = rm.cacheCtrl.Set(ctx, indexKey, checksum)
	} else {
		// For value store, try to use SetNX if available, otherwise check+set
		existing, getErr := rm.valueCtrl.Get(ctx, indexKey)
		if getErr == nil && existing != "" {
			return fmt.Errorf("instance %s already exists", instanceName)
		}
		// If key doesn't exist, set it
		setNXErr = rm.valueCtrl.Set(ctx, indexKey, checksum)
	}

	if setNXErr != nil {
		return fmt.Errorf("failed to create instance: %w", setNXErr)
	}

	// Now store all the fields
	if err := rm.storeRecordFields(ctx, recordType, instanceName, data); err != nil {
		// Cleanup the metadata key on failure
		if rm.useCache {
			_ = rm.cacheCtrl.Delete(ctx, indexKey)
		} else {
			_ = rm.valueCtrl.Delete(ctx, indexKey)
		}
		return fmt.Errorf("failed to store record fields: %w", err)
	}

	return nil
}

func (rm *recordManagerImpl) GetRecord(ctx context.Context, recordType, instanceName string) (interface{}, error) {
	return rm.SyncRecord(ctx, recordType, instanceName)
}

func (rm *recordManagerImpl) SyncRecord(ctx context.Context, recordType, instanceName string) (interface{}, error) {
	rm.registryMu.RLock()
	registeredType, exists := rm.registry[recordType]
	rm.registryMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("record type %s not registered", recordType)
	}

	// Create a new instance of the registered type
	newInstance := reflect.New(registeredType).Elem()

	// Load each field
	for i := 0; i < registeredType.NumField(); i++ {
		field := registeredType.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		key := rm.buildKey(recordType, instanceName, field.Name)

		var valueStr string
		var err error

		if rm.useCache {
			valueStr, err = rm.cacheCtrl.Get(ctx, key)
		} else {
			valueStr, err = rm.valueCtrl.Get(ctx, key)
		}

		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				// Field doesn't exist, skip it
				continue
			}
			return nil, fmt.Errorf("failed to get field %s: %w", field.Name, err)
		}

		// Set the field value
		fieldValue := newInstance.Field(i)
		if err := rm.setFieldValue(fieldValue, field.Type, valueStr); err != nil {
			return nil, fmt.Errorf("failed to set field %s: %w", field.Name, err)
		}
	}

	return newInstance.Interface(), nil
}

func (rm *recordManagerImpl) GetRecordField(ctx context.Context, recordType, instanceName, fieldName string) (interface{}, error) {
	return rm.SyncRecordField(ctx, recordType, instanceName, fieldName)
}

func (rm *recordManagerImpl) SyncRecordField(ctx context.Context, recordType, instanceName, fieldName string) (interface{}, error) {
	rm.registryMu.RLock()
	registeredType, exists := rm.registry[recordType]
	rm.registryMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("record type %s not registered", recordType)
	}

	// Find the field
	field, found := registeredType.FieldByName(fieldName)
	if !found {
		return nil, fmt.Errorf("field %s not found in record type %s", fieldName, recordType)
	}

	key := rm.buildKey(recordType, instanceName, fieldName)

	var valueStr string
	var err error

	if rm.useCache {
		valueStr, err = rm.cacheCtrl.Get(ctx, key)
	} else {
		valueStr, err = rm.valueCtrl.Get(ctx, key)
	}

	if err != nil {
		return nil, err
	}

	// Convert string to appropriate type
	fieldValue := reflect.New(field.Type).Elem()
	if err := rm.setFieldValue(fieldValue, field.Type, valueStr); err != nil {
		return nil, err
	}

	return fieldValue.Interface(), nil
}

func (rm *recordManagerImpl) SetRecord(ctx context.Context, recordType, instanceName string, data interface{}) error {
	rm.registryMu.RLock()
	registeredType, exists := rm.registry[recordType]
	rm.registryMu.RUnlock()

	if !exists {
		return fmt.Errorf("record type %s not registered", recordType)
	}

	// Validate that the data matches the registered type
	dataType := reflect.TypeOf(data)
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
	}

	if dataType != registeredType {
		return fmt.Errorf("data type %s does not match registered type %s", dataType.Name(), registeredType.Name())
	}

	// First, get the existing checksum for safe rollback
	metaKey := rm.buildIndexKey(recordType, instanceName)
	var oldChecksum string
	var err error

	if rm.useCache {
		oldChecksum, err = rm.cacheCtrl.Get(ctx, metaKey)
	} else {
		oldChecksum, err = rm.valueCtrl.Get(ctx, metaKey)
	}

	// If we can't get the old checksum and it's not because the key doesn't exist, bail out
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return fmt.Errorf("failed to get current checksum: %w", err)
	}

	// For rollback purposes, only backup if record exists
	var backup interface{}
	var hasBackup bool
	if oldChecksum != "" {
		backup, err = rm.GetRecord(ctx, recordType, instanceName)
		if err != nil {
			rm.logger.Warn("Failed to backup existing record, proceeding without rollback capability",
				"type", recordType,
				"instance", instanceName,
				"error", err)
		} else {
			hasBackup = true
		}
	}

	// Compute new checksum
	newChecksum, err := rm.computeRecordChecksum(data)
	if err != nil {
		return fmt.Errorf("failed to compute new checksum: %w", err)
	}

	// Update the checksum first
	var checksumErr error
	if oldChecksum == "" {
		// New record, just set it
		if rm.useCache {
			checksumErr = rm.cacheCtrl.Set(ctx, metaKey, newChecksum)
		} else {
			checksumErr = rm.valueCtrl.Set(ctx, metaKey, newChecksum)
		}
	} else {
		// Existing record, use CAS for safety
		if rm.useCache {
			checksumErr = rm.cacheCtrl.CompareAndSwap(ctx, metaKey, oldChecksum, newChecksum)
		} else {
			checksumErr = rm.valueCtrl.CompareAndSwap(ctx, metaKey, oldChecksum, newChecksum)
		}

		if errors.Is(checksumErr, ErrConflict) {
			return fmt.Errorf("record was modified concurrently, use SetRecordIfMatch for safe updates")
		}
	}

	if checksumErr != nil {
		return fmt.Errorf("failed to update checksum: %w", checksumErr)
	}

	// Now try to store the fields
	if err := rm.storeRecordFields(ctx, recordType, instanceName, data); err != nil {
		// Rollback: restore the old checksum
		rm.logger.Warn("SetRecord failed, attempting rollback", "type", recordType, "instance", instanceName, "error", err)

		if hasBackup && oldChecksum != "" {
			// Use safe rollback that checks if record was modified
			if rollbackErr := rm.safeRollbackRecord(ctx, recordType, instanceName, backup, newChecksum); rollbackErr != nil {
				rm.logger.Error("Rollback failed", "type", recordType, "instance", instanceName, "error", rollbackErr)
				return fmt.Errorf("update failed and rollback failed: update error: %w, rollback error: %v", err, rollbackErr)
			}
			rm.logger.Info("Successfully rolled back to previous state", "type", recordType, "instance", instanceName)
		} else if oldChecksum != "" {
			// Just try to restore the old checksum
			if rm.useCache {
				_ = rm.cacheCtrl.CompareAndSwap(ctx, metaKey, newChecksum, oldChecksum)
			} else {
				_ = rm.valueCtrl.CompareAndSwap(ctx, metaKey, newChecksum, oldChecksum)
			}
		} else {
			// New record that failed, try to clean up
			if rm.useCache {
				_ = rm.cacheCtrl.Delete(ctx, metaKey)
			} else {
				_ = rm.valueCtrl.Delete(ctx, metaKey)
			}
		}

		return err
	}

	return nil
}

func (rm *recordManagerImpl) SetRecordIfMatch(ctx context.Context, recordType, instanceName string, expected, data interface{}) error {
	rm.registryMu.RLock()
	registeredType, exists := rm.registry[recordType]
	rm.registryMu.RUnlock()

	if !exists {
		return fmt.Errorf("record type %s not registered", recordType)
	}

	// Validate that the data matches the registered type
	dataType := reflect.TypeOf(data)
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
	}

	if dataType != registeredType {
		return fmt.Errorf("data type %s does not match registered type %s", dataType.Name(), registeredType.Name())
	}

	// FIXED: Use atomic CompareAndSwap on a version key
	// We'll use the __meta__ key to store a version/checksum

	// First, compute a checksum of the expected record
	expectedChecksum, err := rm.computeRecordChecksum(expected)
	if err != nil {
		return fmt.Errorf("failed to compute expected checksum: %w", err)
	}

	// Compute checksum of the new data
	newChecksum, err := rm.computeRecordChecksum(data)
	if err != nil {
		return fmt.Errorf("failed to compute new checksum: %w", err)
	}

	// Get the metadata key that we'll use for atomic CAS
	metaKey := rm.buildIndexKey(recordType, instanceName)

	// Attempt atomic compare-and-swap on the metadata key
	var casErr error
	if rm.useCache {
		casErr = rm.cacheCtrl.CompareAndSwap(ctx, metaKey, expectedChecksum, newChecksum)
	} else {
		casErr = rm.valueCtrl.CompareAndSwap(ctx, metaKey, expectedChecksum, newChecksum)
	}

	if casErr != nil {
		if errors.Is(casErr, ErrConflict) {
			// The record has been modified since we read it
			rm.logger.Warn("SetRecordIfMatch failed: record was modified", "type", recordType, "instance", instanceName)
			return ErrCASFailed
		}
		return fmt.Errorf("failed to perform atomic CAS: %w", casErr)
	}

	// CAS succeeded, now we can safely update all fields
	// If any field update fails, we need to restore the old checksum
	if err := rm.storeRecordFields(ctx, recordType, instanceName, data); err != nil {
		// Rollback: restore the old checksum
		rm.logger.Error("Failed to store record fields, rolling back", "error", err)

		var rollbackErr error
		if rm.useCache {
			rollbackErr = rm.cacheCtrl.CompareAndSwap(ctx, metaKey, newChecksum, expectedChecksum)
		} else {
			rollbackErr = rm.valueCtrl.CompareAndSwap(ctx, metaKey, newChecksum, expectedChecksum)
		}

		if rollbackErr != nil {
			rm.logger.Error("Failed to rollback checksum", "error", rollbackErr)
			return fmt.Errorf("update failed and rollback failed: update error: %w, rollback error: %v", err, rollbackErr)
		}

		return fmt.Errorf("failed to store record fields: %w", err)
	}

	rm.logger.Info("SetRecordIfMatch successful", "type", recordType, "instance", instanceName)
	return nil
}

// computeRecordChecksum generates a deterministic checksum for a record
func (rm *recordManagerImpl) computeRecordChecksum(data interface{}) (string, error) {
	// Use JSON marshaling to get a consistent representation
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal record for checksum: %w", err)
	}

	// Use SHA256 for a proper hash
	hash := sha256.Sum256(jsonBytes)
	return hex.EncodeToString(hash[:]), nil
}

// storeRecordFields stores just the fields, not the metadata
func (rm *recordManagerImpl) storeRecordFields(ctx context.Context, recordType, instanceName string, data interface{}) error {
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		fieldValue := val.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		key := rm.buildKey(recordType, instanceName, field.Name)

		// Convert field value to string
		var valueStr string
		switch fieldValue.Kind() {
		case reflect.String:
			valueStr = fieldValue.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			valueStr = fmt.Sprintf("%d", fieldValue.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			valueStr = fmt.Sprintf("%d", fieldValue.Uint())
		case reflect.Float32, reflect.Float64:
			valueStr = fmt.Sprintf("%f", fieldValue.Float())
		case reflect.Bool:
			valueStr = fmt.Sprintf("%t", fieldValue.Bool())
		case reflect.Struct:
			// Special handling for time.Time
			if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
				t := fieldValue.Interface().(time.Time)
				valueStr = t.Format(time.RFC3339)
			} else {
				// For other structs, use JSON encoding
				data, err := json.Marshal(fieldValue.Interface())
				if err != nil {
					return fmt.Errorf("failed to marshal field %s: %w", field.Name, err)
				}
				valueStr = string(data)
			}
		default:
			// For complex types, use JSON encoding
			data, err := json.Marshal(fieldValue.Interface())
			if err != nil {
				return fmt.Errorf("failed to marshal field %s: %w", field.Name, err)
			}
			valueStr = string(data)
		}

		// Store the field
		var err error
		if rm.useCache {
			err = rm.cacheCtrl.Set(ctx, key, valueStr)
		} else {
			err = rm.valueCtrl.Set(ctx, key, valueStr)
		}

		if err != nil {
			return fmt.Errorf("failed to store field %s: %w", field.Name, err)
		}
	}

	return nil
}

func (rm *recordManagerImpl) SetRecordField(ctx context.Context, recordType, instanceName, fieldName string, value interface{}) error {
	rm.registryMu.RLock()
	registeredType, exists := rm.registry[recordType]
	rm.registryMu.RUnlock()

	if !exists {
		return fmt.Errorf("record type %s not registered", recordType)
	}

	// Find the field
	field, found := registeredType.FieldByName(fieldName)
	if !found {
		return fmt.Errorf("field %s not found in record type %s", fieldName, recordType)
	}

	// Validate type compatibility
	valueType := reflect.TypeOf(value)
	if !valueType.AssignableTo(field.Type) {
		return fmt.Errorf("value type %s is not assignable to field type %s", valueType, field.Type)
	}

	key := rm.buildKey(recordType, instanceName, fieldName)

	// Convert value to string
	var valueStr string
	switch v := value.(type) {
	case string:
		valueStr = v
	case time.Time:
		valueStr = v.Format(time.RFC3339)
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %w", err)
		}
		valueStr = string(data)
	}

	if rm.useCache {
		return rm.cacheCtrl.Set(ctx, key, valueStr)
	}
	return rm.valueCtrl.Set(ctx, key, valueStr)
}

func (rm *recordManagerImpl) DeleteRecord(ctx context.Context, recordType, instanceName string) error {
	rm.registryMu.RLock()
	registeredType, exists := rm.registry[recordType]
	rm.registryMu.RUnlock()

	if !exists {
		return fmt.Errorf("record type %s not registered", recordType)
	}

	// Delete the instance index metadata
	indexKey := rm.buildIndexKey(recordType, instanceName)
	var indexErr error
	if rm.useCache {
		indexErr = rm.cacheCtrl.Delete(ctx, indexKey)
	} else {
		indexErr = rm.valueCtrl.Delete(ctx, indexKey)
	}
	if indexErr != nil && !errors.Is(indexErr, ErrKeyNotFound) {
		return fmt.Errorf("failed to delete instance index: %w", indexErr)
	}

	// Delete each field
	for i := 0; i < registeredType.NumField(); i++ {
		field := registeredType.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		key := rm.buildKey(recordType, instanceName, field.Name)

		var err error
		if rm.useCache {
			err = rm.cacheCtrl.Delete(ctx, key)
		} else {
			err = rm.valueCtrl.Delete(ctx, key)
		}

		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return fmt.Errorf("failed to delete field %s: %w", field.Name, err)
		}
	}

	return nil
}

func (rm *recordManagerImpl) ListInstances(ctx context.Context, recordType string, offset, limit int) ([]string, error) {
	rm.registryMu.RLock()
	_, exists := rm.registry[recordType]
	rm.registryMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("record type %s not registered", recordType)
	}

	// FIX: Use instance index for efficient listing
	// Look for metadata keys instead of iterating all fields
	prefix := recordType + ":"

	// We need to get more keys than requested because we'll filter for metadata keys
	// Get keys in batches until we have enough metadata keys
	const batchSize = 100
	instances := make([]string, 0)
	keyOffset := 0

	for len(instances) < offset+limit {
		var keys []string
		var err error

		if rm.useCache {
			keys, err = rm.cacheCtrl.IterateByPrefix(ctx, prefix, keyOffset, batchSize)
		} else {
			keys, err = rm.valueCtrl.IterateByPrefix(ctx, prefix, keyOffset, batchSize)
		}

		if err != nil {
			return nil, err
		}

		// No more keys available
		if len(keys) == 0 {
			break
		}

		// Extract metadata keys
		for _, key := range keys {
			if strings.HasSuffix(key, ":__meta__") {
				trimmed := strings.TrimPrefix(key, prefix)
				instanceName := strings.TrimSuffix(trimmed, ":__meta__")
				instances = append(instances, instanceName)
			}
		}

		keyOffset += len(keys)

		// If we got less than batchSize, we've reached the end
		if len(keys) < batchSize {
			break
		}
	}

	// Apply offset and limit to the filtered instances
	start := offset
	if start > len(instances) {
		start = len(instances)
	}
	end := start + limit
	if end > len(instances) {
		end = len(instances)
	}

	return instances[start:end], nil
}

func (rm *recordManagerImpl) UpgradeRecord(ctx context.Context, recordType string, upgrader interface{}) error {
	// Validate upgrader function
	upgraderType := reflect.TypeOf(upgrader)
	if upgraderType.Kind() != reflect.Func {
		return errors.New("upgrader must be a function")
	}

	if upgraderType.NumIn() != 1 || upgraderType.NumOut() != 2 {
		return errors.New("upgrader must have signature func(OldType) (NewType, error)")
	}

	if upgraderType.Out(1) != reflect.TypeOf((*error)(nil)).Elem() {
		return errors.New("upgrader must return error as second value")
	}

	upgraderValue := reflect.ValueOf(upgrader)

	// Implement proper pagination to handle large datasets
	const batchSize = 100
	const maxRollbackBatch = 10 // Limit rollback to prevent memory issues
	offset := 0
	totalUpgraded := 0
	totalFailed := 0
	totalSkipped := 0

	// Track recent successful upgrades for limited rollback
	type upgradeRecord struct {
		instanceName string
		oldData      interface{}
		oldChecksum  string
		newChecksum  string
	}
	recentUpgrades := make([]upgradeRecord, 0, maxRollbackBatch)

	for {
		// Get batch of instances
		instances, err := rm.ListInstances(ctx, recordType, offset, batchSize)
		if err != nil {
			return fmt.Errorf("failed to list instances at offset %d: %w", offset, err)
		}

		// If no more instances, we're done
		if len(instances) == 0 {
			break
		}

		// Process this batch
		for _, instanceName := range instances {
			// Check if this instance was already upgraded (for resumability)
			status, _, err := rm.getUpgradeStatus(ctx, recordType, instanceName)
			if err != nil {
				rm.logger.Error("Failed to get upgrade status", "instance", instanceName, "error", err)
				totalFailed++
				continue
			}

			if status == upgradeStatusCompleted {
				rm.logger.Debug("Skipping already upgraded instance", "instance", instanceName)
				totalSkipped++
				continue
			}

			// Get the current record
			oldRecord, err := rm.GetRecord(ctx, recordType, instanceName)
			if err != nil {
				rm.logger.Error("Failed to get record for upgrade", "type", recordType, "instance", instanceName, "error", err)
				totalFailed++
				continue
			}

			// Get current checksum
			metaKey := rm.buildIndexKey(recordType, instanceName)
			var oldChecksum string
			if rm.useCache {
				oldChecksum, _ = rm.cacheCtrl.Get(ctx, metaKey)
			} else {
				oldChecksum, _ = rm.valueCtrl.Get(ctx, metaKey)
			}

			// Mark as in progress
			if err := rm.setUpgradeStatus(ctx, recordType, instanceName, upgradeStatusInProgress, oldChecksum); err != nil {
				rm.logger.Warn("Failed to set upgrade status", "instance", instanceName, "error", err)
			}

			// Call the upgrader
			results := upgraderValue.Call([]reflect.Value{reflect.ValueOf(oldRecord)})

			if !results[1].IsNil() {
				err := results[1].Interface().(error)
				rm.logger.Error("Upgrade failed", "type", recordType, "instance", instanceName, "error", err)

				// Mark as failed
				if statusErr := rm.setUpgradeStatus(ctx, recordType, instanceName, upgradeStatusFailed, oldChecksum); statusErr != nil {
					rm.logger.Warn("Failed to mark upgrade as failed", "instance", instanceName, "error", statusErr)
				}

				totalFailed++

				// Check if we should stop and rollback recent changes
				if totalFailed > 5 && totalFailed > totalUpgraded/2 && len(recentUpgrades) > 0 {
					rm.logger.Error("High failure rate detected, rolling back recent upgrades",
						"failed", totalFailed,
						"upgraded", totalUpgraded,
						"toRollback", len(recentUpgrades))

					// Rollback only recent upgrades (not all)
					rollbackCount := 0
					rollbackFailed := 0
					for _, upgrade := range recentUpgrades {
						if err := rm.safeRollbackRecord(ctx, recordType, upgrade.instanceName, upgrade.oldData, upgrade.newChecksum); err != nil {
							rm.logger.Error("Failed to rollback instance",
								"instance", upgrade.instanceName,
								"error", err)
							rollbackFailed++
						} else {
							// Clear upgrade status on successful rollback
							_ = rm.setUpgradeStatus(ctx, recordType, upgrade.instanceName, upgradeStatusPending, "")
							rollbackCount++
						}
					}

					return fmt.Errorf("upgrade aborted due to high failure rate: %d failed, %d upgraded (rolled back %d recent, %d rollback failed)",
						totalFailed, totalUpgraded, rollbackCount, rollbackFailed)
				}
				continue
			}

			newRecord := results[0].Interface()

			// Store new record using atomic operations
			newChecksum, err := rm.computeRecordChecksum(newRecord)
			if err != nil {
				rm.logger.Error("Failed to compute new checksum", "instance", instanceName, "error", err)
				totalFailed++
				continue
			}

			// Use CAS to ensure record hasn't changed during upgrade
			var casErr error
			if rm.useCache {
				casErr = rm.cacheCtrl.CompareAndSwap(ctx, metaKey, oldChecksum, newChecksum)
			} else {
				casErr = rm.valueCtrl.CompareAndSwap(ctx, metaKey, oldChecksum, newChecksum)
			}

			if casErr != nil {
				if errors.Is(casErr, ErrConflict) {
					rm.logger.Warn("Record was modified during upgrade, skipping", "instance", instanceName)
					totalSkipped++
				} else {
					rm.logger.Error("Failed to update checksum", "instance", instanceName, "error", casErr)
					totalFailed++
				}
				continue
			}

			// Now store the new fields
			if err := rm.storeRecordFields(ctx, recordType, instanceName, newRecord); err != nil {
				rm.logger.Error("Failed to store upgraded fields", "instance", instanceName, "error", err)

				// Try to rollback the checksum
				if rm.useCache {
					_ = rm.cacheCtrl.CompareAndSwap(ctx, metaKey, newChecksum, oldChecksum)
				} else {
					_ = rm.valueCtrl.CompareAndSwap(ctx, metaKey, newChecksum, oldChecksum)
				}

				totalFailed++
				continue
			}

			// Mark as completed
			if err := rm.setUpgradeStatus(ctx, recordType, instanceName, upgradeStatusCompleted, newChecksum); err != nil {
				rm.logger.Warn("Failed to mark upgrade as completed", "instance", instanceName, "error", err)
			}

			// Track in recent upgrades (sliding window)
			if len(recentUpgrades) >= maxRollbackBatch {
				// Remove oldest
				recentUpgrades = recentUpgrades[1:]
			}
			recentUpgrades = append(recentUpgrades, upgradeRecord{
				instanceName: instanceName,
				oldData:      oldRecord,
				oldChecksum:  oldChecksum,
				newChecksum:  newChecksum,
			})

			rm.logger.Info("Upgraded record", "type", recordType, "instance", instanceName)
			totalUpgraded++
		}

		// Move to next batch
		offset += len(instances)

		// Log progress
		if totalUpgraded%1000 == 0 && totalUpgraded > 0 {
			rm.logger.Info("Upgrade progress",
				"type", recordType,
				"upgraded", totalUpgraded,
				"failed", totalFailed,
				"skipped", totalSkipped)
		}
	}

	rm.logger.Info("Upgrade completed",
		"type", recordType,
		"upgraded", totalUpgraded,
		"failed", totalFailed,
		"skipped", totalSkipped)

	return nil
}

func (rm *recordManagerImpl) ListRecordTypes(ctx context.Context) ([]string, error) {
	rm.registryMu.RLock()
	defer rm.registryMu.RUnlock()

	types := make([]string, 0, len(rm.registry))
	for recordType := range rm.registry {
		types = append(types, recordType)
	}

	return types, nil
}

func (rm *recordManagerImpl) ListAllInstances(ctx context.Context, offset, limit int) (map[string][]string, error) {
	result := make(map[string][]string)

	// Get all record types
	recordTypes, err := rm.ListRecordTypes(ctx)
	if err != nil {
		return nil, err
	}

	// FIX: Implement proper pagination across record types
	// We need to track global position across all record types
	globalPosition := 0
	itemsCollected := 0

	for _, recordType := range recordTypes {
		if itemsCollected >= limit {
			break
		}

		// For each record type, we need to calculate the local offset
		// First, get the total count for this type to know if we need to skip it entirely
		// We'll do this by checking if there are any instances at position 0
		testInstances, err := rm.ListInstances(ctx, recordType, 0, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to check instances for type %s: %w", recordType, err)
		}

		// If this type has no instances, skip it
		if len(testInstances) == 0 {
			continue
		}

		// Calculate local offset for this record type
		localOffset := 0
		if globalPosition < offset {
			// We need to determine how many items to skip in this record type
			// Get a batch to count
			countBatch, err := rm.ListInstances(ctx, recordType, 0, offset-globalPosition+1)
			if err != nil {
				return nil, fmt.Errorf("failed to count instances for type %s: %w", recordType, err)
			}

			if len(countBatch) <= offset-globalPosition {
				// All instances in this type should be skipped
				globalPosition += len(countBatch)
				continue
			} else {
				// Some instances should be included
				localOffset = offset - globalPosition
			}
		}

		// Calculate how many items we still need
		remainingLimit := limit - itemsCollected

		// Fetch instances with proper offset and limit
		instances, err := rm.ListInstances(ctx, recordType, localOffset, remainingLimit)
		if err != nil {
			return nil, fmt.Errorf("failed to list instances for type %s: %w", recordType, err)
		}

		if len(instances) > 0 {
			result[recordType] = instances
			itemsCollected += len(instances)
			globalPosition += localOffset + len(instances)
		} else {
			globalPosition += localOffset
		}
	}

	return result, nil
}

func (rm *recordManagerImpl) CleanupOldFields(ctx context.Context, recordType, instanceName string) error {
	rm.registryMu.RLock()
	registeredType, exists := rm.registry[recordType]
	rm.registryMu.RUnlock()

	if !exists {
		return fmt.Errorf("record type %s not registered", recordType)
	}

	// Build a set of valid field names from the registered type
	validFields := make(map[string]bool)
	for i := 0; i < registeredType.NumField(); i++ {
		field := registeredType.Field(i)
		if field.IsExported() {
			validFields[field.Name] = true
		}
	}

	// Always keep the metadata key
	validFields["__meta__"] = true

	// Get all keys for this instance
	prefix := rm.buildKey(recordType, instanceName, "")

	var keys []string
	var err error
	if rm.useCache {
		keys, err = rm.cacheCtrl.IterateByPrefix(ctx, prefix, 0, 1000)
	} else {
		keys, err = rm.valueCtrl.IterateByPrefix(ctx, prefix, 0, 1000)
	}

	if err != nil {
		return fmt.Errorf("failed to list keys for cleanup: %w", err)
	}

	// Check each key and delete if the field is not valid
	deletedCount := 0
	for _, key := range keys {
		// Extract field name from key
		parts := strings.Split(key, ":")
		if len(parts) < 3 {
			continue
		}

		fieldName := parts[len(parts)-1]

		// If field is not in valid set, delete it
		if !validFields[fieldName] {
			rm.logger.Info("Cleaning up orphaned field", "type", recordType, "instance", instanceName, "field", fieldName)

			fullKey := rm.buildKey(recordType, instanceName, fieldName)
			var deleteErr error
			if rm.useCache {
				deleteErr = rm.cacheCtrl.Delete(ctx, fullKey)
			} else {
				deleteErr = rm.valueCtrl.Delete(ctx, fullKey)
			}

			if deleteErr != nil && !errors.Is(deleteErr, ErrKeyNotFound) {
				rm.logger.Error("Failed to delete orphaned field", "field", fieldName, "error", deleteErr)
			} else {
				deletedCount++
			}
		}
	}

	rm.logger.Info("Cleanup completed", "type", recordType, "instance", instanceName, "deletedFields", deletedCount)
	return nil
}

func (rm *recordManagerImpl) setFieldValue(fieldValue reflect.Value, fieldType reflect.Type, valueStr string) error {
	switch fieldType.Kind() {
	case reflect.String:
		fieldValue.SetString(valueStr)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var v int64
		if _, err := fmt.Sscanf(valueStr, "%d", &v); err != nil {
			// Try parsing as float and truncate (for type changes)
			var f float64
			if _, err2 := fmt.Sscanf(valueStr, "%f", &f); err2 == nil {
				v = int64(f)
				rm.logger.Warn("Converted float to int during field type change", "value", valueStr, "result", v)
			} else {
				return fmt.Errorf("cannot parse %q as int: %w", valueStr, err)
			}
		}
		fieldValue.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var v uint64
		if _, err := fmt.Sscanf(valueStr, "%d", &v); err != nil {
			// Try parsing as float and truncate (for type changes)
			var f float64
			if _, err2 := fmt.Sscanf(valueStr, "%f", &f); err2 == nil && f >= 0 {
				v = uint64(f)
				rm.logger.Warn("Converted float to uint during field type change", "value", valueStr, "result", v)
			} else {
				return fmt.Errorf("cannot parse %q as uint: %w", valueStr, err)
			}
		}
		fieldValue.SetUint(v)
	case reflect.Float32, reflect.Float64:
		var v float64
		if _, err := fmt.Sscanf(valueStr, "%f", &v); err != nil {
			// Try parsing as int (for type changes)
			var i int64
			if _, err2 := fmt.Sscanf(valueStr, "%d", &i); err2 == nil {
				v = float64(i)
				rm.logger.Warn("Converted int to float during field type change", "value", valueStr, "result", v)
			} else {
				return fmt.Errorf("cannot parse %q as float: %w", valueStr, err)
			}
		}
		fieldValue.SetFloat(v)
	case reflect.Bool:
		switch valueStr {
		case "true", "1", "yes", "on":
			fieldValue.SetBool(true)
		case "false", "0", "no", "off", "":
			fieldValue.SetBool(false)
		default:
			return fmt.Errorf("invalid boolean value: %s", valueStr)
		}
	case reflect.Struct:
		// Special handling for time.Time
		if fieldType == reflect.TypeOf(time.Time{}) {
			t, err := time.Parse(time.RFC3339, valueStr)
			if err != nil {
				// Try other common formats
				for _, format := range []string{
					time.RFC3339Nano,
					"2006-01-02T15:04:05",
					"2006-01-02 15:04:05",
					"2006-01-02",
				} {
					if parsed, err2 := time.Parse(format, valueStr); err2 == nil {
						t = parsed
						rm.logger.Warn("Parsed time with alternative format", "value", valueStr, "format", format)
						err = nil
						break
					}
				}
				if err != nil {
					return fmt.Errorf("cannot parse time %q: %w", valueStr, err)
				}
			}
			fieldValue.Set(reflect.ValueOf(t))
		} else {
			// For other structs, use JSON decoding
			if err := json.Unmarshal([]byte(valueStr), fieldValue.Addr().Interface()); err != nil {
				return fmt.Errorf("cannot unmarshal JSON: %w", err)
			}
		}
	default:
		// For complex types, use JSON decoding
		if err := json.Unmarshal([]byte(valueStr), fieldValue.Addr().Interface()); err != nil {
			// If JSON fails, try setting as string if the target is a string-like type
			if fieldType.Kind() == reflect.Interface {
				fieldValue.Set(reflect.ValueOf(valueStr))
				rm.logger.Warn("Set interface{} field as string", "value", valueStr)
			} else {
				return fmt.Errorf("cannot unmarshal value: %w", err)
			}
		}
	}

	return nil
}

// upgradeStatus represents the status of a record upgrade
type upgradeStatus string

const (
	upgradeStatusPending    upgradeStatus = "pending"
	upgradeStatusInProgress upgradeStatus = "in_progress"
	upgradeStatusCompleted  upgradeStatus = "completed"
	upgradeStatusFailed     upgradeStatus = "failed"
)

// getUpgradeStatus retrieves the upgrade status for a specific instance
func (rm *recordManagerImpl) getUpgradeStatus(ctx context.Context, recordType, instanceName string) (upgradeStatus, string, error) {
	statusKey := fmt.Sprintf("%s:%s:__upgrade_status__", recordType, instanceName)

	var statusStr string
	var err error
	if rm.useCache {
		statusStr, err = rm.cacheCtrl.Get(ctx, statusKey)
	} else {
		statusStr, err = rm.valueCtrl.Get(ctx, statusKey)
	}

	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return upgradeStatusPending, "", nil
		}
		return "", "", err
	}

	// Status format: "status:checksum"
	parts := strings.SplitN(statusStr, ":", 2)
	if len(parts) != 2 {
		return upgradeStatus(statusStr), "", nil
	}

	return upgradeStatus(parts[0]), parts[1], nil
}

// setUpgradeStatus sets the upgrade status for a specific instance
func (rm *recordManagerImpl) setUpgradeStatus(ctx context.Context, recordType, instanceName string, status upgradeStatus, checksum string) error {
	statusKey := fmt.Sprintf("%s:%s:__upgrade_status__", recordType, instanceName)
	statusValue := fmt.Sprintf("%s:%s", status, checksum)

	if rm.useCache {
		return rm.cacheCtrl.Set(ctx, statusKey, statusValue)
	}
	return rm.valueCtrl.Set(ctx, statusKey, statusValue)
}

// safeRollbackRecord attempts to rollback a record only if it hasn't been modified since upgrade
func (rm *recordManagerImpl) safeRollbackRecord(ctx context.Context, recordType, instanceName string, oldData interface{}, expectedChecksum string) error {
	// First check if the record still has the checksum we expect
	metaKey := rm.buildIndexKey(recordType, instanceName)

	var currentChecksum string
	var err error
	if rm.useCache {
		currentChecksum, err = rm.cacheCtrl.Get(ctx, metaKey)
	} else {
		currentChecksum, err = rm.valueCtrl.Get(ctx, metaKey)
	}

	if err != nil {
		return fmt.Errorf("failed to get current checksum for rollback: %w", err)
	}

	// If checksum doesn't match, record was modified - skip rollback
	if currentChecksum != expectedChecksum {
		rm.logger.Warn("Skipping rollback - record was modified after upgrade",
			"type", recordType,
			"instance", instanceName,
			"expected", expectedChecksum,
			"current", currentChecksum)
		return fmt.Errorf("record was modified after upgrade, cannot rollback")
	}

	// Compute the checksum of the old data
	oldChecksum, err := rm.computeRecordChecksum(oldData)
	if err != nil {
		return fmt.Errorf("failed to compute old data checksum: %w", err)
	}

	// Use atomic CAS to rollback
	var casErr error
	if rm.useCache {
		casErr = rm.cacheCtrl.CompareAndSwap(ctx, metaKey, expectedChecksum, oldChecksum)
	} else {
		casErr = rm.valueCtrl.CompareAndSwap(ctx, metaKey, expectedChecksum, oldChecksum)
	}

	if casErr != nil {
		if errors.Is(casErr, ErrConflict) {
			return fmt.Errorf("record was modified during rollback attempt")
		}
		return fmt.Errorf("failed to rollback checksum: %w", casErr)
	}

	// Now restore the old fields
	if err := rm.storeRecordFields(ctx, recordType, instanceName, oldData); err != nil {
		// Try to restore the new checksum if field restore fails
		if rm.useCache {
			_ = rm.cacheCtrl.CompareAndSwap(ctx, metaKey, oldChecksum, expectedChecksum)
		} else {
			_ = rm.valueCtrl.CompareAndSwap(ctx, metaKey, oldChecksum, expectedChecksum)
		}
		return fmt.Errorf("failed to restore old fields: %w", err)
	}

	return nil
}
