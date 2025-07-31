package ferry

import (
	"context"
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

	// FIX: Prevent colons in record type to avoid key parsing issues
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

	// Store each field separately
	return rm.storeRecord(ctx, recordType, instanceName, data)
}

func (rm *recordManagerImpl) storeRecord(ctx context.Context, recordType, instanceName string, data interface{}) error {
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	typ := val.Type()

	// FIX: Store instance index metadata for efficient listing
	// Store a metadata key to mark this instance exists
	indexKey := rm.buildIndexKey(recordType, instanceName)
	var indexErr error
	if rm.useCache {
		indexErr = rm.cacheCtrl.Set(ctx, indexKey, "1")
	} else {
		indexErr = rm.valueCtrl.Set(ctx, indexKey, "1")
	}
	if indexErr != nil {
		return fmt.Errorf("failed to store instance index: %w", indexErr)
	}

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

	// First, backup the existing record for rollback
	backup, err := rm.GetRecord(ctx, recordType, instanceName)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		// If we can't read the existing record (and it's not because it doesn't exist), bail out
		return fmt.Errorf("failed to backup existing record: %w", err)
	}

	// Attempt to store the new record
	if err := rm.storeRecord(ctx, recordType, instanceName, data); err != nil {
		// If store failed and we have a backup, attempt to restore
		if backup != nil {
			rm.logger.Warn("SetRecord failed, attempting rollback", "type", recordType, "instance", instanceName, "error", err)

			// Best effort rollback - we can't do much if this fails too
			if rollbackErr := rm.storeRecord(ctx, recordType, instanceName, backup); rollbackErr != nil {
				rm.logger.Error("Rollback failed", "type", recordType, "instance", instanceName, "error", rollbackErr)
				return fmt.Errorf("update failed and rollback failed: update error: %w, rollback error: %v", err, rollbackErr)
			}

			rm.logger.Info("Successfully rolled back to previous state", "type", recordType, "instance", instanceName)
		}
		return err
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

	// FIX: Implement proper pagination to handle large datasets
	const batchSize = 100
	offset := 0
	totalUpgraded := 0
	totalFailed := 0

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
			// Get the current record
			oldRecord, err := rm.GetRecord(ctx, recordType, instanceName)
			if err != nil {
				rm.logger.Error("Failed to get record for upgrade", "type", recordType, "instance", instanceName, "error", err)
				totalFailed++
				continue
			}

			// Call the upgrader
			results := upgraderValue.Call([]reflect.Value{reflect.ValueOf(oldRecord)})

			if !results[1].IsNil() {
				err := results[1].Interface().(error)
				rm.logger.Error("Upgrade failed", "type", recordType, "instance", instanceName, "error", err)
				totalFailed++
				continue
			}

			newRecord := results[0].Interface()

			// CRITICAL FIX: Store new record BEFORE deleting old record to prevent data loss
			// First, store the new record
			if err := rm.storeRecord(ctx, recordType, instanceName, newRecord); err != nil {
				rm.logger.Error("Failed to store upgraded record", "type", recordType, "instance", instanceName, "error", err)
				totalFailed++
				continue
			}

			// Only delete old record if new record was successfully stored
			// Note: This may leave some old fields if the new type has fewer fields,
			// but this is safer than losing data
			rm.logger.Info("Upgraded record", "type", recordType, "instance", instanceName)
			totalUpgraded++
		}

		// Move to next batch
		offset += len(instances)

		// If we got less than batchSize, we've reached the end
		if len(instances) < batchSize {
			break
		}
	}

	rm.logger.Info("Upgrade completed", "type", recordType, "upgraded", totalUpgraded, "failed", totalFailed)

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

func (rm *recordManagerImpl) setFieldValue(fieldValue reflect.Value, fieldType reflect.Type, valueStr string) error {
	switch fieldType.Kind() {
	case reflect.String:
		fieldValue.SetString(valueStr)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var v int64
		if _, err := fmt.Sscanf(valueStr, "%d", &v); err != nil {
			return err
		}
		fieldValue.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var v uint64
		if _, err := fmt.Sscanf(valueStr, "%d", &v); err != nil {
			return err
		}
		fieldValue.SetUint(v)
	case reflect.Float32, reflect.Float64:
		var v float64
		if _, err := fmt.Sscanf(valueStr, "%f", &v); err != nil {
			return err
		}
		fieldValue.SetFloat(v)
	case reflect.Bool:
		switch valueStr {
		case "true", "1":
			fieldValue.SetBool(true)
		case "false", "0", "":
			fieldValue.SetBool(false)
		default:
			return fmt.Errorf("invalid boolean value: %s", valueStr)
		}
	case reflect.Struct:
		// Special handling for time.Time
		if fieldType == reflect.TypeOf(time.Time{}) {
			t, err := time.Parse(time.RFC3339, valueStr)
			if err != nil {
				return err
			}
			fieldValue.Set(reflect.ValueOf(t))
		} else {
			// For other structs, use JSON decoding
			if err := json.Unmarshal([]byte(valueStr), fieldValue.Addr().Interface()); err != nil {
				return err
			}
		}
	default:
		// For complex types, use JSON decoding
		if err := json.Unmarshal([]byte(valueStr), fieldValue.Addr().Interface()); err != nil {
			return err
		}
	}

	return nil
}
