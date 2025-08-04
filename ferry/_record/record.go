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

type RecordManager interface {
	Register(recordType string, example interface{}) error
	NewInstance(ctx context.Context, recordType, instanceName string, data interface{}) error
	GetRecord(ctx context.Context, recordType, instanceName string) (interface{}, error)
	GetRecordField(ctx context.Context, recordType, instanceName, fieldName string) (interface{}, error)
	SetRecord(ctx context.Context, recordType, instanceName string, data interface{}) error
	SetRecordIfMatch(ctx context.Context, recordType, instanceName string, expected, data interface{}) error
	SetRecordField(ctx context.Context, recordType, instanceName, fieldName string, value interface{}) error
	DeleteRecord(ctx context.Context, recordType, instanceName string) error
	SyncRecord(ctx context.Context, recordType, instanceName string) (interface{}, error)
	SyncRecordField(ctx context.Context, recordType, instanceName, fieldName string) (interface{}, error)
	UpgradeRecord(ctx context.Context, recordType string, upgrader interface{}) error
	ListInstances(ctx context.Context, recordType string, offset, limit int) ([]string, error)
	ListRecordTypes(ctx context.Context) ([]string, error)
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

	// Upgrade configuration
	upgradeMaxFailures      int
	upgradeFailureRateLimit float64
	upgradeMaxRollback      int
}

type RecordManagerOption func(*recordManagerImpl)

func WithCache() RecordManagerOption {
	return func(rm *recordManagerImpl) {
		rm.useCache = true
	}
}

func WithProduction() RecordManagerOption {
	return func(rm *recordManagerImpl) {
		rm.inProd = true
	}
}

func WithUpgradeThresholds(maxFailures int, failureRateLimit float64, maxRollback int) RecordManagerOption {
	return func(rm *recordManagerImpl) {
		rm.upgradeMaxFailures = maxFailures
		rm.upgradeFailureRateLimit = failureRateLimit
		rm.upgradeMaxRollback = maxRollback
	}
}

func NewRecordManager(ferry *Ferry, logger *slog.Logger, opts ...RecordManagerOption) RecordManager {
	rm := &recordManagerImpl{
		ferry:                   ferry,
		logger:                  logger.WithGroup("record_manager"),
		registry:                make(map[string]reflect.Type),
		inProd:                  false,
		useCache:                false,
		upgradeMaxFailures:      5,
		upgradeFailureRateLimit: 0.5,
		upgradeMaxRollback:      10,
	}

	for _, opt := range opts {
		opt(rm)
	}

	rm.valueCtrl = GetValueController(ferry, "")
	rm.cacheCtrl = GetCacheController(ferry, "")

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

			if strings.Contains(field.Name, ":") {
				return fmt.Errorf("field name %q cannot contain colons (:)", field.Name)
			}

			if strings.HasPrefix(field.Name, "__") && strings.HasSuffix(field.Name, "__") {
				return fmt.Errorf("field name %q uses reserved pattern __name__", field.Name)
			}
		}
	}

	rm.registryMu.Lock()
	if _, exists := rm.registry[recordType]; exists {
		rm.registryMu.Unlock()
		return fmt.Errorf("record type %s is already registered", recordType)
	}
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

func (rm *recordManagerImpl) buildIndexKey(recordType, instanceName string) string {
	return fmt.Sprintf("%s:%s:__meta__", recordType, instanceName)
}

func (rm *recordManagerImpl) buildInstanceIndexKey(recordType, instanceName string) string {
	return fmt.Sprintf("__instances__:%s:%s", recordType, instanceName)
}

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

func (rm *recordManagerImpl) removeFromInstanceIndex(ctx context.Context, recordType, instanceName string) error {
	indexKey := rm.buildInstanceIndexKey(recordType, instanceName)

	if rm.useCache {
		return rm.cacheCtrl.Delete(ctx, indexKey)
	}
	return rm.valueCtrl.Delete(ctx, indexKey)
}

func (rm *recordManagerImpl) NewInstance(ctx context.Context, recordType, instanceName string, data interface{}) error {
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

	dataType := reflect.TypeOf(data)
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
	}

	if dataType != registeredType {
		return fmt.Errorf("data type %s does not match registered type %s", dataType.Name(), registeredType.Name())
	}

	checksum, err := rm.computeRecordChecksum(data)
	if err != nil {
		return fmt.Errorf("failed to compute record checksum: %w", err)
	}

	indexKey := rm.buildIndexKey(recordType, instanceName)
	var setNXErr error

	if rm.useCache {
		existing, getErr := rm.cacheCtrl.Get(ctx, indexKey)
		if getErr == nil && existing != "" {
			return fmt.Errorf("instance %s already exists", instanceName)
		}
		setNXErr = rm.cacheCtrl.Set(ctx, indexKey, checksum)
	} else {
		existing, getErr := rm.valueCtrl.Get(ctx, indexKey)
		if getErr == nil && existing != "" {
			return fmt.Errorf("instance %s already exists", instanceName)
		}
		setNXErr = rm.valueCtrl.Set(ctx, indexKey, checksum)
	}

	if setNXErr != nil {
		return fmt.Errorf("failed to create instance: %w", setNXErr)
	}

	if err := rm.storeRecordFields(ctx, recordType, instanceName, data); err != nil {
		if rm.useCache {
			_ = rm.cacheCtrl.Delete(ctx, indexKey)
		} else {
			_ = rm.valueCtrl.Delete(ctx, indexKey)
		}
		return fmt.Errorf("failed to store record fields: %w", err)
	}

	if err := rm.addToInstanceIndex(ctx, recordType, instanceName); err != nil {
		rm.logger.Warn("Failed to add to instance index",
			"type", recordType,
			"instance", instanceName,
			"error", err)
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

	// Store the fields first, before updating checksum
	if err := rm.storeRecordFields(ctx, recordType, instanceName, data); err != nil {
		// If we have a backup, try to restore the fields
		if hasBackup {
			rm.logger.Warn("SetRecord failed, attempting to restore fields", "type", recordType, "instance", instanceName, "error", err)
			if restoreErr := rm.storeRecordFields(ctx, recordType, instanceName, backup); restoreErr != nil {
				rm.logger.Error("Failed to restore fields during rollback", "type", recordType, "instance", instanceName, "error", restoreErr)
				return fmt.Errorf("update failed and field restoration failed: update error: %w, restore error: %v", err, restoreErr)
			}
		}
		return fmt.Errorf("failed to store record fields: %w", err)
	}

	// Now update the checksum after fields are successfully stored
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
			// Record was modified concurrently after we stored fields
			// Try to restore the old fields if we have a backup
			if hasBackup {
				rm.logger.Warn("Record was modified concurrently, attempting to restore fields", "type", recordType, "instance", instanceName)
				if restoreErr := rm.storeRecordFields(ctx, recordType, instanceName, backup); restoreErr != nil {
					rm.logger.Error("Failed to restore fields after concurrent modification", "type", recordType, "instance", instanceName, "error", restoreErr)
				}
			}
			return fmt.Errorf("record was modified concurrently, use SetRecordIfMatch for safe updates")
		}
	}

	if checksumErr != nil {
		// Checksum update failed, but fields are already written
		// Try to restore old fields if we have a backup
		if hasBackup {
			rm.logger.Warn("Checksum update failed, attempting to restore fields", "type", recordType, "instance", instanceName, "error", checksumErr)
			if restoreErr := rm.storeRecordFields(ctx, recordType, instanceName, backup); restoreErr != nil {
				rm.logger.Error("Failed to restore fields after checksum failure", "type", recordType, "instance", instanceName, "error", restoreErr)
				return fmt.Errorf("checksum update failed and field restoration failed: checksum error: %w, restore error: %v", checksumErr, restoreErr)
			}
		}
		return fmt.Errorf("failed to update checksum: %w", checksumErr)
	}

	// Ensure instance is in the index (important for records created via SetRecord)
	if err := rm.addToInstanceIndex(ctx, recordType, instanceName); err != nil {
		rm.logger.Warn("Failed to add to instance index",
			"type", recordType,
			"instance", instanceName,
			"error", err)
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

	dataType := reflect.TypeOf(data)
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
	}

	if dataType != registeredType {
		return fmt.Errorf("data type %s does not match registered type %s", dataType.Name(), registeredType.Name())
	}

	expectedChecksum, err := rm.computeRecordChecksum(expected)
	if err != nil {
		return fmt.Errorf("failed to compute expected checksum: %w", err)
	}

	newChecksum, err := rm.computeRecordChecksum(data)
	if err != nil {
		return fmt.Errorf("failed to compute new checksum: %w", err)
	}

	metaKey := rm.buildIndexKey(recordType, instanceName)

	var casErr error
	if rm.useCache {
		casErr = rm.cacheCtrl.CompareAndSwap(ctx, metaKey, expectedChecksum, newChecksum)
	} else {
		casErr = rm.valueCtrl.CompareAndSwap(ctx, metaKey, expectedChecksum, newChecksum)
	}

	if casErr != nil {
		if errors.Is(casErr, ErrConflict) {
			rm.logger.Warn("SetRecordIfMatch failed: record was modified", "type", recordType, "instance", instanceName)
			return ErrCASFailed
		}
		return fmt.Errorf("failed to perform atomic CAS: %w", casErr)
	}

	if err := rm.storeRecordFields(ctx, recordType, instanceName, data); err != nil {
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

func (rm *recordManagerImpl) computeRecordChecksum(data interface{}) (string, error) {
	// Use a stable serialization approach by converting to a map first
	// This ensures consistent ordering of fields
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal record for checksum: %w", err)
	}

	// Parse into a map to ensure stable ordering
	var m map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &m); err != nil {
		return "", fmt.Errorf("failed to unmarshal for stable ordering: %w", err)
	}

	// Marshal again with sorted keys (Go's json.Marshal sorts map keys)
	stableBytes, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("failed to marshal with stable ordering: %w", err)
	}

	hash := sha256.Sum256(stableBytes)
	return hex.EncodeToString(hash[:]), nil
}

func (rm *recordManagerImpl) storeRecordFields(ctx context.Context, recordType, instanceName string, data interface{}) error {
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		fieldValue := val.Field(i)

		if !field.IsExported() {
			continue
		}

		key := rm.buildKey(recordType, instanceName, field.Name)

		var valueStr string
		switch fieldValue.Kind() {
		case reflect.String:
			valueStr = fieldValue.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			valueStr = fmt.Sprintf("%d", fieldValue.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			valueStr = fmt.Sprintf("%d", fieldValue.Uint())
		case reflect.Float32, reflect.Float64:
			// Use consistent precision for floats
			valueStr = fmt.Sprintf("%.15g", fieldValue.Float())
		case reflect.Bool:
			valueStr = fmt.Sprintf("%t", fieldValue.Bool())
		case reflect.Struct:
			if fieldValue.Type() == reflect.TypeOf(time.Time{}) {
				t := fieldValue.Interface().(time.Time)
				// Always use RFC3339Nano for maximum precision
				valueStr = t.Format(time.RFC3339Nano)
			} else {
				// For all other structs, use JSON
				data, err := json.Marshal(fieldValue.Interface())
				if err != nil {
					return fmt.Errorf("failed to marshal struct field %s: %w", field.Name, err)
				}
				valueStr = string(data)
			}
		case reflect.Slice, reflect.Array, reflect.Map, reflect.Interface:
			// Use JSON for complex types
			data, err := json.Marshal(fieldValue.Interface())
			if err != nil {
				return fmt.Errorf("failed to marshal complex field %s of type %s: %w", field.Name, fieldValue.Kind(), err)
			}
			valueStr = string(data)
		default:
			// Log warning for unexpected types
			rm.logger.Warn("Unexpected field type, using JSON marshaling",
				"field", field.Name,
				"type", fieldValue.Kind(),
				"recordType", recordType,
				"instance", instanceName)
			data, err := json.Marshal(fieldValue.Interface())
			if err != nil {
				return fmt.Errorf("failed to marshal field %s: %w", field.Name, err)
			}
			valueStr = string(data)
		}

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

	field, found := registeredType.FieldByName(fieldName)
	if !found {
		return fmt.Errorf("field %s not found in record type %s", fieldName, recordType)
	}

	valueType := reflect.TypeOf(value)
	if !valueType.AssignableTo(field.Type) {
		return fmt.Errorf("value type %s is not assignable to field type %s", valueType, field.Type)
	}

	key := rm.buildKey(recordType, instanceName, fieldName)

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

	if err := rm.removeFromInstanceIndex(ctx, recordType, instanceName); err != nil && !errors.Is(err, ErrKeyNotFound) {
		rm.logger.Warn("Failed to remove from instance index",
			"type", recordType,
			"instance", instanceName,
			"error", err)
	}

	upgradeStatusKey := fmt.Sprintf("%s:%s:__upgrade_status__", recordType, instanceName)
	if rm.useCache {
		if err := rm.cacheCtrl.Delete(ctx, upgradeStatusKey); err != nil && !errors.Is(err, ErrKeyNotFound) {
			rm.logger.Warn("Failed to delete upgrade status",
				"type", recordType,
				"instance", instanceName,
				"error", err)
		}
	} else {
		if err := rm.valueCtrl.Delete(ctx, upgradeStatusKey); err != nil && !errors.Is(err, ErrKeyNotFound) {
			rm.logger.Warn("Failed to delete upgrade status",
				"type", recordType,
				"instance", instanceName,
				"error", err)
		}
	}

	for i := 0; i < registeredType.NumField(); i++ {
		field := registeredType.Field(i)

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

	indexPrefix := fmt.Sprintf("__instances__:%s:", recordType)

	var keys []string
	var err error

	if rm.useCache {
		keys, err = rm.cacheCtrl.IterateByPrefix(ctx, indexPrefix, offset, limit)
	} else {
		keys, err = rm.valueCtrl.IterateByPrefix(ctx, indexPrefix, offset, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to iterate instance index: %w", err)
	}

	instances := make([]string, 0, len(keys))
	for _, key := range keys {
		instanceName := strings.TrimPrefix(key, indexPrefix)
		instances = append(instances, instanceName)
	}

	if len(instances) < limit && len(keys) < limit {
		rm.logger.Info("Instance index incomplete, falling back to metadata scan",
			"type", recordType,
			"indexCount", len(instances),
			"requested", limit)

		additionalInstances, err := rm.listInstancesFallback(ctx, recordType, offset+len(instances), limit-len(instances))
		if err != nil {
			return instances, nil
		}

		instances = append(instances, additionalInstances...)
	}

	return instances, nil
}

func (rm *recordManagerImpl) listInstancesFallback(ctx context.Context, recordType string, offset, limit int) ([]string, error) {
	prefix := recordType + ":"

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

		if len(keys) == 0 {
			break
		}

		for _, key := range keys {
			if strings.HasSuffix(key, ":__meta__") {
				trimmed := strings.TrimPrefix(key, prefix)
				instanceName := strings.TrimSuffix(trimmed, ":__meta__")
				instances = append(instances, instanceName)
			}
		}

		keyOffset += len(keys)

		if len(keys) < batchSize {
			break
		}
	}

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

	const batchSize = 100
	offset := 0
	totalUpgraded := 0
	totalFailed := 0
	totalSkipped := 0

	type upgradeRecord struct {
		instanceName string
		oldData      interface{}
		oldChecksum  string
		newChecksum  string
	}
	recentUpgrades := make([]upgradeRecord, 0, rm.upgradeMaxRollback)

	for {
		instances, err := rm.ListInstances(ctx, recordType, offset, batchSize)
		if err != nil {
			return fmt.Errorf("failed to list instances at offset %d: %w", offset, err)
		}

		if len(instances) == 0 {
			break
		}

		for _, instanceName := range instances {
			status, _, err := rm.getUpgradeStatus(ctx, recordType, instanceName)
			if err != nil {
				rm.logger.Debug("Failed to get upgrade status, assuming not upgraded", "instance", instanceName, "error", err)
				status = upgradeStatusPending
			}

			if status == upgradeStatusCompleted {
				rm.logger.Debug("Skipping already upgraded instance", "instance", instanceName)
				totalSkipped++
				continue
			}

			oldRecord, err := rm.GetRecord(ctx, recordType, instanceName)
			if err != nil {
				rm.logger.Error("Failed to get record for upgrade", "type", recordType, "instance", instanceName, "error", err)
				totalFailed++
				continue
			}

			metaKey := rm.buildIndexKey(recordType, instanceName)
			var oldChecksum string
			if rm.useCache {
				oldChecksum, _ = rm.cacheCtrl.Get(ctx, metaKey)
			} else {
				oldChecksum, _ = rm.valueCtrl.Get(ctx, metaKey)
			}

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
				failureRate := float64(totalFailed) / float64(totalFailed+totalUpgraded)
				if totalFailed > rm.upgradeMaxFailures && failureRate > rm.upgradeFailureRateLimit && len(recentUpgrades) > 0 {
					rm.logger.Error("High failure rate detected, rolling back recent upgrades",
						"failed", totalFailed,
						"upgraded", totalUpgraded,
						"failureRate", failureRate,
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

			if err := rm.setUpgradeStatus(ctx, recordType, instanceName, upgradeStatusCompleted, newChecksum); err != nil {
				rm.logger.Warn("Failed to mark upgrade as completed", "instance", instanceName, "error", err)
			}

			if err := rm.addToInstanceIndex(ctx, recordType, instanceName); err != nil {
				rm.logger.Warn("Failed to add to instance index during upgrade",
					"type", recordType,
					"instance", instanceName,
					"error", err)
			}

			// Track in recent upgrades (sliding window)
			if len(recentUpgrades) >= rm.upgradeMaxRollback {
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

		offset += len(instances)

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
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		return make(map[string][]string), nil
	}

	result := make(map[string][]string)

	recordTypes, err := rm.ListRecordTypes(ctx)
	if err != nil {
		return nil, err
	}

	// Sort record types for consistent ordering
	sortedTypes := make([]string, len(recordTypes))
	copy(sortedTypes, recordTypes)
	// Sort to ensure consistent ordering across calls
	for i := 0; i < len(sortedTypes); i++ {
		for j := i + 1; j < len(sortedTypes); j++ {
			if sortedTypes[i] > sortedTypes[j] {
				sortedTypes[i], sortedTypes[j] = sortedTypes[j], sortedTypes[i]
			}
		}
	}

	// Track global position and items collected
	itemsSkipped := 0
	itemsCollected := 0

	for _, recordType := range sortedTypes {
		if itemsCollected >= limit {
			break
		}

		// Calculate how many items to skip for this type
		itemsToSkip := 0
		if itemsSkipped < offset {
			itemsToSkip = offset - itemsSkipped
		}

		// Calculate how many items we need from this type
		itemsNeeded := limit - itemsCollected

		// Fetch instances with calculated offset and limit
		// Add a buffer to handle the case where we might skip some
		fetchLimit := itemsToSkip + itemsNeeded
		instances, err := rm.ListInstances(ctx, recordType, 0, fetchLimit)
		if err != nil {
			rm.logger.Warn("Failed to list instances for type, skipping",
				"type", recordType,
				"error", err)
			continue
		}

		// If we got fewer instances than itemsToSkip, update our skip count
		if len(instances) <= itemsToSkip {
			itemsSkipped += len(instances)
			continue
		}

		// Calculate the actual instances to return
		startIdx := itemsToSkip
		endIdx := len(instances)
		if endIdx > startIdx+itemsNeeded {
			endIdx = startIdx + itemsNeeded
		}

		returnInstances := instances[startIdx:endIdx]
		if len(returnInstances) > 0 {
			result[recordType] = returnInstances
			itemsCollected += len(returnInstances)
		}

		itemsSkipped += startIdx
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

	validFields := make(map[string]bool)
	for i := 0; i < registeredType.NumField(); i++ {
		field := registeredType.Field(i)
		if field.IsExported() {
			validFields[field.Name] = true
		}
	}

	validFields["__meta__"] = true

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

	deletedCount := 0
	for _, key := range keys {
		parts := strings.Split(key, ":")
		if len(parts) < 3 {
			continue
		}

		fieldName := parts[len(parts)-1]

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
			return fmt.Errorf("cannot parse %q as int: %w", valueStr, err)
		}
		fieldValue.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var v uint64
		if _, err := fmt.Sscanf(valueStr, "%d", &v); err != nil {
			return fmt.Errorf("cannot parse %q as uint: %w", valueStr, err)
		}
		fieldValue.SetUint(v)
	case reflect.Float32, reflect.Float64:
		var v float64
		if _, err := fmt.Sscanf(valueStr, "%g", &v); err != nil {
			return fmt.Errorf("cannot parse %q as float: %w", valueStr, err)
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
			// Try RFC3339Nano first (our standard format)
			t, err := time.Parse(time.RFC3339Nano, valueStr)
			if err != nil {
				// Fall back to RFC3339 for backward compatibility
				t, err = time.Parse(time.RFC3339, valueStr)
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

type upgradeStatus string

const (
	upgradeStatusPending    upgradeStatus = "pending"
	upgradeStatusInProgress upgradeStatus = "in_progress"
	upgradeStatusCompleted  upgradeStatus = "completed"
	upgradeStatusFailed     upgradeStatus = "failed"
)

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

func (rm *recordManagerImpl) setUpgradeStatus(ctx context.Context, recordType, instanceName string, status upgradeStatus, checksum string) error {
	statusKey := fmt.Sprintf("%s:%s:__upgrade_status__", recordType, instanceName)
	statusValue := fmt.Sprintf("%s:%s", status, checksum)

	if rm.useCache {
		return rm.cacheCtrl.Set(ctx, statusKey, statusValue)
	}
	return rm.valueCtrl.Set(ctx, statusKey, statusValue)
}

func (rm *recordManagerImpl) safeRollbackRecord(ctx context.Context, recordType, instanceName string, oldData interface{}, expectedChecksum string) error {
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

	if currentChecksum != expectedChecksum {
		rm.logger.Warn("Skipping rollback - record was modified after upgrade",
			"type", recordType,
			"instance", instanceName,
			"expected", expectedChecksum,
			"current", currentChecksum)
		return fmt.Errorf("record was modified after upgrade, cannot rollback")
	}

	oldChecksum, err := rm.computeRecordChecksum(oldData)
	if err != nil {
		return fmt.Errorf("failed to compute old data checksum: %w", err)
	}

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

	if err := rm.storeRecordFields(ctx, recordType, instanceName, oldData); err != nil {
		if rm.useCache {
			_ = rm.cacheCtrl.CompareAndSwap(ctx, metaKey, oldChecksum, expectedChecksum)
		} else {
			_ = rm.valueCtrl.CompareAndSwap(ctx, metaKey, oldChecksum, expectedChecksum)
		}
		return fmt.Errorf("failed to restore old fields: %w", err)
	}

	return nil
}

func (rm *recordManagerImpl) RebuildInstanceIndex(ctx context.Context, recordType string) error {
	rm.registryMu.RLock()
	_, exists := rm.registry[recordType]
	rm.registryMu.RUnlock()

	if !exists {
		return fmt.Errorf("record type %s not registered", recordType)
	}

	rm.logger.Info("Starting instance index rebuild", "type", recordType)

	const batchSize = 100
	offset := 0
	totalIndexed := 0
	totalFailed := 0

	for {
		instances, err := rm.listInstancesFallback(ctx, recordType, offset, batchSize)
		if err != nil {
			return fmt.Errorf("failed to list instances for rebuild: %w", err)
		}

		if len(instances) == 0 {
			break
		}

		for _, instanceName := range instances {
			if err := rm.addToInstanceIndex(ctx, recordType, instanceName); err != nil {
				rm.logger.Error("Failed to add instance to index during rebuild",
					"type", recordType,
					"instance", instanceName,
					"error", err)
				totalFailed++
			} else {
				totalIndexed++
			}
		}

		offset += len(instances)

		if totalIndexed%1000 == 0 && totalIndexed > 0 {
			rm.logger.Info("Instance index rebuild progress",
				"type", recordType,
				"indexed", totalIndexed,
				"failed", totalFailed)
		}

		if len(instances) < batchSize {
			break
		}
	}

	rm.logger.Info("Instance index rebuild completed",
		"type", recordType,
		"totalIndexed", totalIndexed,
		"totalFailed", totalFailed)

	return nil
}
