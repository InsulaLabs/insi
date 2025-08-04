package ferry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"sync/atomic"

	"github.com/InsulaLabs/insi/client"
)

// Mock implementations for testing

type mockClient struct {
	data      map[string]string
	cacheData map[string]string
	mu        sync.RWMutex
}

func newMockClient() *mockClient {
	return &mockClient{
		data:      make(map[string]string),
		cacheData: make(map[string]string),
	}
}

func (m *mockClient) Get(key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if val, ok := m.data[key]; ok {
		return val, nil
	}
	return "", client.ErrKeyNotFound
}

func (m *mockClient) Set(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
	return nil
}

func (m *mockClient) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

func (m *mockClient) IterateByPrefix(prefix string, offset, limit int) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for k := range m.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}

	// Sort keys for consistent pagination
	sort.Strings(keys)

	// Apply offset and limit
	start := offset
	if start > len(keys) {
		start = len(keys)
	}
	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}

	return keys[start:end], nil
}

func (m *mockClient) GetCache(key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if val, ok := m.cacheData[key]; ok {
		return val, nil
	}
	return "", client.ErrKeyNotFound
}

func (m *mockClient) SetCache(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cacheData[key] = value
	return nil
}

func (m *mockClient) DeleteCache(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.cacheData, key)
	return nil
}

func (m *mockClient) IterateCacheByPrefix(prefix string, offset, limit int) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for k := range m.cacheData {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}

	// Sort keys for consistent pagination
	sort.Strings(keys)

	// Apply offset and limit
	start := offset
	if start > len(keys) {
		start = len(keys)
	}
	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}

	return keys[start:end], nil
}

// Stub methods for client interface
func (m *mockClient) CompareAndSwap(key, oldValue, newValue string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, exists := m.data[key]
	if !exists && oldValue != "" {
		return client.ErrKeyNotFound
	}
	if exists && current != oldValue {
		return client.ErrConflict
	}

	m.data[key] = newValue
	return nil
}

func (m *mockClient) CompareAndSwapCache(key, oldValue, newValue string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, exists := m.cacheData[key]
	if !exists && oldValue != "" {
		return client.ErrKeyNotFound
	}
	if exists && current != oldValue {
		return client.ErrConflict
	}

	m.cacheData[key] = newValue
	return nil
}

func (m *mockClient) SetCacheNX(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.cacheData[key]; exists {
		return client.ErrConflict
	}

	m.cacheData[key] = value
	return nil
}

func (m *mockClient) Bump(key string, value int) error {
	return errors.New("not implemented")
}

// Test structures

type TestRecord struct {
	ID        string
	Name      string
	Age       int
	IsActive  bool
	CreatedAt time.Time
	UpdatedAt time.Time
	Tags      []string
	Metadata  map[string]string
}

type SimpleRecord struct {
	Name  string
	Value int
}

// Helper function to create test ferry with mock client
func createTestFerry() (*Ferry, *mockClient) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	mockC := newMockClient()

	// Create a real client but replace its underlying connection with our mock
	ferry := &Ferry{
		client: &client.Client{}, // This is a simplified mock setup
		logger: logger,
	}

	// For testing, we'll directly use the mock client in our controllers
	return ferry, mockC
}

// Test functions

func TestRecordManager_Register(t *testing.T) {
	ferry, _ := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:    ferry,
		logger:   logger.WithGroup("test_record_manager"),
		registry: make(map[string]reflect.Type),
	}

	// Test successful registration
	err := rm.Register("testRecord", TestRecord{})
	if err != nil {
		t.Fatalf("Failed to register record type: %v", err)
	}

	// Test registration with pointer
	err = rm.Register("testRecord2", &TestRecord{})
	if err != nil {
		t.Fatalf("Failed to register record type with pointer: %v", err)
	}

	// Test empty record type
	err = rm.Register("", TestRecord{})
	if err == nil {
		t.Fatal("Expected error for empty record type")
	}

	// Test non-struct type
	err = rm.Register("invalid", "not a struct")
	if err == nil {
		t.Fatal("Expected error for non-struct type")
	}

	// Test record type with colon
	err = rm.Register("test:record", TestRecord{})
	if err == nil {
		t.Fatal("Expected error for record type with colon")
	}

	// Test struct with reserved field names
	type BadRecord1 struct {
		Meta string // Will be checked as "__meta__" in validation
		Name string
	}

	// Manually construct a type that would have the reserved name
	// Since Go doesn't allow __meta__ as a field name, we'll test this differently

	// Test struct with field containing colon
	type BadRecord2 struct {
		Name     string
		BadField string // Can't actually have : in field name
	}

	err = rm.Register("okrecord", BadRecord2{})
	if err != nil {
		t.Fatalf("Should allow normal struct: %v", err)
	}

	// Since Go's syntax doesn't allow the bad field names we want to prevent,
	// the validation is more of a safeguard for dynamic/reflection-based usage
}

func TestRecordManager_NewInstance(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Create a custom controller that uses our mock
	valueCtrl := &vcImpl[string]{
		defaultT: "",
		client:   ferry.client,
		logger:   logger,
		scopes:   []string{"dev", "records"},
		prefix:   "dev:records",
	}

	// Override the client methods to use our mock
	valueCtrl.client = (*client.Client)(nil) // Clear it

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: valueCtrl,
		useCache:  false,
	}

	// Inject mock behavior
	rm.valueCtrl = &mockValueController{mock: mockC, prefix: "dev:records"}

	ctx := context.Background()

	// Register record type
	err := rm.Register("testRecord", TestRecord{})
	if err != nil {
		t.Fatalf("Failed to register record type: %v", err)
	}

	// Create instance
	now := time.Now()
	record := TestRecord{
		ID:        "123",
		Name:      "Test User",
		Age:       25,
		IsActive:  true,
		CreatedAt: now,
		UpdatedAt: now,
		Tags:      []string{"tag1", "tag2"},
		Metadata:  map[string]string{"key": "value"},
	}

	err = rm.NewInstance(ctx, "testRecord", "instance1", record)
	if err != nil {
		t.Fatalf("Failed to create new instance: %v", err)
	}

	// Verify data was stored
	expectedKeys := []string{
		"dev:records:testRecord:instance1:__meta__",
		"dev:records:testRecord:instance1:ID",
		"dev:records:testRecord:instance1:Name",
		"dev:records:testRecord:instance1:Age",
		"dev:records:testRecord:instance1:IsActive",
		"dev:records:testRecord:instance1:CreatedAt",
		"dev:records:testRecord:instance1:UpdatedAt",
		"dev:records:testRecord:instance1:Tags",
		"dev:records:testRecord:instance1:Metadata",
	}

	for _, key := range expectedKeys {
		if _, exists := mockC.data[key]; !exists {
			t.Errorf("Expected key %s not found in storage", key)
		}
	}

	// Test with unregistered type
	err = rm.NewInstance(ctx, "unregistered", "instance2", TestRecord{})
	if err == nil {
		t.Fatal("Expected error for unregistered record type")
	}

	// Test with wrong type
	err = rm.NewInstance(ctx, "testRecord", "instance3", SimpleRecord{})
	if err == nil {
		t.Fatal("Expected error for mismatched record type")
	}
}

func TestRecordManager_GetRecord(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register and create a record
	rm.Register("testRecord", TestRecord{})

	now := time.Now().Truncate(time.Second) // Truncate for comparison
	original := TestRecord{
		ID:        "456",
		Name:      "Get Test",
		Age:       30,
		IsActive:  false,
		CreatedAt: now,
		UpdatedAt: now,
	}

	rm.NewInstance(ctx, "testRecord", "getTest", original)

	// Get the record back
	result, err := rm.GetRecord(ctx, "testRecord", "getTest")
	if err != nil {
		t.Fatalf("Failed to get record: %v", err)
	}

	retrieved, ok := result.(TestRecord)
	if !ok {
		t.Fatal("Retrieved record has wrong type")
	}

	// Compare fields
	if retrieved.ID != original.ID {
		t.Errorf("ID mismatch: got %s, want %s", retrieved.ID, original.ID)
	}
	if retrieved.Name != original.Name {
		t.Errorf("Name mismatch: got %s, want %s", retrieved.Name, original.Name)
	}
	if retrieved.Age != original.Age {
		t.Errorf("Age mismatch: got %d, want %d", retrieved.Age, original.Age)
	}
	if retrieved.IsActive != original.IsActive {
		t.Errorf("IsActive mismatch: got %v, want %v", retrieved.IsActive, original.IsActive)
	}

	// Test non-existent record
	_, err = rm.GetRecord(ctx, "testRecord", "nonExistent")
	if err != nil {
		t.Log("Expected behavior: record not found")
	}
}

func TestRecordManager_SetRecordField(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register and create a record
	rm.Register("testRecord", TestRecord{})

	original := TestRecord{
		ID:   "789",
		Name: "Original Name",
		Age:  25,
	}

	rm.NewInstance(ctx, "testRecord", "fieldTest", original)

	// Update a field
	err := rm.SetRecordField(ctx, "testRecord", "fieldTest", "Name", "Updated Name")
	if err != nil {
		t.Fatalf("Failed to set record field: %v", err)
	}

	// Verify the update
	value, err := rm.GetRecordField(ctx, "testRecord", "fieldTest", "Name")
	if err != nil {
		t.Fatalf("Failed to get record field: %v", err)
	}

	if value.(string) != "Updated Name" {
		t.Errorf("Field not updated: got %v, want 'Updated Name'", value)
	}

	// Test updating non-existent field
	err = rm.SetRecordField(ctx, "testRecord", "fieldTest", "NonExistent", "value")
	if err == nil {
		t.Fatal("Expected error for non-existent field")
	}

	// Test type mismatch
	err = rm.SetRecordField(ctx, "testRecord", "fieldTest", "Age", "not a number")
	if err == nil {
		t.Fatal("Expected error for type mismatch")
	}
}

func TestRecordManager_DeleteRecord(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register and create a record
	rm.Register("testRecord", TestRecord{})

	record := TestRecord{
		ID:   "999",
		Name: "To Delete",
		Age:  40,
	}

	rm.NewInstance(ctx, "testRecord", "deleteTest", record)

	// Verify it exists
	_, err := rm.GetRecord(ctx, "testRecord", "deleteTest")
	if err != nil {
		t.Fatalf("Record should exist before deletion: %v", err)
	}

	// Delete the record
	err = rm.DeleteRecord(ctx, "testRecord", "deleteTest")
	if err != nil {
		t.Fatalf("Failed to delete record: %v", err)
	}

	// Verify all fields are deleted
	keys := []string{
		"dev:records:testRecord:deleteTest:__meta__",
		"dev:records:testRecord:deleteTest:ID",
		"dev:records:testRecord:deleteTest:Name",
		"dev:records:testRecord:deleteTest:Age",
		// Also check instance index
		"dev:records:__instances__:testRecord:deleteTest",
	}

	for _, key := range keys {
		if _, exists := mockC.data[key]; exists {
			t.Errorf("Key %s should have been deleted", key)
		}
	}

	// Verify it doesn't appear in listing
	instances, err := rm.ListInstances(ctx, "testRecord", 0, 10)
	if err != nil {
		t.Fatalf("Failed to list instances: %v", err)
	}
	for _, inst := range instances {
		if inst == "deleteTest" {
			t.Error("Deleted instance still appears in listing")
		}
	}
}

func TestRecordManager_ListInstances(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register record type
	rm.Register("testRecord", SimpleRecord{})

	// Create multiple instances
	for i := 0; i < 5; i++ {
		record := SimpleRecord{
			Name:  fmt.Sprintf("Instance %d", i),
			Value: i,
		}
		rm.NewInstance(ctx, "testRecord", fmt.Sprintf("instance%d", i), record)
	}

	// List instances
	instances, err := rm.ListInstances(ctx, "testRecord", 0, 10)
	if err != nil {
		t.Fatalf("Failed to list instances: %v", err)
	}

	if len(instances) != 5 {
		t.Errorf("Expected 5 instances, got %d", len(instances))
	}

	// Test with limit
	instances, err = rm.ListInstances(ctx, "testRecord", 0, 3)
	if err != nil {
		t.Fatalf("Failed to list instances with limit: %v", err)
	}

	if len(instances) > 3 {
		t.Errorf("Expected at most 3 instances, got %d", len(instances))
	}
}

func TestRecordManager_UpgradeRecord(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Define old and new record types
	type OldRecord struct {
		Name string
		Age  int
	}

	type NewRecord struct {
		Name      string
		Age       int
		UpdatedAt time.Time
	}

	// Register old record type
	rm.Register("upgradeable", OldRecord{})

	// Create instances
	for i := 0; i < 3; i++ {
		record := OldRecord{
			Name: fmt.Sprintf("User %d", i),
			Age:  20 + i,
		}
		rm.NewInstance(ctx, "upgradeable", fmt.Sprintf("user%d", i), record)
	}

	// Don't re-register with new type, as we need to read old type first

	// Upgrade function
	upgrader := func(old OldRecord) (NewRecord, error) {
		newRec := NewRecord{
			Name:      old.Name,
			Age:       old.Age,
			UpdatedAt: time.Now(),
		}
		t.Logf("Upgrader creating: Name=%s, Age=%d, UpdatedAt=%v", newRec.Name, newRec.Age, newRec.UpdatedAt)
		return newRec, nil
	}

	// Perform upgrade
	err := rm.UpgradeRecord(ctx, "upgradeable", upgrader)
	if err != nil {
		t.Fatalf("Failed to upgrade records: %v", err)
	}

	// Now register the new type so we can read the upgraded records
	rm.Register("upgradeable", NewRecord{})

	// Verify upgrades
	for i := 0; i < 3; i++ {
		// Debug: check what keys exist for this instance
		instancePrefix := fmt.Sprintf("dev:records:upgradeable:user%d:", i)
		var foundKeys []string
		for k := range mockC.data {
			if strings.HasPrefix(k, instancePrefix) {
				foundKeys = append(foundKeys, k)
			}
		}
		t.Logf("Keys for user%d: %v", i, foundKeys)

		result, err := rm.GetRecord(ctx, "upgradeable", fmt.Sprintf("user%d", i))
		if err != nil {
			t.Fatalf("Failed to get upgraded record: %v", err)
		}

		upgraded, ok := result.(NewRecord)
		if !ok {
			t.Fatal("Upgraded record has wrong type")
		}

		if upgraded.Name != fmt.Sprintf("User %d", i) {
			t.Errorf("Name not preserved during upgrade: got %s", upgraded.Name)
		}

		if upgraded.UpdatedAt.IsZero() {
			t.Error("UpdatedAt field not set during upgrade")
		}
	}
}

func TestRecordManager_CacheBackend(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		cacheCtrl: &mockCacheController{mock: mockC, prefix: "dev:records"},
		useCache:  true,
	}

	ctx := context.Background()

	// Register and create a record using cache
	rm.Register("cacheRecord", SimpleRecord{})

	record := SimpleRecord{
		Name:  "Cached",
		Value: 100,
	}

	rm.NewInstance(ctx, "cacheRecord", "cached1", record)

	// Verify it's in cache, not value store
	if len(mockC.cacheData) == 0 {
		t.Fatal("Expected data in cache")
	}

	if len(mockC.data) > 0 {
		t.Fatal("Did not expect data in value store")
	}

	// Check specific key
	key := "dev:records:cacheRecord:cached1:Name"
	if _, exists := mockC.cacheData[key]; !exists {
		t.Errorf("Expected key %s in cache", key)
	}
}

func TestRecordManager_ProductionMode(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "prod:records"},
		inProd:    true,
	}

	ctx := context.Background()

	// Register and create a record in production mode
	rm.Register("prodRecord", SimpleRecord{})

	record := SimpleRecord{
		Name:  "Production",
		Value: 200,
	}

	rm.NewInstance(ctx, "prodRecord", "prod1", record)

	// Verify keys have prod:records prefix
	key := "prod:records:prodRecord:prod1:__meta__"
	if _, exists := mockC.data[key]; !exists {
		t.Errorf("Expected metadata key %s with prod:records prefix", key)
	}

	key = "prod:records:prodRecord:prod1:Name"
	if _, exists := mockC.data[key]; !exists {
		t.Errorf("Expected key %s with prod:records prefix", key)
	}

	// Verify dev prefix is not used
	devKey := "dev:records:prodRecord:prod1:Name"
	if _, exists := mockC.data[devKey]; exists {
		t.Errorf("Did not expect key %s with dev:records prefix", devKey)
	}
}

func TestRecordManager_RecordsPrefix(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register and create a record
	rm.Register("testRecord", SimpleRecord{})

	record := SimpleRecord{
		Name:  "Test",
		Value: 100,
	}

	rm.NewInstance(ctx, "testRecord", "test1", record)

	// Verify keys have the records prefix
	expectedKey := "dev:records:testRecord:test1:Name"
	if _, exists := mockC.data[expectedKey]; !exists {
		t.Errorf("Expected key %s with records prefix", expectedKey)
	}

	// Verify old format without records prefix is not used
	oldKey := "dev:testRecord:test1:Name"
	if _, exists := mockC.data[oldKey]; exists {
		t.Errorf("Did not expect key %s without records prefix", oldKey)
	}
}

func TestRecordManager_ListRecordTypes(t *testing.T) {
	ferry, _ := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:    ferry,
		logger:   logger.WithGroup("test_record_manager"),
		registry: make(map[string]reflect.Type),
	}

	ctx := context.Background()

	// Initially empty
	types, err := rm.ListRecordTypes(ctx)
	if err != nil {
		t.Fatalf("Failed to list record types: %v", err)
	}
	if len(types) != 0 {
		t.Errorf("Expected 0 record types, got %d", len(types))
	}

	// Register multiple types
	rm.Register("typeA", SimpleRecord{})
	rm.Register("typeB", TestRecord{})
	rm.Register("typeC", SimpleRecord{})

	// List types
	types, err = rm.ListRecordTypes(ctx)
	if err != nil {
		t.Fatalf("Failed to list record types: %v", err)
	}

	if len(types) != 3 {
		t.Errorf("Expected 3 record types, got %d", len(types))
	}

	// Verify all types are present
	typeMap := make(map[string]bool)
	for _, typ := range types {
		typeMap[typ] = true
	}

	expectedTypes := []string{"typeA", "typeB", "typeC"}
	for _, expected := range expectedTypes {
		if !typeMap[expected] {
			t.Errorf("Expected type %s not found", expected)
		}
	}
}

func TestRecordManager_ListAllInstances(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register multiple record types
	rm.Register("users", SimpleRecord{})
	rm.Register("products", SimpleRecord{})

	// Create instances for users
	for i := 0; i < 3; i++ {
		record := SimpleRecord{
			Name:  fmt.Sprintf("User %d", i),
			Value: i,
		}
		rm.NewInstance(ctx, "users", fmt.Sprintf("user%d", i), record)
	}

	// Create instances for products
	for i := 0; i < 2; i++ {
		record := SimpleRecord{
			Name:  fmt.Sprintf("Product %d", i),
			Value: i * 100,
		}
		rm.NewInstance(ctx, "products", fmt.Sprintf("product%d", i), record)
	}

	// List all instances
	allInstances, err := rm.ListAllInstances(ctx, 0, 10)
	if err != nil {
		t.Fatalf("Failed to list all instances: %v", err)
	}

	// Verify we got instances for both types
	if len(allInstances) != 2 {
		t.Errorf("Expected 2 record types, got %d", len(allInstances))
	}

	// Verify users
	if userInstances, ok := allInstances["users"]; ok {
		if len(userInstances) != 3 {
			t.Errorf("Expected 3 user instances, got %d", len(userInstances))
		}
	} else {
		t.Error("Expected users in result")
	}

	// Verify products
	if productInstances, ok := allInstances["products"]; ok {
		if len(productInstances) != 2 {
			t.Errorf("Expected 2 product instances, got %d", len(productInstances))
		}
	} else {
		t.Error("Expected products in result")
	}

	// Test with limit
	limited, err := rm.ListAllInstances(ctx, 0, 3)
	if err != nil {
		t.Fatalf("Failed to list limited instances: %v", err)
	}

	totalCount := 0
	for _, instances := range limited {
		totalCount += len(instances)
	}

	if totalCount != 3 {
		t.Errorf("Expected 3 total instances with limit, got %d", totalCount)
	}

	// Test with offset
	offset, err := rm.ListAllInstances(ctx, 2, 10)
	if err != nil {
		t.Fatalf("Failed to list instances with offset: %v", err)
	}

	totalCount = 0
	for _, instances := range offset {
		totalCount += len(instances)
	}

	if totalCount != 3 {
		t.Errorf("Expected 3 instances after offset 2, got %d", totalCount)
	}
}

func TestRecordManager_SetRecordIfMatch(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register record type
	rm.Register("user", TestRecord{})

	// Create initial record
	original := TestRecord{
		ID:        "123",
		Name:      "John Doe",
		Age:       30,
		IsActive:  true,
		CreatedAt: time.Now().Truncate(time.Second),
		UpdatedAt: time.Now().Truncate(time.Second),
	}

	err := rm.NewInstance(ctx, "user", "john", original)
	if err != nil {
		t.Fatalf("Failed to create record: %v", err)
	}

	// Test successful CAS update
	updated := original
	updated.Name = "John Smith"
	updated.Age = 31

	err = rm.SetRecordIfMatch(ctx, "user", "john", original, updated)
	if err != nil {
		t.Fatalf("SetRecordIfMatch should succeed with correct expected value: %v", err)
	}

	// Verify update
	result, err := rm.GetRecord(ctx, "user", "john")
	if err != nil {
		t.Fatalf("Failed to get updated record: %v", err)
	}

	updatedRecord := result.(TestRecord)
	if updatedRecord.Name != "John Smith" || updatedRecord.Age != 31 {
		t.Errorf("Record not updated correctly: got name=%s age=%d", updatedRecord.Name, updatedRecord.Age)
	}

	// Test failed CAS update (wrong expected value)
	wrongExpected := original
	wrongExpected.Age = 99 // This doesn't match current state

	newUpdate := updated
	newUpdate.Name = "Should Not Update"

	err = rm.SetRecordIfMatch(ctx, "user", "john", wrongExpected, newUpdate)
	if err == nil {
		t.Fatal("SetRecordIfMatch should fail with incorrect expected value")
	}
	if !errors.Is(err, ErrCASFailed) {
		t.Errorf("Expected ErrCASFailed, got %v", err)
	}

	// Verify record wasn't changed
	result, err = rm.GetRecord(ctx, "user", "john")
	if err != nil {
		t.Fatalf("Failed to get record: %v", err)
	}

	stillUpdated := result.(TestRecord)
	if stillUpdated.Name != "John Smith" {
		t.Errorf("Record should not have been updated: got name=%s", stillUpdated.Name)
	}
}

func TestRecordManager_SetRecordIfMatch_ConcurrentSafety(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register record type
	rm.Register("counter", SimpleRecord{})

	// Create initial record
	initial := SimpleRecord{
		Name:  "Counter",
		Value: 0,
	}

	err := rm.NewInstance(ctx, "counter", "shared", initial)
	if err != nil {
		t.Fatalf("Failed to create record: %v", err)
	}

	// Simulate concurrent updates
	const numGoroutines = 10
	var wg sync.WaitGroup
	successCount := atomic.Int32{}
	conflictCount := atomic.Int32{}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Each goroutine tries to increment the counter
			for attempt := 0; attempt < 5; attempt++ {
				// Read current value
				current, err := rm.GetRecord(ctx, "counter", "shared")
				if err != nil {
					t.Errorf("Goroutine %d: Failed to get record: %v", goroutineID, err)
					return
				}

				currentRecord := current.(SimpleRecord)

				// Try to update with CAS
				updated := SimpleRecord{
					Name:  currentRecord.Name,
					Value: currentRecord.Value + 1,
				}

				err = rm.SetRecordIfMatch(ctx, "counter", "shared", currentRecord, updated)
				if err == nil {
					successCount.Add(1)
				} else if errors.Is(err, ErrCASFailed) {
					conflictCount.Add(1)
					// This is expected - retry
					time.Sleep(time.Millisecond) // Small backoff
				} else {
					t.Errorf("Goroutine %d: Unexpected error: %v", goroutineID, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	final, err := rm.GetRecord(ctx, "counter", "shared")
	if err != nil {
		t.Fatalf("Failed to get final record: %v", err)
	}

	finalRecord := final.(SimpleRecord)

	t.Logf("Final counter value: %d", finalRecord.Value)
	t.Logf("Successful updates: %d", successCount.Load())
	t.Logf("Conflicts detected: %d", conflictCount.Load())

	// The final value should equal the number of successful updates
	if int32(finalRecord.Value) != successCount.Load() {
		t.Errorf("Data integrity issue: final value %d != successful updates %d",
			finalRecord.Value, successCount.Load())
	}

	// We should have seen some conflicts with concurrent access
	if conflictCount.Load() == 0 {
		t.Log("Warning: No conflicts detected, test might not be exercising concurrency properly")
	}
}

func TestRecordManager_CleanupOldFields(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register a simple record type
	rm.Register("simple", SimpleRecord{})

	// Create a record
	record := SimpleRecord{
		Name:  "Test",
		Value: 100,
	}

	rm.NewInstance(ctx, "simple", "test1", record)

	// Manually add some extra fields that aren't in the type definition
	// This simulates what happens after a type change that removes fields
	extraKey1 := "dev:records:simple:test1:OldField1"
	extraKey2 := "dev:records:simple:test1:OldField2"
	mockC.data[extraKey1] = "old value 1"
	mockC.data[extraKey2] = "old value 2"

	// Verify extra fields exist
	if _, exists := mockC.data[extraKey1]; !exists {
		t.Fatal("Setup failed: extra field 1 should exist")
	}
	if _, exists := mockC.data[extraKey2]; !exists {
		t.Fatal("Setup failed: extra field 2 should exist")
	}

	// Run cleanup
	err := rm.CleanupOldFields(ctx, "simple", "test1")
	if err != nil {
		t.Fatalf("CleanupOldFields failed: %v", err)
	}

	// Verify extra fields were removed
	if _, exists := mockC.data[extraKey1]; exists {
		t.Error("Extra field 1 should have been cleaned up")
	}
	if _, exists := mockC.data[extraKey2]; exists {
		t.Error("Extra field 2 should have been cleaned up")
	}

	// Verify valid fields still exist
	nameKey := "dev:records:simple:test1:Name"
	valueKey := "dev:records:simple:test1:Value"
	metaKey := "dev:records:simple:test1:__meta__"

	if _, exists := mockC.data[nameKey]; !exists {
		t.Error("Name field should still exist")
	}
	if _, exists := mockC.data[valueKey]; !exists {
		t.Error("Value field should still exist")
	}
	if _, exists := mockC.data[metaKey]; !exists {
		t.Error("Metadata key should still exist")
	}

	// Verify record is still readable
	result, err := rm.GetRecord(ctx, "simple", "test1")
	if err != nil {
		t.Fatalf("Failed to get record after cleanup: %v", err)
	}

	cleaned := result.(SimpleRecord)
	if cleaned.Name != "Test" || cleaned.Value != 100 {
		t.Errorf("Record data corrupted after cleanup: got name=%s value=%d", cleaned.Name, cleaned.Value)
	}
}

func TestRecordManager_FieldTypeConversion(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Define old record type with string value
	type OldRecord struct {
		Name  string
		Value string // Was string
		Score string // Was string
	}

	// Define new record type with numeric values
	type NewRecord struct {
		Name  string
		Value int     // Now int
		Score float64 // Now float
	}

	// Register old type and create records
	rm.Register("convertible", OldRecord{})

	// Create records with string values that can be converted
	old1 := OldRecord{
		Name:  "Test1",
		Value: "42",
		Score: "98.5",
	}
	rm.NewInstance(ctx, "convertible", "test1", old1)

	// Create record with float stored as string
	old2 := OldRecord{
		Name:  "Test2",
		Value: "3.14", // Float as string, will truncate to 3 when converted to int
		Score: "100",  // Int as string, will convert to float
	}
	rm.NewInstance(ctx, "convertible", "test2", old2)

	// Re-register with new type
	rm.Register("convertible", NewRecord{})

	// Read records with new type - should handle type conversion
	result1, err := rm.GetRecord(ctx, "convertible", "test1")
	if err != nil {
		t.Fatalf("Failed to get record after type change: %v", err)
	}

	new1 := result1.(NewRecord)
	if new1.Name != "Test1" || new1.Value != 42 || new1.Score != 98.5 {
		t.Errorf("Type conversion failed for test1: got name=%s value=%d score=%f",
			new1.Name, new1.Value, new1.Score)
	}

	result2, err := rm.GetRecord(ctx, "convertible", "test2")
	if err != nil {
		t.Fatalf("Failed to get record after type change: %v", err)
	}

	new2 := result2.(NewRecord)
	if new2.Name != "Test2" || new2.Value != 3 || new2.Score != 100.0 {
		t.Errorf("Type conversion failed for test2: got name=%s value=%d score=%f",
			new2.Name, new2.Value, new2.Score)
	}

	// Test boolean conversion
	type BoolRecord struct {
		Name    string
		Active  string
		Enabled string
		Flag    string
	}

	type NewBoolRecord struct {
		Name    string
		Active  bool
		Enabled bool
		Flag    bool
	}

	rm.Register("booltest", BoolRecord{})

	boolOld := BoolRecord{
		Name:    "BoolTest",
		Active:  "true",
		Enabled: "1",
		Flag:    "yes",
	}
	rm.NewInstance(ctx, "booltest", "test1", boolOld)

	rm.Register("booltest", NewBoolRecord{})

	resultBool, err := rm.GetRecord(ctx, "booltest", "test1")
	if err != nil {
		t.Fatalf("Failed to get bool record: %v", err)
	}

	newBool := resultBool.(NewBoolRecord)
	if !newBool.Active || !newBool.Enabled || !newBool.Flag {
		t.Errorf("Boolean conversion failed: active=%v enabled=%v flag=%v",
			newBool.Active, newBool.Enabled, newBool.Flag)
	}
}

func TestRecordManager_InstanceNameValidation(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register record type
	rm.Register("user", SimpleRecord{})

	// Test empty instance name
	err := rm.NewInstance(ctx, "user", "", SimpleRecord{Name: "Test"})
	if err == nil {
		t.Fatal("Expected error for empty instance name")
	}

	// Test instance name with colon
	err = rm.NewInstance(ctx, "user", "bad:instance", SimpleRecord{Name: "Test"})
	if err == nil {
		t.Fatal("Expected error for instance name with colon")
	}

	// Test valid instance name
	err = rm.NewInstance(ctx, "user", "valid_instance", SimpleRecord{Name: "Test", Value: 123})
	if err != nil {
		t.Fatalf("Should allow valid instance name: %v", err)
	}

	// Verify it was created
	record, err := rm.GetRecord(ctx, "user", "valid_instance")
	if err != nil {
		t.Fatalf("Failed to get valid record: %v", err)
	}

	result := record.(SimpleRecord)
	if result.Name != "Test" || result.Value != 123 {
		t.Errorf("Record not stored correctly: got name=%s value=%d", result.Name, result.Value)
	}
}

// Mock controllers that directly use our mock client

type mockValueController struct {
	mock   *mockClient
	prefix string
}

func (m *mockValueController) PushScope(prefix string) error { return nil }
func (m *mockValueController) PopScope() error               { return nil }
func (m *mockValueController) GetPrefix() string             { return m.prefix }

func (m *mockValueController) Get(ctx context.Context, key string) (string, error) {
	fullKey := fmt.Sprintf("%s:%s", m.prefix, key)
	return m.mock.Get(fullKey)
}

func (m *mockValueController) Set(ctx context.Context, key string, value string) error {
	fullKey := fmt.Sprintf("%s:%s", m.prefix, key)
	return m.mock.Set(fullKey, value)
}

func (m *mockValueController) Delete(ctx context.Context, key string) error {
	fullKey := fmt.Sprintf("%s:%s", m.prefix, key)
	return m.mock.Delete(fullKey)
}

func (m *mockValueController) CompareAndSwap(ctx context.Context, key string, oldValue, newValue string) error {
	fullKey := fmt.Sprintf("%s:%s", m.prefix, key)
	err := m.mock.CompareAndSwap(fullKey, oldValue, newValue)
	// Translate client errors to ferry errors
	if errors.Is(err, client.ErrConflict) {
		return ErrConflict
	}
	if errors.Is(err, client.ErrKeyNotFound) {
		return ErrKeyNotFound
	}
	return err
}

func (m *mockValueController) Bump(ctx context.Context, key string, value int) error {
	return m.mock.Bump(key, value)
}

func (m *mockValueController) IterateByPrefix(ctx context.Context, prefix string, offset, limit int) ([]string, error) {
	fullPrefix := fmt.Sprintf("%s:%s", m.prefix, prefix)
	keys, err := m.mock.IterateByPrefix(fullPrefix, offset, limit)
	if err != nil {
		return nil, err
	}

	// Remove the prefix from returned keys
	prefixLen := len(m.prefix) + 1
	result := make([]string, len(keys))
	for i, key := range keys {
		if len(key) > prefixLen {
			result[i] = key[prefixLen:]
		}
	}
	return result, nil
}

type mockCacheController struct {
	mock   *mockClient
	prefix string
}

func (m *mockCacheController) PushScope(prefix string) error { return nil }
func (m *mockCacheController) PopScope() error               { return nil }
func (m *mockCacheController) GetPrefix() string             { return m.prefix }

func (m *mockCacheController) Get(ctx context.Context, key string) (string, error) {
	fullKey := fmt.Sprintf("%s:%s", m.prefix, key)
	return m.mock.GetCache(fullKey)
}

func (m *mockCacheController) Set(ctx context.Context, key string, value string) error {
	fullKey := fmt.Sprintf("%s:%s", m.prefix, key)
	return m.mock.SetCache(fullKey, value)
}

func (m *mockCacheController) SetNX(ctx context.Context, key string, value string) error {
	fullKey := fmt.Sprintf("%s:%s", m.prefix, key)
	return m.mock.SetCacheNX(fullKey, value)
}

func (m *mockCacheController) Delete(ctx context.Context, key string) error {
	fullKey := fmt.Sprintf("%s:%s", m.prefix, key)
	return m.mock.DeleteCache(fullKey)
}

func (m *mockCacheController) CompareAndSwap(ctx context.Context, key string, oldValue, newValue string) error {
	fullKey := fmt.Sprintf("%s:%s", m.prefix, key)
	err := m.mock.CompareAndSwapCache(fullKey, oldValue, newValue)
	// Translate client errors to ferry errors
	if errors.Is(err, client.ErrConflict) {
		return ErrConflict
	}
	if errors.Is(err, client.ErrKeyNotFound) {
		return ErrKeyNotFound
	}
	return err
}

func (m *mockCacheController) IterateByPrefix(ctx context.Context, prefix string, offset, limit int) ([]string, error) {
	fullPrefix := fmt.Sprintf("%s:%s", m.prefix, prefix)
	keys, err := m.mock.IterateCacheByPrefix(fullPrefix, offset, limit)
	if err != nil {
		return nil, err
	}

	// Sort keys for consistent pagination
	sort.Strings(keys)

	// Apply offset and limit
	start := offset
	if start > len(keys) {
		start = len(keys)
	}
	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}

	return keys[start:end], nil
}

type failingValueController struct {
	mockValueController
	failOnKey string
}

func (m *failingValueController) Set(ctx context.Context, key string, value string) error {
	fullKey := fmt.Sprintf("%s:%s", m.prefix, key)
	if fullKey == m.failOnKey {
		return errors.New("simulated storage failure")
	}
	return m.mockValueController.Set(ctx, key, value)
}

func TestRecordManager_SetRecordRollback(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register record type
	rm.Register("user", TestRecord{})

	// Create initial record (without failing mock)
	original := TestRecord{
		ID:        "123",
		Name:      "Original",
		Age:       25,
		IsActive:  true,
		CreatedAt: time.Now().Truncate(time.Second),
		UpdatedAt: time.Now().Truncate(time.Second),
	}

	err := rm.NewInstance(ctx, "user", "test1", original)
	if err != nil {
		t.Fatalf("Failed to create record: %v", err)
	}

	// Verify original state
	result, err := rm.GetRecord(ctx, "user", "test1")
	if err != nil {
		t.Fatalf("Failed to get record: %v", err)
	}

	orig := result.(TestRecord)
	if orig.Name != "Original" || orig.Age != 25 {
		t.Errorf("Unexpected original state: name=%s age=%d", orig.Name, orig.Age)
	}

	// Now create the failing mock for the update
	failingMock := &failingValueController{
		mockValueController: mockValueController{mock: mockC, prefix: "dev:records"},
		failOnKey:           "dev:records:user:test1:IsActive", // Will fail when storing this field
	}

	// Replace the value controller with the failing one
	rm.valueCtrl = failingMock

	updated := TestRecord{
		ID:        "123",
		Name:      "Should Rollback",
		Age:       30,
		IsActive:  false, // This field will fail to store
		CreatedAt: original.CreatedAt,
		UpdatedAt: time.Now().Truncate(time.Second),
	}

	err = rm.SetRecord(ctx, "user", "test1", updated)
	if err == nil {
		t.Fatal("SetRecord should have failed")
	}

	// Reset to normal controller for reading
	rm.valueCtrl = &mockValueController{mock: mockC, prefix: "dev:records"}

	// Verify record was rolled back to original state
	result, err = rm.GetRecord(ctx, "user", "test1")
	if err != nil {
		t.Fatalf("Failed to get record after rollback: %v", err)
	}

	rolledBack := result.(TestRecord)
	if rolledBack.Name != "Original" || rolledBack.Age != 25 {
		t.Errorf("Record not rolled back correctly: got name=%s age=%d, want name=Original age=25",
			rolledBack.Name, rolledBack.Age)
	}
}

func TestRecordManager_SafeRollback(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register record type
	rm.Register("user", TestRecord{})

	// Create initial record
	initial := TestRecord{
		ID:        "123",
		Name:      "Original User",
		Age:       25,
		IsActive:  true,
		CreatedAt: time.Now().Truncate(time.Second),
		UpdatedAt: time.Now().Truncate(time.Second),
	}

	// Store initial record
	err := rm.NewInstance(ctx, "user", "test1", initial)
	if err != nil {
		t.Fatalf("Failed to create initial record: %v", err)
	}

	// Get the initial checksum
	metaKey := rm.buildIndexKey("user", "test1")
	initialChecksum, _ := rm.valueCtrl.Get(ctx, metaKey)

	// Simulate an upgrade that will fail
	upgraded := TestRecord{
		ID:        "123",
		Name:      "Upgraded User",
		Age:       26,
		IsActive:  false,
		CreatedAt: initial.CreatedAt,
		UpdatedAt: time.Now().Truncate(time.Second),
	}

	// Update the checksum as if upgrade started
	upgradedChecksum, _ := rm.computeRecordChecksum(upgraded)
	err = rm.valueCtrl.CompareAndSwap(ctx, metaKey, initialChecksum, upgradedChecksum)
	if err != nil {
		t.Fatalf("Failed to update checksum: %v", err)
	}

	// Store some fields but not all (simulating partial failure)
	rm.valueCtrl.Set(ctx, "user:test1:Name", "Upgraded User")
	rm.valueCtrl.Set(ctx, "user:test1:Age", "26")
	// Don't update IsActive - simulating failure

	// Now simulate another process modifying the record
	concurrent := TestRecord{
		ID:        "123",
		Name:      "Concurrent Update",
		Age:       30,
		IsActive:  true,
		CreatedAt: initial.CreatedAt,
		UpdatedAt: time.Now().Add(time.Minute).Truncate(time.Second),
	}

	// Another process updates with proper CAS
	concurrentChecksum, _ := rm.computeRecordChecksum(concurrent)
	err = rm.valueCtrl.CompareAndSwap(ctx, metaKey, upgradedChecksum, concurrentChecksum)
	if err != nil {
		t.Fatalf("Concurrent update should succeed: %v", err)
	}

	// Update all fields for concurrent update
	rm.storeRecordFields(ctx, "user", "test1", concurrent)

	// Now try to rollback the original upgrade
	err = rm.safeRollbackRecord(ctx, "user", "test1", initial, upgradedChecksum)
	if err == nil {
		t.Fatal("Rollback should fail because record was modified")
	}

	// Verify the record still has the concurrent update
	final, err := rm.GetRecord(ctx, "user", "test1")
	if err != nil {
		t.Fatalf("Failed to get final record: %v", err)
	}

	finalRecord := final.(TestRecord)
	if finalRecord.Name != "Concurrent Update" || finalRecord.Age != 30 {
		t.Errorf("Record should keep concurrent update: got name=%s age=%d", finalRecord.Name, finalRecord.Age)
	}

	// Test successful rollback when no concurrent modification
	// Create another record
	initial2 := TestRecord{
		ID:   "456",
		Name: "Test User 2",
		Age:  40,
	}

	err = rm.NewInstance(ctx, "user", "test2", initial2)
	if err != nil {
		t.Fatalf("Failed to create second record: %v", err)
	}

	// Get checksum
	metaKey2 := rm.buildIndexKey("user", "test2")
	checksum2, _ := rm.valueCtrl.Get(ctx, metaKey2)

	// Update to new version
	updated2 := TestRecord{
		ID:   "456",
		Name: "Updated User 2",
		Age:  41,
	}

	newChecksum2, _ := rm.computeRecordChecksum(updated2)
	rm.valueCtrl.CompareAndSwap(ctx, metaKey2, checksum2, newChecksum2)
	rm.storeRecordFields(ctx, "user", "test2", updated2)

	// Now rollback should succeed
	err = rm.safeRollbackRecord(ctx, "user", "test2", initial2, newChecksum2)
	if err != nil {
		t.Fatalf("Rollback should succeed when no concurrent modification: %v", err)
	}

	// Verify rollback worked
	rolled, err := rm.GetRecord(ctx, "user", "test2")
	if err != nil {
		t.Fatalf("Failed to get rolled back record: %v", err)
	}

	rolledRecord := rolled.(TestRecord)
	if rolledRecord.Name != "Test User 2" || rolledRecord.Age != 40 {
		t.Errorf("Record not rolled back correctly: got name=%s age=%d", rolledRecord.Name, rolledRecord.Age)
	}
}

func TestRecordManager_EfficientListInstances(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register record type
	rm.Register("product", SimpleRecord{})

	// Test 1: Empty list
	instances, err := rm.ListInstances(ctx, "product", 0, 10)
	if err != nil {
		t.Fatalf("Failed to list empty instances: %v", err)
	}
	if len(instances) != 0 {
		t.Errorf("Expected 0 instances, got %d", len(instances))
	}

	// Test 2: Create instances and verify they appear in the index
	for i := 0; i < 25; i++ {
		record := SimpleRecord{
			Name:  fmt.Sprintf("Product %d", i),
			Value: i * 10,
		}
		err := rm.NewInstance(ctx, "product", fmt.Sprintf("prod_%03d", i), record)
		if err != nil {
			t.Fatalf("Failed to create instance %d: %v", i, err)
		}
	}

	// Verify instance index keys were created
	indexCount := 0
	for key := range mockC.data {
		if strings.HasPrefix(key, "dev:records:__instances__:product:") {
			indexCount++
		}
	}
	if indexCount != 25 {
		t.Errorf("Expected 25 instance index entries, got %d", indexCount)
	}

	// Test 3: List all instances
	instances, err = rm.ListInstances(ctx, "product", 0, 30)
	if err != nil {
		t.Fatalf("Failed to list all instances: %v", err)
	}
	if len(instances) != 25 {
		t.Errorf("Expected 25 instances, got %d", len(instances))
	}

	// Test 4: Pagination
	page1, err := rm.ListInstances(ctx, "product", 0, 10)
	if err != nil {
		t.Fatalf("Failed to list page 1: %v", err)
	}
	if len(page1) != 10 {
		t.Errorf("Expected 10 instances in page 1, got %d", len(page1))
	}

	page2, err := rm.ListInstances(ctx, "product", 10, 10)
	if err != nil {
		t.Fatalf("Failed to list page 2: %v", err)
	}
	if len(page2) != 10 {
		t.Errorf("Expected 10 instances in page 2, got %d", len(page2))
	}

	page3, err := rm.ListInstances(ctx, "product", 20, 10)
	if err != nil {
		t.Fatalf("Failed to list page 3: %v", err)
	}
	if len(page3) != 5 {
		t.Errorf("Expected 5 instances in page 3, got %d", len(page3))
	}

	// Verify no overlap between pages
	pageMap := make(map[string]bool)
	for _, inst := range page1 {
		pageMap[inst] = true
	}
	for _, inst := range page2 {
		if pageMap[inst] {
			t.Errorf("Instance %s appears in both page 1 and page 2", inst)
		}
		pageMap[inst] = true
	}
	for _, inst := range page3 {
		if pageMap[inst] {
			t.Errorf("Instance %s appears in multiple pages", inst)
		}
	}

	// Test 5: Delete instance removes from index
	err = rm.DeleteRecord(ctx, "product", "prod_010")
	if err != nil {
		t.Fatalf("Failed to delete record: %v", err)
	}

	// Verify index entry was removed
	indexKey := "dev:records:__instances__:product:prod_010"
	if _, exists := mockC.data[indexKey]; exists {
		t.Error("Instance index entry should have been deleted")
	}

	// List should now show 24 instances
	instances, err = rm.ListInstances(ctx, "product", 0, 30)
	if err != nil {
		t.Fatalf("Failed to list after delete: %v", err)
	}
	if len(instances) != 24 {
		t.Errorf("Expected 24 instances after delete, got %d", len(instances))
	}

	// Test 6: Performance comparison (simulate)
	// Count how many keys we iterate with new method vs old
	iteratedKeys := 0
	indexPrefix := "dev:records:__instances__:product:"
	for key := range mockC.data {
		if strings.HasPrefix(key, indexPrefix) {
			iteratedKeys++
		}
	}

	// With old method, we'd iterate all field keys
	totalFieldKeys := 0
	recordPrefix := "dev:records:product:"
	for key := range mockC.data {
		if strings.HasPrefix(key, recordPrefix) && !strings.HasPrefix(key, indexPrefix) {
			totalFieldKeys++
		}
	}

	t.Logf("Performance improvement: %d index keys vs %d total field keys (%.1fx improvement)",
		iteratedKeys, totalFieldKeys, float64(totalFieldKeys)/float64(iteratedKeys))

	if iteratedKeys >= totalFieldKeys {
		t.Error("Index should have fewer keys than total fields")
	}
}

func TestRecordManager_IndexMigration(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register record type
	rm.Register("legacy", SimpleRecord{})

	// Create records WITHOUT using NewInstance (simulating old records)
	// Directly set the data to simulate pre-index records
	for i := 0; i < 10; i++ {
		instanceName := fmt.Sprintf("legacy_%d", i)

		// Set metadata key (old style)
		metaKey := fmt.Sprintf("dev:records:legacy:%s:__meta__", instanceName)
		mockC.data[metaKey] = "old_checksum"

		// Set fields
		mockC.data[fmt.Sprintf("dev:records:legacy:%s:Name", instanceName)] = fmt.Sprintf("Legacy %d", i)
		mockC.data[fmt.Sprintf("dev:records:legacy:%s:Value", instanceName)] = fmt.Sprintf("%d", i*5)
	}

	// Verify no index entries exist
	indexCount := 0
	for key := range mockC.data {
		if strings.Contains(key, "__instances__") {
			indexCount++
		}
	}
	if indexCount != 0 {
		t.Errorf("Expected 0 index entries for legacy records, got %d", indexCount)
	}

	// List should still work (using fallback)
	instances, err := rm.ListInstances(ctx, "legacy", 0, 20)
	if err != nil {
		t.Fatalf("Failed to list legacy instances: %v", err)
	}
	if len(instances) != 10 {
		t.Errorf("Expected 10 legacy instances, got %d", len(instances))
	}

	// Run index rebuild
	err = rm.RebuildInstanceIndex(ctx, "legacy")
	if err != nil {
		t.Fatalf("Failed to rebuild index: %v", err)
	}

	// Verify index entries were created
	indexCount = 0
	for key := range mockC.data {
		if strings.HasPrefix(key, "dev:records:__instances__:legacy:") {
			indexCount++
		}
	}
	if indexCount != 10 {
		t.Errorf("Expected 10 index entries after rebuild, got %d", indexCount)
	}

	// List should now use the efficient index
	instances, err = rm.ListInstances(ctx, "legacy", 0, 20)
	if err != nil {
		t.Fatalf("Failed to list after rebuild: %v", err)
	}
	if len(instances) != 10 {
		t.Errorf("Expected 10 instances after rebuild, got %d", len(instances))
	}
}

func TestRecordManager_MixedIndexState(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register record type
	rm.Register("mixed", SimpleRecord{})

	// Create some records with index
	for i := 0; i < 5; i++ {
		record := SimpleRecord{
			Name:  fmt.Sprintf("Indexed %d", i),
			Value: i,
		}
		rm.NewInstance(ctx, "mixed", fmt.Sprintf("indexed_%d", i), record)
	}

	// Create some records without index (simulate old records)
	for i := 0; i < 5; i++ {
		instanceName := fmt.Sprintf("old_%d", i)

		// Set metadata key
		metaKey := fmt.Sprintf("dev:records:mixed:%s:__meta__", instanceName)
		mockC.data[metaKey] = "checksum"

		// Set fields
		mockC.data[fmt.Sprintf("dev:records:mixed:%s:Name", instanceName)] = fmt.Sprintf("Old %d", i)
		mockC.data[fmt.Sprintf("dev:records:mixed:%s:Value", instanceName)] = fmt.Sprintf("%d", i)
	}

	// List should return all 10 (5 from index, 5 from fallback)
	instances, err := rm.ListInstances(ctx, "mixed", 0, 20)
	if err != nil {
		t.Fatalf("Failed to list mixed instances: %v", err)
	}
	if len(instances) != 10 {
		t.Errorf("Expected 10 total instances, got %d", len(instances))
	}

	// Verify we have both indexed and old instances
	hasIndexed := false
	hasOld := false
	for _, inst := range instances {
		if strings.HasPrefix(inst, "indexed_") {
			hasIndexed = true
		}
		if strings.HasPrefix(inst, "old_") {
			hasOld = true
		}
	}

	if !hasIndexed {
		t.Error("Missing indexed instances in list")
	}
	if !hasOld {
		t.Error("Missing old instances in list")
	}
}

func TestRecordManager_DeleteRecordCleansAllMetadata(t *testing.T) {
	ferry, mockC := createTestFerry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	rm := &recordManagerImpl{
		ferry:     ferry,
		logger:    logger.WithGroup("test_record_manager"),
		registry:  make(map[string]reflect.Type),
		valueCtrl: &mockValueController{mock: mockC, prefix: "dev:records"},
		useCache:  false,
	}

	ctx := context.Background()

	// Register record type
	rm.Register("metadata_test", TestRecord{})

	// Create a record
	record := TestRecord{
		ID:        "123",
		Name:      "Test Metadata",
		Age:       30,
		IsActive:  true,
		CreatedAt: time.Now().Truncate(time.Second),
		UpdatedAt: time.Now().Truncate(time.Second),
	}

	err := rm.NewInstance(ctx, "metadata_test", "test_instance", record)
	if err != nil {
		t.Fatalf("Failed to create record: %v", err)
	}

	// Simulate an upgrade to create upgrade status
	err = rm.setUpgradeStatus(ctx, "metadata_test", "test_instance", upgradeStatusCompleted, "test_checksum")
	if err != nil {
		t.Fatalf("Failed to set upgrade status: %v", err)
	}

	// Verify all metadata exists before deletion
	expectedKeys := map[string]bool{
		// Record fields
		"dev:records:metadata_test:test_instance:ID":        true,
		"dev:records:metadata_test:test_instance:Name":      true,
		"dev:records:metadata_test:test_instance:Age":       true,
		"dev:records:metadata_test:test_instance:IsActive":  true,
		"dev:records:metadata_test:test_instance:CreatedAt": true,
		"dev:records:metadata_test:test_instance:UpdatedAt": true,
		"dev:records:metadata_test:test_instance:Tags":      true,
		"dev:records:metadata_test:test_instance:Metadata":  true,
		// Metadata keys
		"dev:records:metadata_test:test_instance:__meta__":           true,
		"dev:records:__instances__:metadata_test:test_instance":      true,
		"dev:records:metadata_test:test_instance:__upgrade_status__": true,
	}

	for key := range expectedKeys {
		if _, exists := mockC.data[key]; !exists {
			t.Logf("Warning: Expected key %s not found before deletion", key)
		}
	}

	// Count total keys before deletion
	keysBeforeDeletion := 0
	keyPrefix := "dev:records:"
	instanceRelatedKeys := []string{}
	for key := range mockC.data {
		if strings.HasPrefix(key, keyPrefix) && strings.Contains(key, "test_instance") {
			keysBeforeDeletion++
			instanceRelatedKeys = append(instanceRelatedKeys, key)
		}
	}
	t.Logf("Keys before deletion (%d): %v", keysBeforeDeletion, instanceRelatedKeys)

	// Delete the record
	err = rm.DeleteRecord(ctx, "metadata_test", "test_instance")
	if err != nil {
		t.Fatalf("Failed to delete record: %v", err)
	}

	// Verify ALL keys are deleted
	keysAfterDeletion := 0
	remainingKeys := []string{}
	for key := range mockC.data {
		if strings.Contains(key, "test_instance") {
			keysAfterDeletion++
			remainingKeys = append(remainingKeys, key)
		}
	}

	if keysAfterDeletion > 0 {
		t.Errorf("Found %d keys after deletion that should have been removed: %v",
			keysAfterDeletion, remainingKeys)
	}

	// Specifically check each type of metadata
	metadataChecks := []struct {
		name string
		key  string
	}{
		{"checksum metadata", "dev:records:metadata_test:test_instance:__meta__"},
		{"instance index", "dev:records:__instances__:metadata_test:test_instance"},
		{"upgrade status", "dev:records:metadata_test:test_instance:__upgrade_status__"},
		{"ID field", "dev:records:metadata_test:test_instance:ID"},
		{"Name field", "dev:records:metadata_test:test_instance:Name"},
	}

	for _, check := range metadataChecks {
		if _, exists := mockC.data[check.key]; exists {
			t.Errorf("%s should have been deleted: %s", check.name, check.key)
		}
	}

	// Verify instance doesn't appear in listing
	instances, err := rm.ListInstances(ctx, "metadata_test", 0, 10)
	if err != nil {
		t.Fatalf("Failed to list instances: %v", err)
	}
	if len(instances) != 0 {
		t.Errorf("Deleted instance still appears in listing: %v", instances)
	}
}
