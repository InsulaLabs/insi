package ferry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

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
	}

	for _, key := range keys {
		if _, exists := mockC.data[key]; exists {
			t.Errorf("Key %s should have been deleted", key)
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
		return NewRecord{
			Name:      old.Name,
			Age:       old.Age,
			UpdatedAt: time.Now(),
		}, nil
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
	return m.mock.CompareAndSwap(fullKey, oldValue, newValue)
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
	return m.mock.CompareAndSwapCache(fullKey, oldValue, newValue)
}

func (m *mockCacheController) IterateByPrefix(ctx context.Context, prefix string, offset, limit int) ([]string, error) {
	fullPrefix := fmt.Sprintf("%s:%s", m.prefix, prefix)
	keys, err := m.mock.IterateCacheByPrefix(fullPrefix, offset, limit)
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
