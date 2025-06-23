// This test suite sets up a live single-node insi cluster to perform
// integration tests on the FWI (Fuck With It) layer. It ensures that
// the entity-scoped clients for various features (Values, Cache, Events)
// are functioning correctly.
//
// The setup is borrowed from the FWI usage example, creating a temporary
// cluster for each test execution to ensure a clean state.
package fwi_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/fwi"
	"github.com/InsulaLabs/insi/runtime"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// --- Test Suite Setup ---

// testSetup handles the boilerplate of setting up a test cluster and FWI instance.
// It returns the FWI instance and a teardown function to be deferred.
func testSetup(t *testing.T) (fwi.FWI, func()) {
	// In tests, we can use a more verbose logger.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	tempDir, err := os.MkdirTemp("", "insi-fwi-test-*")
	require.NoError(t, err, "failed to create temp dir")

	cfg := generateTestClusterConfig(tempDir)

	configPath := filepath.Join(tempDir, "cluster.yaml")
	configData, err := yaml.Marshal(cfg)
	require.NoError(t, err, "failed to marshal config")
	err = os.WriteFile(configPath, configData, 0644)
	require.NoError(t, err, "failed to write config file")

	rt, err := runtime.New([]string{"--config", configPath, "--host"}, configPath)
	require.NoError(t, err, "failed to create runtime")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := rt.Run(); err != nil {
			// This error is expected to be nil on clean shutdown.
			// During tests, it might log an error which is fine.
		}
	}()

	t.Log("waiting for cluster to initialize...")
	// Give the cluster a moment to start up and elect a leader.
	time.Sleep(3 * time.Second)

	fwiInstance, err := setupFWI(cfg, logger)
	require.NoError(t, err, "failed to setup FWI")

	teardown := func() {
		t.Log("shutting down test cluster.")
		rt.Stop()
		wg.Wait()
		os.RemoveAll(tempDir)
	}

	return fwiInstance, teardown
}

// generateTestClusterConfig creates a configuration for a single-node test cluster.
func generateTestClusterConfig(homeDir string) *config.Cluster {
	nodeID := "test-node-0"
	nodes := map[string]config.Node{
		nodeID: {
			PublicBinding:  "127.0.0.1:8081", // Use different ports to avoid conflict
			PrivateBinding: "127.0.0.1:7071",
			RaftBinding:    "127.0.0.1:2223",
			NodeSecret:     "secret-for-test-node-0",
			ClientDomain:   "localhost",
		},
	}
	keyPath := filepath.Join(homeDir, "keys")

	cfg, _ := config.GenerateConfig("")
	cfg.InstanceSecret = "fwi-test-secret"
	cfg.InsidHome = homeDir
	cfg.ClientSkipVerify = true
	cfg.DefaultLeader = nodeID
	cfg.Nodes = nodes
	cfg.PermittedIPs = []string{"127.0.0.1"}
	cfg.TLS = config.TLS{
		Cert: filepath.Join(keyPath, "server.crt"),
		Key:  filepath.Join(keyPath, "server.key"),
	}
	cfg.RootPrefix = "fwi-test-root"
	// Use smaller, faster values for tests
	cfg.Cache.StandardTTL = 1 * time.Second
	cfg.Cache.Keys = 1 * time.Minute

	return cfg
}

// setupFWI initializes the FWI service client for tests.
func setupFWI(cfg *config.Cluster, logger *slog.Logger) (fwi.FWI, error) {
	endpoints := make([]client.Endpoint, 0, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		endpoints = append(endpoints, client.Endpoint{
			PublicBinding:  node.PublicBinding,
			PrivateBinding: node.PrivateBinding,
			ClientDomain:   node.ClientDomain,
		})
	}

	h := sha256.New()
	h.Write([]byte(cfg.InstanceSecret))
	rootApiKeyHex := hex.EncodeToString(h.Sum(nil))
	rootApiKey := base64.StdEncoding.EncodeToString([]byte(rootApiKeyHex))

	rootClient, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeRandom,
		Endpoints:      endpoints,
		SkipVerify:     true,
		ApiKey:         rootApiKey,
		Logger:         logger.WithGroup("fwi-test-root-client"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create initial root client: %w", err)
	}

	rootLimits := models.Limits{
		BytesOnDisk:   new(int64),
		BytesInMemory: new(int64),
		EventsEmitted: new(int64),
		Subscribers:   new(int64),
	}
	*rootLimits.BytesOnDisk = 1 * 1024 * 1024 * 1024   // 1 GB
	*rootLimits.BytesInMemory = 1 * 1024 * 1024 * 1024 // 1 GB
	*rootLimits.EventsEmitted = 100000
	*rootLimits.Subscribers = 1000

	if err := rootClient.SetLimits(rootApiKey, rootLimits); err != nil {
		// In a test scenario, the key might already exist from a previous failed run
		// We can log this, but shouldn't fail the test setup.
		// A proper integration test would check the error type more carefully.
		logger.Warn("could not set limits on root api key, it may already be set", "error", err)
	}

	return fwi.NewFWI(
		&client.Config{
			ConnectionType: client.ConnectionTypeRandom,
			Endpoints:      endpoints,
			SkipVerify:     true,
			Logger:         logger.WithGroup("fwi-test-entities"),
		},
		rootClient,
		logger,
	)
}

// --- Tests ---

func TestFWI_CreateOrLoadEntity(t *testing.T) {
	fwiInstance, teardown := testSetup(t)
	defer teardown()

	ctx := context.Background()
	entityName := "test-entity-1"

	// Create
	entity, err := fwiInstance.CreateOrLoadEntity(ctx, entityName, models.Limits{})
	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Equal(t, entityName, entity.GetName())
	require.NotEmpty(t, entity.GetKey(), "entity should have an API key")

	// Load
	entity2, err := fwiInstance.CreateOrLoadEntity(ctx, entityName, models.Limits{})
	require.NoError(t, err)
	require.NotNil(t, entity2)
	require.Equal(t, entity.GetKey(), entity2.GetKey(), "loading an existing entity should return the same key")
}

func TestEntity_ValueStore_SetGetDelete(t *testing.T) {
	fwiInstance, teardown := testSetup(t)
	defer teardown()

	ctx := context.Background()
	appEntity, err := fwiInstance.CreateOrLoadEntity(ctx, "test-vs-entity", models.Limits{})
	require.NoError(t, err)
	vs := appEntity.GetValueStore()

	key := "my-key"
	value := "my-value"

	// Set
	err = vs.Set(ctx, key, value)
	require.NoError(t, err)

	// Get
	retrievedValue, err := vs.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, retrievedValue)

	// Delete
	err = vs.Delete(ctx, key)
	require.NoError(t, err)

	// Get after delete
	_, err = vs.Get(ctx, key)
	require.Error(t, err, "getting a deleted key should return an error")
	require.ErrorIs(t, err, client.ErrKeyNotFound, "error should be ErrNotFound")
}

func TestEntity_CacheStore_SetGetDelete(t *testing.T) {
	fwiInstance, teardown := testSetup(t)
	defer teardown()

	ctx := context.Background()
	appEntity, err := fwiInstance.CreateOrLoadEntity(ctx, "test-cs-entity", models.Limits{})
	require.NoError(t, err)
	cs := appEntity.GetCacheStore()

	key := "my-cache-key"
	value := "my-cache-value"

	// Set
	err = cs.Set(ctx, key, value)
	require.NoError(t, err)

	// Get
	retrievedValue, err := cs.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, retrievedValue)

	// Delete
	err = cs.Delete(ctx, key)
	require.NoError(t, err)

	// Get after delete
	_, err = cs.Get(ctx, key)
	require.Error(t, err, "getting a deleted key should return an error")
	require.ErrorIs(t, err, client.ErrKeyNotFound, "error should be ErrNotFound")
}

func TestEntity_Events_PubSub(t *testing.T) {
	fwiInstance, teardown := testSetup(t)
	defer teardown()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appEntity, err := fwiInstance.CreateOrLoadEntity(ctx, "test-events-entity", models.Limits{})
	require.NoError(t, err)
	events := appEntity.GetEvents()

	topic := "test-topic"
	payload := map[string]any{"hello": "world"}
	received := make(chan any, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := events.Subscribe(ctx, topic, func(data any) {
			received <- data
		})
		require.ErrorIs(t, err, context.Canceled, "subscribe should exit cleanly on context cancel")
	}()

	// Give subscriber time to connect
	time.Sleep(1 * time.Second)

	// Publish
	err = events.Publish(ctx, topic, payload)
	require.NoError(t, err)

	// Wait for receipt or timeout
	select {
	case data := <-received:
		require.Equal(t, payload, data, "received data does not match published payload")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event")
	}

	// Cancel context to stop subscriber
	cancel()
	wg.Wait()
}
