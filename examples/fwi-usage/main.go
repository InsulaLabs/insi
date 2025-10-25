// This example demonstrates how to set up and use the FWI (Fuck With It) library.
// FWI provides a high-level interface for interacting with an insi cluster,
// managing data through entities, and abstracting away the underlying client communication.
//
// The example will:
// 1. Programmatically configure and start a single-node insi cluster.
// 2. Initialize the FWI service.
// 3. Create an "Entity," which is a data scope with its own API key.
// 4. Demonstrate usage of the Entity's features:
//   - Persistent Value Store (key-value)
//   - Volatile Cache Store (key-value)
//   - Pub/Sub Events system
//
// 5. Cleanly shut down the cluster.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/InsulaLabs/insi/pkg/client"
	"github.com/InsulaLabs/insi/pkg/config"
	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/models"
	"github.com/InsulaLabs/insi/pkg/runtime"
	"gopkg.in/yaml.v3"
)

var ctx = context.Background()

func main() {
	// Use Go's structured logger for clear, leveled logging.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("starting FWI usage example")

	// For a self-contained example, we create a temporary directory to store cluster data.
	// In a real application, this would be a persistent path.
	tempDir, err := os.MkdirTemp("", "insi-fwi-example-*")
	if err != nil {
		logger.Error("failed to create temp dir", "error", err)
		os.Exit(1)
	}
	// Ensure cleanup happens even if the program exits unexpectedly.
	defer os.RemoveAll(tempDir)
	logger.Info("created temporary directory for cluster data", "path", tempDir)

	// FWI needs to connect to a running insi cluster. Here, we generate the configuration
	// for a single-node cluster programmatically.
	cfg, err := generateClusterConfig(tempDir)
	if err != nil {
		logger.Error("failed to generate cluster config", "error", err)
		os.Exit(1)
	}

	// The insi runtime reads its configuration from a YAML file.
	configPath := filepath.Join(tempDir, "cluster.yaml")
	configData, err := yaml.Marshal(cfg)
	if err != nil {
		logger.Error("failed to marshal config", "error", err)
		os.Exit(1)
	}
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		logger.Error("failed to write config file", "error", err)
		os.Exit(1)
	}
	logger.Info("generated cluster configuration", "path", configPath)

	// The `runtime` package is used to host an insi node (or a full cluster).
	// We run it in a background goroutine so our main application logic can proceed.
	rt, err := runtime.New(ctx, []string{"--config", configPath, "--host"}, configPath)
	if err != nil {
		logger.Error("failed to create runtime", "error", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// rt.Run() is a blocking call that starts the server.
		if err := rt.Run(); err != nil {
			// This error will be nil on a clean shutdown.
			logger.Error("runtime exited with error", "error", err)
		}
	}()

	// Give the cluster a moment to start up and elect a leader before we try to connect.
	logger.Info("waiting for cluster to initialize...")
	time.Sleep(5 * time.Second)

	// `setupFWI` is a helper that encapsulates the boilerplate of creating the FWI instance.
	// It creates a root client and uses it to initialize the FWI service.
	fwiInstance, err := setupFWI(cfg, logger)
	if err != nil {
		logger.Error("failed to setup FWI", "error", err)
		rt.Stop() // a failed setup should stop the runtime
		wg.Wait()
		os.Exit(1)
	}
	logger.Info("FWI instance created successfully")

	// Use a cancellable context for all operations to allow for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C (SIGINT) and other termination signals to shut down gracefully.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("shutdown signal received, cleaning up.")
		cancel()
	}()

	// --- FWI CORE LOGIC ---

	// Entities are the primary way to scope data and access in FWI.
	// Each entity gets its own API key and its data is isolated from other entities.
	// `CreateOrLoadEntity` is idempotent: it returns an existing entity or creates a new one.
	entityName := "my-awesome-app"
	logger.Info("creating or loading entity", "name", entityName)
	// We pass empty limits, which means the server will use its defaults.
	appEntity, err := fwiInstance.CreateOrLoadEntity(ctx, entityName, models.Limits{})
	if err != nil {
		logger.Error("failed to create or load entity", "error", err)
		rt.Stop()
		wg.Wait()
		os.Exit(1)
	}
	logger.Info("entity ready", "name", appEntity.GetName(), "key-hint", appEntity.GetKey()[:8]+"...")

	// FWI provides scoped access to a persistent Value Store and a volatile Cache Store.
	// All operations performed on these stores are automatically scoped to `appEntity`.

	// -- Value Store Example (Persistent) --
	logger.Info("--- Demonstrating Value Store ---")
	vs := appEntity.GetValueStore()

	vsKey := "user:123:profile"
	vsValue := `{"username": "bosley", "permissions": ["read", "write"]}`
	logger.Info("setting value in Value Store", "key", vsKey, "value", vsValue)
	if err := vs.Set(ctx, vsKey, vsValue); err != nil {
		logger.Error("failed to set value", "error", err)
	}

	retrievedValue, err := vs.Get(ctx, vsKey)
	if err != nil {
		logger.Error("failed to get value", "error", err)
	} else {
		logger.Info("retrieved value from Value Store", "key", vsKey, "value", retrievedValue)
	}

	// -- Cache Store Example (Volatile, TTL-based) --
	logger.Info("--- Demonstrating Cache Store ---")
	cs := appEntity.GetCacheStore()

	csKey := "session:token:xyz"
	csValue := "active"
	logger.Info("setting value in Cache Store", "key", csKey, "value", csValue)
	if err := cs.Set(ctx, csKey, csValue); err != nil {
		logger.Error("failed to set cache value", "error", err)
	}

	retrievedCacheValue, err := cs.Get(ctx, csKey)
	if err != nil {
		logger.Error("failed to get cache value", "error", err)
	} else {
		logger.Info("retrieved value from Cache Store", "key", csKey, "value", retrievedCacheValue)
	}

	// -- Events Example (Pub/Sub) --
	logger.Info("--- Demonstrating Events ---")
	events := appEntity.GetEvents()
	eventTopic := "user-updates"

	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		logger.Info("subscribing to topic", "topic", eventTopic)
		// Subscribe blocks until the context is cancelled or an unrecoverable error occurs.
		err := events.Subscribe(ctx, eventTopic, func(data any) {
			// This callback is executed for each event received.
			// The `data` is automatically unmarshalled from JSON.
			logger.Info("event received!", "topic", eventTopic, "data", data)
		})
		// When the context is cancelled, the subscriber will exit gracefully.
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("subscriber exited with error", "error", err)
		} else {
			logger.Info("subscriber shutdown gracefully")
		}
	}()

	// Give the subscriber a moment to establish its connection.
	time.Sleep(1 * time.Second)

	eventPayload := map[string]any{"userID": 123, "status": "online"}
	logger.Info("publishing event", "topic", eventTopic, "payload", eventPayload)
	if err := events.Publish(ctx, eventTopic, eventPayload); err != nil {
		logger.Error("failed to publish event", "error", err)
	}

	// Wait a moment to ensure the event is processed before we shut down.
	time.Sleep(1 * time.Second)

	// -- Update Entity Limits Example --
	logger.Info("--- Demonstrating Entity Limit Updates ---")
	// You can update limits for an entity after it has been created.
	// For example, to increase the number of events this entity can emit per time window.
	// Any limits you don't specify in the `models.Limits` struct will remain unchanged.
	newEventsLimit := int64(5000)
	newLimits := models.Limits{
		EventsEmitted: &newEventsLimit,
	}
	logger.Info("updating entity limits", "name", appEntity.GetName(), "new_events_limit", newEventsLimit)
	if err := fwiInstance.UpdateEntityLimits(ctx, appEntity.GetName(), newLimits); err != nil {
		logger.Error("failed to update entity limits", "error", err)
	} else {
		logger.Info("successfully requested entity limit update")
	}

	// --- SHUTDOWN ---
	logger.Info("example finished, initiating shutdown.")
	// Cancel the context to signal subscribers and other background tasks to stop.
	cancel()
	// Wait for the event subscriber goroutine to finish.
	eventWg.Wait()

	// Stop the insi runtime.
	rt.Stop()
	// Wait for the runtime goroutine to exit.
	wg.Wait()
	logger.Info("cluster shut down successfully.")
}

// generateClusterConfig creates a configuration for a single-node test cluster.
// This is boilerplate for setting up a local server to test against.
func generateClusterConfig(homeDir string) (*config.Cluster, error) {
	nodeID := "node0"
	nodes := map[string]config.Node{
		nodeID: {
			PublicBinding:  "127.0.0.1:8080",
			PrivateBinding: "127.0.0.1:7070",
			RaftBinding:    "127.0.0.1:2222",
			NodeSecret:     "secret-for-node0",
			ClientDomain:   "localhost",
		},
	}
	keyPath := filepath.Join(homeDir, "keys")

	return &config.Cluster{
		InstanceSecret:   "fwi-example-secret",
		InsidHome:        homeDir,
		ClientSkipVerify: true,
		DefaultLeader:    nodeID,
		Nodes:            nodes,
		TrustedProxies:   []string{"127.0.0.1"},
		PermittedIPs:     []string{"127.0.0.1"},
		TLS: config.TLS{
			// The runtime will generate self-signed certs if these files don't exist.
			Cert: filepath.Join(keyPath, "server.crt"),
			Key:  filepath.Join(keyPath, "server.key"),
		},
		// Using simple default values for the example.
		Cache: config.Cache{
			StandardTTL: 5 * time.Minute,
			Keys:        1 * time.Hour,
		},
		RootPrefix: "fwi-example-root",
		RateLimiters: config.RateLimiters{
			Values:  config.RateLimiterConfig{Limit: 1000, Burst: 2000},
			Cache:   config.RateLimiterConfig{Limit: 1000, Burst: 2000},
			System:  config.RateLimiterConfig{Limit: 5000, Burst: 10000},
			Default: config.RateLimiterConfig{Limit: 1000, Burst: 2000},
			Events:  config.RateLimiterConfig{Limit: 2000, Burst: 4000},
		},
		Sessions: config.SessionsConfig{
			EventChannelSize:         100,
			WebSocketReadBufferSize:  4096,
			WebSocketWriteBufferSize: 4096,
			MaxConnections:           100,
		},
	}, nil
}

// setupFWI initializes the FWI service client. It handles the creation of a
// root-level API client that has permissions to create entities, and then
// initializes the FWI instance which will be used to manage those entities.
func setupFWI(cfg *config.Cluster, logger *slog.Logger) (fwi.FWI, error) {
	// The FWI client needs to know the endpoints of the cluster nodes.
	endpoints := make([]client.Endpoint, 0, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		endpoints = append(endpoints, client.Endpoint{
			PublicBinding:  node.PublicBinding,
			PrivateBinding: node.PrivateBinding,
			ClientDomain:   node.ClientDomain,
		})
	}

	// The root API key is derived from the `InstanceSecret` in the cluster config.
	// This provides a predictable way to get admin access to a known cluster.
	h := sha256.New()
	h.Write([]byte(cfg.InstanceSecret))
	rootApiKeyHex := hex.EncodeToString(h.Sum(nil))
	rootApiKey := base64.StdEncoding.EncodeToString([]byte(rootApiKeyHex))

	// Create a "root" client using the derived key. This client has permissions
	// to perform administrative tasks, like creating API keys for entities.
	rootClient, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeRandom,
		Endpoints:      endpoints,
		SkipVerify:     true, // For local/test setups
		ApiKey:         rootApiKey,
		Logger:         logger.WithGroup("fwi-root-client"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create initial root client: %w", err)
	}

	// The root key needs to have limits set on itself so it can then provision
	// limits for the entities it creates. This is a critical step.
	logger.Info("setting limits for the root API key to allow entity creation")
	rootLimits := models.Limits{
		BytesOnDisk:   new(int64),
		BytesInMemory: new(int64),
		EventsEmitted: new(int64),
		Subscribers:   new(int64),
	}
	// Give the root key generous limits (e.g., 10GB disk/memory).
	// These limits constrain the total resources all entities created by this key can have.
	*rootLimits.BytesOnDisk = 10 * 1024 * 1024 * 1024   // 10 GB
	*rootLimits.BytesInMemory = 10 * 1024 * 1024 * 1024 // 10 GB
	*rootLimits.EventsEmitted = 1000000
	*rootLimits.Subscribers = 10000

	if err := rootClient.SetLimits(rootApiKey, rootLimits); err != nil {
		return nil, fmt.Errorf("failed to set limits on root api key: %w", err)
	}
	logger.Info("successfully set limits for the root API key")

	// NewFWI takes the root client and a base configuration that will be used
	// for the clients it creates for each entity.
	return fwi.NewFWI(
		&client.Config{
			ConnectionType: client.ConnectionTypeRandom,
			Endpoints:      endpoints,
			SkipVerify:     true,
			Logger:         logger.WithGroup("fwi-entities"),
		},
		logger,
	)
}
