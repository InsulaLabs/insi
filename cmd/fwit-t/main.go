package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/fwi"
	"github.com/InsulaLabs/insi/runtime"
	"github.com/fatih/color"
	"gopkg.in/yaml.v3"
)

// fuck with it - test

// Using FWT we will spin up a a cluster of 1, stress it out, get metrics, log it, and then do the same for 3 and 5.

type StressMetrics struct {
	EntityName      string
	Sets            int64
	SetsTime        time.Duration
	Gets            int64
	GetsTime        time.Duration
	Deletes         int64
	DeletesTime     time.Duration
	Caches          int64
	CachesTime      time.Duration
	Publishes       int64
	PublishesTime   time.Duration
	Bumps           int64
	BumpsTime       time.Duration
	BlobSets        int64
	BlobSetsTime    time.Duration
	BlobGets        int64
	BlobGetsTime    time.Duration
	BlobDeletes     int64
	BlobDeletesTime time.Duration
	Failures        int64
}

type TimedError struct {
	Timestamp time.Time
	Entity    string
	Error     error
}

var (
	globalErrors   []TimedError
	globalErrorsMu sync.Mutex
)

type SubscriberMetrics struct {
	SubscriberID   int
	ListeningTo    string
	EventsReceived int64
}

type SafeEntityPool struct {
	mu       sync.RWMutex
	entities []fwi.Entity
}

func main() {

	// misconfigured logger in raft bbolt backend
	// we use slog so this only silences the bbolt backend
	// to raft snapshot store that isn't directly used by us
	log.SetOutput(io.Discard)

	clusterSize := flag.Int("cluster-size", 1, "Number of nodes in the test cluster.")
	numEntities := flag.Int("entities", 10, "Number of entities to create for the stress test.")
	duration := flag.Duration("duration", 1*time.Minute, "Duration of the stress test.")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error).")
	disableChaos := flag.Bool("disable-chaos", false, "Disable the chaos monkey for deleting entities.")
	numSubscribers := flag.Int("subscribers", 2, "Number of concurrent event subscribers to run.")
	configFile := flag.String("config-file", "", "Path to an existing cluster config file. If provided, a local cluster will not be created.")
	flag.Parse()

	// Setup logger
	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})).With("service", "fwit-t")

	// Colors
	cyan := color.New(color.FgCyan)
	green := color.New(color.FgGreen)
	yellow := color.New(color.FgYellow)
	red := color.New(color.FgRed)

	var cfg *config.Cluster
	var rt *runtime.Runtime
	var wg sync.WaitGroup
	var err error

	if *configFile == "" {
		cyan.Printf("🚀 Starting a new local cluster of size %d\n", *clusterSize)
		// Create a temporary directory for the cluster
		tempDir, err := os.MkdirTemp("", "insi-stress-test-*")
		if err != nil {
			red.Printf("❌ Failed to create temp dir: %v\n", err)
			os.Exit(1)
		}
		defer func() {
			yellow.Printf("🧹 Cleaning up temporary directory: %s\n", tempDir)
			os.RemoveAll(tempDir)
		}()
		cyan.Printf("📂 Created temporary directory for cluster data at %s\n", tempDir)

		// Generate cluster configuration
		cfg, err = generateClusterConfig(*clusterSize, tempDir)
		if err != nil {
			red.Printf("❌ Failed to generate cluster config: %v\n", err)
			os.Exit(1)
		}

		configPath := filepath.Join(tempDir, "cluster.yaml")
		configData, err := yaml.Marshal(cfg)
		if err != nil {
			red.Printf("❌ Failed to marshal config: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(configPath, configData, 0644); err != nil {
			red.Printf("❌ Failed to write config file: %v\n", err)
			os.Exit(1)
		}
		cyan.Printf("📜 Generated cluster configuration: %s\n", configPath)

		// Setup and run the cluster runtime
		rt, err = runtime.New([]string{"--config", configPath, "--host"}, configPath)
		if err != nil {
			red.Printf("❌ Failed to create runtime: %v\n", err)
			os.Exit(1)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rt.Run(); err != nil {
				red.Printf("❌ Runtime exited with error: %v\n", err)
			}
		}()

		cyan.Printf("⏳ Waiting for local cluster to initialize... (15s)\n")
		time.Sleep(15 * time.Second)
	} else {
		cyan.Printf("🔎 Using existing cluster configuration from: %s\n", *configFile)
		configData, err := os.ReadFile(*configFile)
		if err != nil {
			red.Printf("❌ Failed to read config file %s: %v\n", *configFile, err)
			os.Exit(1)
		}
		cfg = &config.Cluster{}
		if err := yaml.Unmarshal(configData, cfg); err != nil {
			red.Printf("❌ Failed to unmarshal config file %s: %v\n", *configFile, err)
			os.Exit(1)
		}
		green.Printf("✅ Successfully loaded configuration for external cluster.\n")
	}

	// Setup FWI client
	fwiClient, err := setupFWI(cfg, logger)
	if err != nil {
		red.Printf("❌ Failed to setup FWI: %v\n", err)
		os.Exit(1)
	}

	// Create entities and store them in a thread-safe pool
	entityPool := &SafeEntityPool{
		entities: make([]fwi.Entity, 0, *numEntities),
	}

	cyan.Printf("👥 Creating %d entities in parallel (one goroutine per entity)...\n", *numEntities)

	var createWg sync.WaitGroup
	createWg.Add(*numEntities)

	for i := 0; i < *numEntities; i++ {
		go func(entityIndex int) {
			defer createWg.Done()
			entityName := fmt.Sprintf("entity-%d", entityIndex)
			// This inner loop handles retries for a single entity
			for {
				entity, err := fwiClient.CreateOrLoadEntity(context.Background(), entityName, models.Limits{})
				if err == nil {
					entityPool.mu.Lock()
					entityPool.entities = append(entityPool.entities, entity)
					entityPool.mu.Unlock()
					break // Success, break retry loop
				}

				var rateLimitErr *client.ErrRateLimited
				if errors.As(err, &rateLimitErr) {
					yellow.Printf("⏳ [Entity %d] Rate limited. Waiting for %s...\n", entityIndex, rateLimitErr.RetryAfter.String())
					time.Sleep(rateLimitErr.RetryAfter)
					continue // Retry creating the same entity
				}

				// Any other error during initial creation is fatal
				red.Printf("❌ [Entity %d] Failed to create: %v. Aborting.\n", entityIndex, err)
				os.Exit(1)
			}
		}(i)
	}

	createWg.Wait() // Wait for all workers to finish

	green.Printf("✅ Successfully created %d entities\n", len(entityPool.entities))

	if len(entityPool.entities) == 0 {
		red.Println("❌ No entities were created, cannot run stress test.")
		if rt != nil {
			rt.Stop()
			wg.Wait()
		}
		os.Exit(1)
	}

	// Run stress test
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	subscriberMetrics := make([]*SubscriberMetrics, *numSubscribers)
	metricsChan := make(chan StressMetrics, len(entityPool.entities))
	var stressWg sync.WaitGroup

	// Start event subscribers
	if *numSubscribers > 0 {
		cyan.Printf("📡 Starting %d event subscribers...\n", *numSubscribers)
		for i := 0; i < *numSubscribers; i++ {
			stressWg.Add(1)
			entityForSub := entityPool.entities[rand.Intn(len(entityPool.entities))]

			subscriberMetrics[i] = &SubscriberMetrics{
				SubscriberID: i,
				ListeningTo:  entityForSub.GetName(),
			}
			cyan.Printf("📡 Subscriber %d is listening to events from entity %s\n", i, entityForSub.GetName())

			go func(subMetric *SubscriberMetrics) {
				defer stressWg.Done()
				onEvent := func(data any) {
					atomic.AddInt64(&subMetric.EventsReceived, 1)
				}
				// The entity used for the subscription must be the same one we log.
				err := entityForSub.GetEvents().Subscribe(ctx, "stress-topic", onEvent)
				if err != nil && !errors.Is(err, context.Canceled) {
					yellow.Printf("⚠️ Subscriber %d exited with error: %v\n", subMetric.SubscriberID, err)
				}
			}(subscriberMetrics[i])
		}
	}

	entityPool.mu.RLock()
	for _, entity := range entityPool.entities {
		stressWg.Add(1)
		go func(e fwi.Entity) {
			defer stressWg.Done()
			metrics := runEntityStress(ctx, e, logger.With("entity", e.GetName()))
			metricsChan <- metrics
		}(entity)
	}
	entityPool.mu.RUnlock()

	deletedEntities := struct {
		sync.RWMutex
		m map[string]bool
	}{m: make(map[string]bool)}

	// Start the chaos monkey to delete entities, if not disabled
	if !*disableChaos {
		stressWg.Add(1)
		go func() {
			defer stressWg.Done()
			chaosMonkey(ctx, fwiClient, entityPool, &deletedEntities, yellow)
		}()
		cyan.Printf("🚀 Stress test started! Duration: %s, Entities: %d (Chaos Monkey Enabled 🔥)\n", duration.String(), len(entityPool.entities))
	} else {
		cyan.Printf("🚀 Stress test started! Duration: %s, Entities: %d (Chaos Monkey Disabled)\n", duration.String(), len(entityPool.entities))
	}

	// Wait for shutdown signal or test completion
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigChan:
		yellow.Println("\n🚦 Shutdown signal received, stopping stress test...")
		cancel() // stop the stress test
	case <-ctx.Done():
		green.Println("\n🏁 Stress test duration completed.")
	}

	stressWg.Wait()
	close(metricsChan)
	green.Println("✅ Stress test finished.")

	// Collect and print metrics
	allMetrics := make([]StressMetrics, 0, len(entityPool.entities))
	for m := range metricsChan {
		allMetrics = append(allMetrics, m)
	}
	printMetricsSummary(allMetrics, &deletedEntities, subscriberMetrics)

	// Stop the runtime
	if rt != nil {
		yellow.Println("🛑 Shutting down local cluster...")
		rt.Stop()
		wg.Wait()

		// Give a moment for all node-level services to fully release file handles
		// before we wipe the directory, preventing the BadgerDB error on exit.
		time.Sleep(2 * time.Second)

		green.Println("✅ Local cluster shut down successfully.")
	} else {
		green.Println("✅ Stress test on external cluster complete.")
	}
}

func generateClusterConfig(size int, homeDir string) (*config.Cluster, error) {
	nodes := make(map[string]config.Node)
	raftPeers := make([]string, size)

	for i := 0; i < size; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		publicPort := 8280 + i
		privatePort := 9090 + i // Different port for private API
		raftPort := 7070 + i

		nodes[nodeID] = config.Node{
			PublicBinding:  fmt.Sprintf("127.0.0.1:%d", publicPort),
			PrivateBinding: fmt.Sprintf("127.0.0.1:%d", privatePort),
			RaftBinding:    fmt.Sprintf("127.0.0.1:%d", raftPort),
			NodeSecret:     fmt.Sprintf("secret-for-%s", nodeID),
			ClientDomain:   "localhost",
		}
		raftPeers[i] = nodes[nodeID].RaftBinding
	}

	keyPath := filepath.Join(homeDir, "keys")

	return &config.Cluster{
		InstanceSecret:   "stress-test-secret",
		InsidHome:        homeDir,
		ClientSkipVerify: true,
		DefaultLeader:    "node0",
		Nodes:            nodes,
		TLS: config.TLS{
			Cert: filepath.Join(keyPath, "server.crt"),
			Key:  filepath.Join(keyPath, "server.key"),
		},
		Cache: config.Cache{
			StandardTTL: 5 * time.Minute,
			Keys:        1 * time.Hour,
		},
		RootPrefix: "stress-test-root",
		RateLimiters: config.RateLimiters{
			Values:  config.RateLimiterConfig{Limit: 10000, Burst: 20000},
			Cache:   config.RateLimiterConfig{Limit: 10000, Burst: 20000},
			System:  config.RateLimiterConfig{Limit: 50000, Burst: 100000},
			Default: config.RateLimiterConfig{Limit: 10000, Burst: 20000},
			Events:  config.RateLimiterConfig{Limit: 20000, Burst: 40000},
		},
		Sessions: config.SessionsConfig{
			EventChannelSize:         1000,
			WebSocketReadBufferSize:  4096,
			WebSocketWriteBufferSize: 4096,
			MaxConnections:           1000,
		},
		TrustedProxies: []string{"127.0.0.1", "::1"},
		PermittedIPs:   []string{"127.0.0.1", "::1"},
	}, nil
}

func setupFWI(cfg *config.Cluster, logger *slog.Logger) (fwi.FWI, error) {
	endpoints := make([]client.Endpoint, 0, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		endpoints = append(endpoints, client.Endpoint{
			PublicBinding:  node.PublicBinding,
			PrivateBinding: node.PrivateBinding,
			ClientDomain:   node.ClientDomain,
		})
	}

	// Replicate root API key generation from runtime/runtime.go
	h := sha256.New()
	h.Write([]byte(cfg.InstanceSecret))
	rootApiKeyHex := hex.EncodeToString(h.Sum(nil))
	rootApiKey := base64.StdEncoding.EncodeToString([]byte(rootApiKeyHex))

	// Create a root client to initialize FWI
	rootClient, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeRandom,
		Endpoints:      endpoints,
		SkipVerify:     true,
		ApiKey:         rootApiKey,
		Logger:         logger.WithGroup("fwi-root-client"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create initial root client: %w", err)
	}

	return fwi.NewFWI(
		&client.Config{
			ConnectionType: client.ConnectionTypeRandom,
			Endpoints:      endpoints,
			SkipVerify:     true,
			Logger:         logger.WithGroup("fwi-entities"),
		},
		rootClient,
		logger,
	)
}

func runEntityStress(ctx context.Context, entity fwi.Entity, logger *slog.Logger) StressMetrics {
	metrics := StressMetrics{EntityName: entity.GetName()}
	valueStore := entity.GetValueStore()
	cacheStore := entity.GetCacheStore()
	events := entity.GetEvents()
	blobs := entity.GetBlobs()

	knownKeys := make([]string, 0, 100)
	knownBlobKeys := make([]string, 0, 100)
	const keyPoolSize = 100

	for {
		select {
		case <-ctx.Done():
			return metrics
		default:
			// Non-blocking check, fall through to run an operation
		}

		op := rand.Intn(9) // Now 9 operations to include blobs
		key := "stress-key-" + strconv.Itoa(rand.Intn(1000))
		value := "stress-value-" + strconv.Itoa(rand.Intn(100000))

		var err error
		var start time.Time
		var duration time.Duration

		switch op {
		case 0: // Set Value
			start = time.Now()
			err = valueStore.Set(ctx, key, value)
			duration = time.Since(start)
			if err == nil {
				atomic.AddInt64(&metrics.Sets, 1)
				atomic.AddInt64((*int64)(&metrics.SetsTime), int64(duration))
				if len(knownKeys) < keyPoolSize {
					knownKeys = append(knownKeys, key)
				} else {
					knownKeys[rand.Intn(keyPoolSize)] = key
				}
			}
		case 1: // Get Value
			if len(knownKeys) > 0 {
				keyToGet := knownKeys[rand.Intn(len(knownKeys))]
				start = time.Now()
				_, err = valueStore.Get(ctx, keyToGet)
				duration = time.Since(start)
				if err == nil || errors.Is(err, client.ErrKeyNotFound) {
					atomic.AddInt64(&metrics.Gets, 1)
					atomic.AddInt64((*int64)(&metrics.GetsTime), int64(duration))
				}
			}
		case 2: // Set Cache
			start = time.Now()
			err = cacheStore.Set(ctx, key, value)
			duration = time.Since(start)
			if err == nil {
				atomic.AddInt64(&metrics.Caches, 1)
				atomic.AddInt64((*int64)(&metrics.CachesTime), int64(duration))
			}
		case 3: // Publish Event
			start = time.Now()
			err = events.Publish(ctx, "stress-topic", map[string]string{"key": key, "value": value})
			duration = time.Since(start)
			if err == nil {
				atomic.AddInt64(&metrics.Publishes, 1)
				atomic.AddInt64((*int64)(&metrics.PublishesTime), int64(duration))
			}
		case 4: // Bump
			start = time.Now()
			err = entity.Bump(ctx, "stress-counter", 1)
			duration = time.Since(start)
			if err == nil {
				atomic.AddInt64(&metrics.Bumps, 1)
				atomic.AddInt64((*int64)(&metrics.BumpsTime), int64(duration))
			}
		case 5: // Delete Value
			if len(knownKeys) > 0 {
				idx := rand.Intn(len(knownKeys))
				keyToDelete := knownKeys[idx]
				start = time.Now()
				err = valueStore.Delete(ctx, keyToDelete)
				duration = time.Since(start)
				if err == nil {
					atomic.AddInt64(&metrics.Deletes, 1)
					atomic.AddInt64((*int64)(&metrics.DeletesTime), int64(duration))
					knownKeys = append(knownKeys[:idx], knownKeys[idx+1:]...)
				}
			}
		case 6: // Set Blob
			key := "stress-blob-" + strconv.Itoa(rand.Intn(1000))
			blobValue := "blob-data-" + strconv.Itoa(rand.Intn(100000))
			start = time.Now()
			err = blobs.Set(ctx, key, blobValue)
			duration = time.Since(start)
			if err == nil {
				atomic.AddInt64(&metrics.BlobSets, 1)
				atomic.AddInt64((*int64)(&metrics.BlobSetsTime), int64(duration))
				if len(knownBlobKeys) < keyPoolSize {
					knownBlobKeys = append(knownBlobKeys, key)
				} else {
					knownBlobKeys[rand.Intn(keyPoolSize)] = key
				}
			}
		case 7: // Get Blob
			if len(knownBlobKeys) > 0 {
				keyToGet := knownBlobKeys[rand.Intn(len(knownBlobKeys))]
				start = time.Now()
				_, err = blobs.Get(ctx, keyToGet)
				duration = time.Since(start)
				if err == nil || errors.Is(err, client.ErrKeyNotFound) {
					atomic.AddInt64(&metrics.BlobGets, 1)
					atomic.AddInt64((*int64)(&metrics.BlobGetsTime), int64(duration))
				}
			}
		case 8: // Delete Blob
			if len(knownBlobKeys) > 0 {
				idx := rand.Intn(len(knownBlobKeys))
				keyToDelete := knownBlobKeys[idx]
				start = time.Now()
				err = blobs.Delete(ctx, keyToDelete)
				duration = time.Since(start)
				if err == nil {
					atomic.AddInt64(&metrics.BlobDeletes, 1)
					atomic.AddInt64((*int64)(&metrics.BlobDeletesTime), int64(duration))
					knownBlobKeys = append(knownBlobKeys[:idx], knownBlobKeys[idx+1:]...)
				}
			}
		}
		if err != nil && !errors.Is(err, client.ErrKeyNotFound) {
			atomic.AddInt64(&metrics.Failures, 1)
			globalErrorsMu.Lock()
			globalErrors = append(globalErrors, TimedError{
				Timestamp: time.Now(),
				Entity:    entity.GetName(),
				Error:     err,
			})
			globalErrorsMu.Unlock()
		}

		baseSleep := 10 * time.Millisecond
		jitter := time.Duration(rand.Intn(10)) * time.Millisecond // Jitter up to 10ms
		time.Sleep(baseSleep + jitter)
	}
}

func printMetricsSummary(metrics []StressMetrics, deleted *struct {
	sync.RWMutex
	m map[string]bool
}, subscriberMetrics []*SubscriberMetrics) {
	header := color.New(color.FgYellow, color.Bold)
	cyan := color.New(color.FgCyan)
	red := color.New(color.FgRed)
	green := color.New(color.FgGreen)
	bold := color.New(color.Bold)

	header.Println("\n--- 📊 Stress Test Summary ---")
	fmt.Printf("%-15s | %-16s | %-16s | %-16s | %-16s | %-16s | %-16s | %-16s | %-16s | %-16s | %-10s\n", "Entity", "Sets (avg)", "Gets (avg)", "Deletes (avg)", "Caches (avg)", "Publishes (avg)", "Bumps (avg)", "Blob Sets (avg)", "Blob Gets (avg)", "Blob Dels (avg)", "Failures")
	fmt.Println("---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

	var total StressMetrics
	for _, m := range metrics {
		entityName := m.EntityName
		deleted.RLock()
		if _, ok := deleted.m[m.EntityName]; ok {
			entityName = m.EntityName + " 🔥"
		}
		deleted.RUnlock()

		fmt.Printf("%-15s | ", entityName)
		printOpMetric(cyan, m.Sets, m.SetsTime)
		printOpMetric(cyan, m.Gets, m.GetsTime)
		printOpMetric(cyan, m.Deletes, m.DeletesTime)
		printOpMetric(cyan, m.Caches, m.CachesTime)
		printOpMetric(cyan, m.Publishes, m.PublishesTime)
		printOpMetric(cyan, m.Bumps, m.BumpsTime)
		printOpMetric(cyan, m.BlobSets, m.BlobSetsTime)
		printOpMetric(cyan, m.BlobGets, m.BlobGetsTime)
		printOpMetric(cyan, m.BlobDeletes, m.BlobDeletesTime)

		if m.Failures > 0 {
			red.Printf("%-10d\n", m.Failures)
		} else {
			green.Printf("%-10d\n", m.Failures)
		}

		total.Sets += m.Sets
		total.SetsTime += m.SetsTime
		total.Gets += m.Gets
		total.GetsTime += m.GetsTime
		total.Deletes += m.Deletes
		total.DeletesTime += m.DeletesTime
		total.Caches += m.Caches
		total.CachesTime += m.CachesTime
		total.Publishes += m.Publishes
		total.PublishesTime += m.PublishesTime
		total.Bumps += m.Bumps
		total.BumpsTime += m.BumpsTime
		total.BlobSets += m.BlobSets
		total.BlobSetsTime += m.BlobSetsTime
		total.BlobGets += m.BlobGets
		total.BlobGetsTime += m.BlobGetsTime
		total.BlobDeletes += m.BlobDeletes
		total.BlobDeletesTime += m.BlobDeletesTime
		total.Failures += m.Failures
	}

	fmt.Println("---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
	bold.Printf("%-15s | ", "TOTALS")
	printOpMetric(cyan, total.Sets, total.SetsTime)
	printOpMetric(cyan, total.Gets, total.GetsTime)
	printOpMetric(cyan, total.Deletes, total.DeletesTime)
	printOpMetric(cyan, total.Caches, total.CachesTime)
	printOpMetric(cyan, total.Publishes, total.PublishesTime)
	printOpMetric(cyan, total.Bumps, total.BumpsTime)
	printOpMetric(cyan, total.BlobSets, total.BlobSetsTime)
	printOpMetric(cyan, total.BlobGets, total.BlobGetsTime)
	printOpMetric(cyan, total.BlobDeletes, total.BlobDeletesTime)

	if total.Failures > 0 {
		red.Printf("%-10d\n", total.Failures)
	} else {
		green.Printf("%-10d\n", total.Failures)
	}
	fmt.Println()

	header.Println("--- 📡 Subscriber Summary ---")
	var totalEventsReceived int64
	for _, subMetric := range subscriberMetrics {
		if subMetric != nil {
			green.Printf("📡 Subscriber %d (listening to %s) received %d events\n", subMetric.SubscriberID, subMetric.ListeningTo, subMetric.EventsReceived)
			totalEventsReceived += subMetric.EventsReceived
		}
	}
	bold.Printf("📡 Total Events Received by All Subscribers: %d\n", totalEventsReceived)
	fmt.Println()

	if len(globalErrors) > 0 {
		header.Println("--- ❗ Detailed Error Log ---")
		globalErrorsMu.Lock()
		for _, e := range globalErrors {
			red.Printf("[%s] Entity '%s': %v\n", e.Timestamp.Format(time.RFC3339), e.Entity, e.Error)
		}
		globalErrorsMu.Unlock()
		fmt.Println()
	}
}

func printOpMetric(c *color.Color, count int64, totalTime time.Duration) {
	if count == 0 {
		fmt.Printf("%-16s | ", "0 (n/a)")
		return
	}
	avgTime := totalTime / time.Duration(count)
	c.Printf("%-5d (%-8s) | ", count, avgTime)
}

func chaosMonkey(ctx context.Context, fwiClient fwi.FWI, pool *SafeEntityPool, deleted *struct {
	sync.RWMutex
	m map[string]bool
}, yellow *color.Color) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 25% chance to delete an entity
			if rand.Intn(4) == 0 {
				pool.mu.Lock()
				if len(pool.entities) <= 1 { // Don't delete the last entity
					pool.mu.Unlock()
					continue
				}

				// Pick a random entity to delete
				idx := rand.Intn(len(pool.entities))
				entityToDelete := pool.entities[idx]

				// Remove from pool
				pool.entities = append(pool.entities[:idx], pool.entities[idx+1:]...)
				pool.mu.Unlock()

				deleted.Lock()
				deleted.m[entityToDelete.GetName()] = true
				deleted.Unlock()

				// Perform the deletion
				yellow.Printf("🔥 Chaos Monkey is deleting entity: %s\n", entityToDelete.GetName())
				if err := fwiClient.DeleteEntity(ctx, entityToDelete.GetName()); err != nil {
					yellow.Printf("🔥 Chaos Monkey failed to delete entity %s: %v\n", entityToDelete.GetName(), err)
				}
			}
		}
	}
}
