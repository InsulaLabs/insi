package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	EntityName string
	Sets       int64
	Gets       int64
	Caches     int64
	Publishes  int64
	Bumps      int64
	Failures   int64
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
	cfg, err := generateClusterConfig(*clusterSize, tempDir)
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
	rt, err := runtime.New([]string{"--config", configPath, "--host"}, configPath)
	if err != nil {
		red.Printf("❌ Failed to create runtime: %v\n", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := rt.Run(); err != nil {
			red.Printf("❌ Runtime exited with error: %v\n", err)
		}
	}()

	cyan.Printf("⏳ Waiting for cluster to initialize and for root trackers to be set... (15s)\n")
	time.Sleep(15 * time.Second)

	// Setup FWI client
	fwiClient, err := setupFWI(cfg, logger)
	if err != nil {
		red.Printf("❌ Failed to setup FWI: %v\n", err)
		os.Exit(1)
	}

	// Create entities
	entities := make([]fwi.Entity, 0, *numEntities)
	for i := 0; i < *numEntities; i++ {
		entityName := fmt.Sprintf("entity-%d", i)
		entity, err := fwiClient.CreateOrLoadEntity(context.Background(), entityName, models.Limits{})
		if err != nil {
			yellow.Printf("⚠️ Failed to create entity %s: %v\n", entityName, err)
			continue
		}
		entities = append(entities, entity)
	}
	green.Printf("✅ Successfully created %d entities\n", len(entities))

	if len(entities) == 0 {
		red.Println("❌ No entities were created, cannot run stress test.")
		rt.Stop()
		wg.Wait()
		os.Exit(1)
	}

	// Run stress test
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	metricsChan := make(chan StressMetrics, len(entities))
	var stressWg sync.WaitGroup
	for _, entity := range entities {
		stressWg.Add(1)
		go func(e fwi.Entity) {
			defer stressWg.Done()
			metrics := runEntityStress(ctx, e, logger.With("entity", e.GetName()))
			metricsChan <- metrics
		}(entity)
	}
	cyan.Printf("🚀 Stress test started! Duration: %s, Entities: %d\n", duration.String(), len(entities))

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
	allMetrics := make([]StressMetrics, 0, len(entities))
	for m := range metricsChan {
		allMetrics = append(allMetrics, m)
	}
	printMetricsSummary(allMetrics)

	// Stop the runtime
	yellow.Println("🛑 Shutting down cluster...")
	rt.Stop()
	wg.Wait()
	green.Println("✅ Cluster shut down successfully.")
}

func generateClusterConfig(size int, homeDir string) (*config.Cluster, error) {
	nodes := make(map[string]config.Node)
	raftPeers := make([]string, size)

	for i := 0; i < size; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		httpPort := 8080 + i
		raftPort := 7070 + i

		nodes[nodeID] = config.Node{
			HttpBinding:  fmt.Sprintf("127.0.0.1:%d", httpPort),
			RaftBinding:  fmt.Sprintf("127.0.0.1:%d", raftPort),
			NodeSecret:   fmt.Sprintf("secret-for-%s", nodeID),
			ClientDomain: "localhost",
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
			Values:  config.RateLimiterConfig{Limit: 1000, Burst: 2000},
			Cache:   config.RateLimiterConfig{Limit: 1000, Burst: 2000},
			System:  config.RateLimiterConfig{Limit: 500, Burst: 1000},
			Default: config.RateLimiterConfig{Limit: 1000, Burst: 2000},
			Events:  config.RateLimiterConfig{Limit: 2000, Burst: 4000},
		},
		Sessions: config.SessionsConfig{
			EventChannelSize:         1000,
			WebSocketReadBufferSize:  4096,
			WebSocketWriteBufferSize: 4096,
			MaxConnections:           1000,
		},
	}, nil
}

func setupFWI(cfg *config.Cluster, logger *slog.Logger) (fwi.FWI, error) {
	endpoints := make([]client.Endpoint, 0, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		endpoints = append(endpoints, client.Endpoint{
			HostPort:     node.HttpBinding,
			ClientDomain: node.ClientDomain,
		})
	}

	// Replicate root API key generation from runtime/runtime.go
	h := sha256.New()
	h.Write([]byte(cfg.InstanceSecret))
	rootApiKeyHex := hex.EncodeToString(h.Sum(nil))
	rootApiKey := base64.StdEncoding.EncodeToString([]byte(rootApiKeyHex))

	// Create a root client to initialize FWI
	rootClient, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeDirect,
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
			ConnectionType: client.ConnectionTypeDirect,
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
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return metrics
		case <-ticker.C:
			op := rand.Intn(5)
			key := "stress-key-" + strconv.Itoa(rand.Intn(1000))
			value := "stress-value-" + strconv.Itoa(rand.Intn(100000))

			var err error
			switch op {
			case 0: // Set Value
				err = valueStore.Set(key, value)
				if err == nil {
					atomic.AddInt64(&metrics.Sets, 1)
				}
			case 1: // Get Value
				_, err = valueStore.Get(key)
				if err == nil || err == client.ErrKeyNotFound {
					atomic.AddInt64(&metrics.Gets, 1)
				}
			case 2: // Set Cache
				err = cacheStore.Set(key, value)
				if err == nil {
					atomic.AddInt64(&metrics.Caches, 1)
				}
			case 3: // Publish Event
				err = events.Publish("stress-topic", map[string]string{"key": key, "value": value})
				if err == nil {
					atomic.AddInt64(&metrics.Publishes, 1)
				}
			case 4: // Bump
				err = entity.Bump("stress-counter", 1)
				if err == nil {
					atomic.AddInt64(&metrics.Bumps, 1)
				}
			}
			if err != nil && err != client.ErrKeyNotFound {
				atomic.AddInt64(&metrics.Failures, 1)
				// Optional: log the specific failure
				// logger.Warn("Stress operation failed", "op", op, "error", err)
			}
		}
	}
}

func printMetricsSummary(metrics []StressMetrics) {
	header := color.New(color.FgYellow, color.Bold)
	cyan := color.New(color.FgCyan)
	red := color.New(color.FgRed)
	green := color.New(color.FgGreen)
	bold := color.New(color.Bold)

	header.Println("\n--- 📊 Stress Test Summary ---")
	fmt.Printf("%-12s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n", "Entity", "Sets", "Gets", "Caches", "Publishes", "Bumps", "Failures")
	fmt.Println("---------------------------------------------------------------------------------------")

	var total StressMetrics
	for _, m := range metrics {
		fmt.Printf("%-12s | ", m.EntityName)
		cyan.Printf("%-10d", m.Sets)
		fmt.Print(" | ")
		cyan.Printf("%-10d", m.Gets)
		fmt.Print(" | ")
		cyan.Printf("%-10d", m.Caches)
		fmt.Print(" | ")
		cyan.Printf("%-10d", m.Publishes)
		fmt.Print(" | ")
		cyan.Printf("%-10d", m.Bumps)
		fmt.Print(" | ")
		if m.Failures > 0 {
			red.Printf("%-10d\n", m.Failures)
		} else {
			green.Printf("%-10d\n", m.Failures)
		}
		total.Sets += m.Sets
		total.Gets += m.Gets
		total.Caches += m.Caches
		total.Publishes += m.Publishes
		total.Bumps += m.Bumps
		total.Failures += m.Failures
	}

	fmt.Println("---------------------------------------------------------------------------------------")
	bold.Printf("%-12s | ", "TOTALS")
	cyan.Printf("%-10d", total.Sets)
	fmt.Print(" | ")
	cyan.Printf("%-10d", total.Gets)
	fmt.Print(" | ")
	cyan.Printf("%-10d", total.Caches)
	fmt.Print(" | ")
	cyan.Printf("%-10d", total.Publishes)
	fmt.Print(" | ")
	cyan.Printf("%-10d", total.Bumps)
	fmt.Print(" | ")
	if total.Failures > 0 {
		red.Printf("%-10d\n", total.Failures)
	} else {
		green.Printf("%-10d\n", total.Failures)
	}
	fmt.Println()
}
