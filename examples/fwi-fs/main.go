// This example demonstrates how to set up and use the FWI VFS (Virtual File System).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/fwi"
	"github.com/InsulaLabs/insi/runtime"
	"gopkg.in/yaml.v3"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("starting FWI VFS example")

	tempDir, err := os.MkdirTemp("", "insi-vfs-example-*")
	if err != nil {
		logger.Error("failed to create temp dir", "error", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tempDir)
	logger.Info("created temporary directory for cluster data", "path", tempDir)

	cfg, err := generateClusterConfig(tempDir)
	if err != nil {
		logger.Error("failed to generate cluster config", "error", err)
		os.Exit(1)
	}

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

	rt, err := runtime.New([]string{"--config", configPath, "--host"}, configPath)
	if err != nil {
		logger.Error("failed to create runtime", "error", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := rt.Run(); err != nil {
			logger.Error("runtime exited with error", "error", err)
		}
	}()

	logger.Info("waiting for cluster to initialize...")
	time.Sleep(5 * time.Second)

	fwiInstance, err := setupFWI(cfg, logger)
	if err != nil {
		logger.Error("failed to setup FWI", "error", err)
		rt.Stop()
		wg.Wait()
		os.Exit(1)
	}
	logger.Info("FWI instance created successfully")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("shutdown signal received, cleaning up.")
		cancel()
	}()

	// --- FWI VFS LOGIC ---
	entityName := "vfs-app"
	logger.Info("creating or loading entity", "name", entityName)
	appEntity, err := fwiInstance.CreateOrLoadEntity(ctx, entityName, models.Limits{})
	if err != nil {
		logger.Error("failed to create or load entity", "error", err)
		rt.Stop()
		wg.Wait()
		os.Exit(1)
	}
	logger.Info("entity ready", "name", appEntity.GetName())

	// Get the VFS for the entity.
	fs := appEntity.GetFS()

	// 1. Create a directory structure.
	logger.Info("creating directory /home/bosley")
	if err := fs.Mkdir(ctx, "/home"); err != nil {
		logger.Error("failed to create dir /home", "error", err)
	}
	if err := fs.Mkdir(ctx, "/home/bosley"); err != nil {
		logger.Error("failed to create dir /home/bosley", "error", err)
	}

	// 2. Create and write a file.
	filePath := "/home/bosley/readme.txt"
	logger.Info("creating and writing to file", "path", filePath)
	file, err := fs.Create(ctx, filePath)
	if err != nil {
		logger.Error("failed to create file", "error", err)
	} else {
		_, _ = file.Write([]byte("Hello, Virtual File System!"))
		if err := file.Close(); err != nil { // Close flushes the write.
			logger.Error("failed to close (write) file", "error", err)
		}
	}

	// 3. Read the file back.
	logger.Info("reading file back", "path", filePath)
	file, err = fs.Open(ctx, filePath)
	if err != nil {
		logger.Error("failed to open file", "error", err)
	} else {
		content, err := io.ReadAll(file)
		if err != nil {
			logger.Error("failed to read file content", "error", err)
		} else {
			logger.Info("file content retrieved successfully", "content", string(content))
		}
		_ = file.Close()
	}

	// 4. List directory contents.
	logger.Info("listing contents of /home/bosley")
	entries, err := fs.ReadDir(ctx, "/home/bosley")
	if err != nil {
		logger.Error("failed to read directory", "error", err)
	} else {
		for _, entry := range entries {
			logger.Info("found entry", "name", entry.Name(), "isDir", entry.IsDir(), "size", entry.Size())
		}
	}

	// 5. Demonstrate recursive remove
	logger.Info("--- Demonstrating Recursive Remove ---")
	// Create a nested structure
	nestedDir := "/home/bosley/subdir"
	nestedFile := path.Join(nestedDir, "deleteme.txt")
	logger.Info("creating nested structure for deletion test")
	_ = fs.Mkdir(ctx, nestedDir)
	f, _ := fs.Create(ctx, nestedFile)
	_, _ = f.Write([]byte("this file is temporary"))
	_ = f.Close()

	// Attempt to remove non-empty parent directory (should fail)
	logger.Info("attempting to remove non-empty directory /home/bosley (should fail)")
	if err := fs.Remove(ctx, "/home/bosley"); err != nil {
		logger.Info("received expected error", "error", err)
	} else {
		logger.Error("expected an error but got none, directory was deleted")
	}

	// Attempt to remove with recursive flag (should succeed)
	logger.Info("attempting to remove /home/bosley with recursive flag (should succeed)")
	if err := fs.Remove(ctx, "/home/bosley", fwi.WithRecursiveRemove()); err != nil {
		logger.Error("failed to remove directory recursively", "error", err)
	} else {
		logger.Info("successfully removed directory and its contents")
	}

	// 6. Clean up: remove remaining top-level directory.
	logger.Info("cleaning up VFS entries")
	if err := fs.Remove(ctx, "/home", fwi.WithRecursiveRemove()); err != nil {
		logger.Error("failed to remove directory", "error", err)
	}

	// --- SHUTDOWN ---
	logger.Info("example finished, initiating shutdown.")
	cancel()
	rt.Stop()
	wg.Wait()
	logger.Info("cluster shut down successfully.")
}

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
		InstanceSecret:   "fwi-vfs-example-secret",
		InsidHome:        homeDir,
		ClientSkipVerify: true,
		DefaultLeader:    nodeID,
		Nodes:            nodes,
		PermittedIPs:     []string{"127.0.0.1"},
		TLS: config.TLS{
			Cert: filepath.Join(keyPath, "server.crt"),
			Key:  filepath.Join(keyPath, "server.key"),
		},
		Cache: config.Cache{
			StandardTTL: 5 * time.Minute,
			Keys:        1 * time.Hour,
		},
		RootPrefix: "fwi-vfs-example-root",
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
		Logger:         logger.WithGroup("fwi-root-client"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create initial root client: %w", err)
	}

	logger.Info("setting limits for the root API key to allow entity creation")
	rootLimits := models.Limits{
		BytesOnDisk:   new(int64),
		BytesInMemory: new(int64),
		EventsEmitted: new(int64),
		Subscribers:   new(int64),
	}
	*rootLimits.BytesOnDisk = 10 * 1024 * 1024 * 1024
	*rootLimits.BytesInMemory = 10 * 1024 * 1024 * 1024
	*rootLimits.EventsEmitted = 1000000
	*rootLimits.Subscribers = 10000

	if err := rootClient.SetLimits(rootApiKey, rootLimits); err != nil {
		return nil, fmt.Errorf("failed to set limits on root api key: %w", err)
	}
	logger.Info("successfully set limits for the root API key")

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
