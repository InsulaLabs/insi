package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/InsulaLabs/insi/internal/config"
	"github.com/InsulaLabs/insi/internal/service"
	"github.com/InsulaLabs/insula/security/badge"
	"github.com/InsulaLabs/insula/tkv"
)

var appCtx context.Context
var appCancel context.CancelFunc

func main() {

	appCtx, appCancel = context.WithCancel(context.Background())

	mainLogger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", "insudb")

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		mainLogger.Info("Received signal", "signal", sig)
		appCancel()
	}()

	configFile := flag.String("config", "cluster.yaml", "Path to the cluster configuration file.")
	asNodeId := flag.String("as", "", "Node ID to run as (e.g., node0). Mutually exclusive with --host.")
	hostMode := flag.Bool("host", false, "Run instances for all nodes in the config. Mutually exclusive with --as.")
	flag.Parse()

	if (*asNodeId == "" && !*hostMode) || (*asNodeId != "" && *hostMode) {
		mainLogger.Error("Either --as <nodeId> or --host must be specified, but not both.")
		flag.Usage()
		os.Exit(1)
	}

	clusterCfg, err := config.LoadConfig(*configFile)
	if err != nil {
		mainLogger.Error("Failed to load configuration", "file", *configFile, "error", err)
		os.Exit(1)
	}

	if *hostMode {
		if len(clusterCfg.Nodes) == 0 {
			mainLogger.Error("No nodes defined in the configuration file for host mode.")
			os.Exit(1)
		}
		mainLogger.Info("Running in --host mode. Starting instances for all configured nodes.", "count", len(clusterCfg.Nodes))

		var wg sync.WaitGroup
		for nodeId, nodeCfg := range clusterCfg.Nodes {
			wg.Add(1)
			go func(id string, cfg config.Node) {
				defer wg.Done()
				startNodeInstance(appCtx, mainLogger, clusterCfg, id, cfg)
			}(nodeId, nodeCfg)
		}

		go func() {
			wg.Wait()
			mainLogger.Info("All node instances have completed.")
		}()

		<-appCtx.Done()
		mainLogger.Info("Shutdown signal received or all services completed. Exiting host mode.")
		return
	}

	// Logic for single node startup (using --as)
	nodeSpecificCfg, ok := clusterCfg.Nodes[*asNodeId]
	if !ok {
		mainLogger.Error("Node ID not found in configuration file", "node", *asNodeId, "available_nodes", getMapKeys(clusterCfg.Nodes))
		os.Exit(1)
	}
	startNodeInstance(appCtx, mainLogger, clusterCfg, *asNodeId, nodeSpecificCfg)

	// Wait for the application context to be canceled (e.g., by a signal) for the single node case
	<-appCtx.Done()
	mainLogger.Info("Node service shutting down or completed for single node mode.")
}

// startNodeInstance sets up and runs a single node instance.
func startNodeInstance(ctx context.Context, logger *slog.Logger, clusterCfg *config.Cluster, nodeId string, nodeCfg config.Node) {
	nodeLogger := logger.With("node", nodeId)
	nodeLogger.Info("Starting node instance")

	if err := os.MkdirAll(clusterCfg.InsudbDir, os.ModePerm); err != nil {
		nodeLogger.Error("Failed to create insudbDir", "path", clusterCfg.InsudbDir, "error", err)
		os.Exit(1)
	}

	nodeDataRootPath := filepath.Join(clusterCfg.InsudbDir, nodeId)
	if err := os.MkdirAll(nodeDataRootPath, os.ModePerm); err != nil {
		nodeLogger.Error("Could not create node data root directory", "path", nodeDataRootPath, "error", err)
		os.Exit(1)
	}

	b, err := loadOrCreateBadge(nodeId, nodeDataRootPath, nodeCfg.NodeSecret)
	if err != nil {
		nodeLogger.Error("Failed to load or create badge", "error", err)
		os.Exit(1)
	}

	// This mkdirAll seems redundant given the one for nodeDataRootPath,
	// but keeping it as per original logic for now.
	// The original path was: nodeDir := filepath.Join(nodeDataRootPath, *asNodeId)
	// which now becomes:
	nodeDir := filepath.Join(nodeDataRootPath, nodeId) // Corrected to use nodeId
	if err := os.MkdirAll(nodeDir, os.ModePerm); err != nil {
		nodeLogger.Error("Failed to create node specific data directory", "path", nodeDir, "error", err)
		return
	}

	kvm, err := tkv.New(tkv.Config{
		Identity:  b,
		Logger:    nodeLogger.WithGroup("tkv"), // Deriving logger for TKV
		Directory: nodeDir,
		AppCtx:    ctx, // Pass the instance-specific context if available, or appCtx
		CacheTTL:  clusterCfg.Cache.StandardTTL,
	})
	if err != nil {
		nodeLogger.Error("Failed to create KV manager", "error", err)
		return
	}
	defer kvm.Close()

	srvc, err := service.NewService(ctx, nodeLogger.WithGroup("service"), &nodeCfg, b, kvm, clusterCfg, nodeId)
	if err != nil {
		nodeLogger.Error("Failed to create service", "error", err)
		return
	}

	// Run the service. This should block until the service is done or context is cancelled.
	srvc.Run()

	nodeLogger.Info("Node instance shut down gracefully.")
}

func getMapKeys(m map[string]config.Node) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func loadOrCreateBadge(nodeId string, installDir string, secret string) (badge.Badge, error) {

	fileName := filepath.Join(installDir,
		fmt.Sprintf("%s.identity.encrypted", nodeId),
	)

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		badge, err := badge.BuildBadge(
			badge.WithID(nodeId),
			badge.WithCurveSelector(badge.BadgeCurveSelectorP256),
		)
		if err != nil {
			return nil, err
		}

		encryptedBadge, err := badge.EncryptBadge([]byte(secret))
		if err != nil {
			return nil, err
		}

		os.WriteFile(fileName, encryptedBadge, 0644)
		return badge, nil
	}

	rawBadge, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	badge, err := badge.FromEncryptedBadge([]byte(secret), rawBadge)
	if err != nil {
		return nil, err
	}
	return badge, nil
}
