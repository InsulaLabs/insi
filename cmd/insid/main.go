package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/service"
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
	asNodeId := flag.String("as", "", "Node ID to run as (e.g., node0). Must be defined in the config file.")
	flag.Parse()

	if *asNodeId == "" {
		mainLogger.Error("--as <nodeId> flag is required.")
		os.Exit(1)
	}

	clusterCfg, err := config.LoadConfig(*configFile)
	if err != nil {
		mainLogger.Error("Failed to load configuration", "file", *configFile, "error", err)
		os.Exit(1)
	}

	nodeLogger := mainLogger.With("node", *asNodeId)

	nodeSpecificCfg, ok := clusterCfg.Nodes[*asNodeId]
	if !ok {
		nodeLogger.Error("Node ID not found in configuration file", "available_nodes", getMapKeys(clusterCfg.Nodes))
		os.Exit(1)
	}

	if err := os.MkdirAll(clusterCfg.InsudbDir, os.ModePerm); err != nil {
		nodeLogger.Error("Failed to create insudbDir", "path", clusterCfg.InsudbDir, "error", err)
		os.Exit(1)
	}

	nodeDataRootPath := filepath.Join(clusterCfg.InsudbDir, *asNodeId)
	if err := os.MkdirAll(nodeDataRootPath, os.ModePerm); err != nil {
		nodeLogger.Error("Could not create node data root directory", "path", nodeDataRootPath, "error", err)
		os.Exit(1)
	}

	badge, err := loadOrCreateBadge(*asNodeId, nodeDataRootPath, nodeSpecificCfg.NodeSecret)
	if err != nil {
		nodeLogger.Error("Failed to load or create badge", "error", err)
		os.Exit(1)
	}

	if os.MkdirAll(nodeDataRootPath, os.ModePerm); err != nil {
		nodeLogger.Error("Failed to create node data root directory", "path", nodeDataRootPath, "error", err)
		os.Exit(1)
	}

	nodeDir := filepath.Join(nodeDataRootPath, *asNodeId)
	if err := os.MkdirAll(nodeDir, os.ModePerm); err != nil {
		nodeLogger.Error("Failed to create node data directory", "path", nodeDir, "error", err)
		os.Exit(1)
	}

	kvm, err := tkv.New(tkv.Config{
		Identity:       badge,
		Logger:         nodeLogger,
		Directory:      nodeDir, // actual nodes specific dir
		AppCtx:         appCtx,
		CacheTTL:       clusterCfg.Cache.StandardTTL,
		SecureCacheTTL: clusterCfg.Cache.SecureTTL,
	})
	if err != nil {
		nodeLogger.Error("Failed to create KV manager", "error", err)
		os.Exit(1)
	}
	defer func() {
		kvm.Close()
	}()

	srvc, err := service.NewService(appCtx, nodeLogger, &nodeSpecificCfg, badge, kvm, clusterCfg, *asNodeId)
	if err != nil {
		nodeLogger.Error("Failed to create service", "error", err)
		os.Exit(1)
	}

	srvc.Run()
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
