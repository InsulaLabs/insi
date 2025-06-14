package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/ovm"
	"gopkg.in/yaml.v3"
)

var (
	logger     *slog.Logger
	configPath string
	clusterCfg *config.Cluster
	targetNode string
	useRootKey bool
	scriptPath string
)

func init() {
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(os.Stderr, logOpts)
	logger = slog.New(handler)

	flag.StringVar(&configPath, "config", "cluster.yaml", "Path to the cluster configuration file")
	flag.StringVar(&targetNode, "target", "", "Target node ID (e.g., node0, node1). Defaults to DefaultLeader in config.")
	flag.BoolVar(&useRootKey, "root", false, "Use the root key for the cluster. Defaults to false.")
	flag.StringVar(&scriptPath, "script", "", "Path to the OVM script to execute.")
}

func loadConfig(path string) (*config.Cluster, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg config.Cluster
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config data from %s: %w", path, err)
	}
	return &cfg, nil
}

func getClient(cfg *config.Cluster, targetNodeID string) (*client.Client, error) {
	nodeToConnect := targetNodeID
	if nodeToConnect == "" {
		if cfg.DefaultLeader == "" {
			return nil, fmt.Errorf("targetNodeID is empty and no DefaultLeader is set in config")
		}
		nodeToConnect = cfg.DefaultLeader
		logger.Debug("No target node specified, using DefaultLeader", "node_id", nodeToConnect)
	}

	nodeDetails, ok := cfg.Nodes[nodeToConnect]
	if !ok {
		return nil, fmt.Errorf("node ID '%s' not found in configuration", nodeToConnect)
	}

	clientLogger := logger.WithGroup("client")

	var apiKey string
	if useRootKey {
		if cfg.InstanceSecret == "" {
			return nil, fmt.Errorf("InstanceSecret is not defined in the cluster configuration, cannot generate root API key")
		}
		secretHash := sha256.New()
		secretHash.Write([]byte(cfg.InstanceSecret))
		apiKey = hex.EncodeToString(secretHash.Sum(nil))
		apiKey = base64.StdEncoding.EncodeToString([]byte(apiKey))
	} else {
		apiKey = os.Getenv("INSI_API_KEY")
	}

	if apiKey == "" {
		return nil, fmt.Errorf("apiKey is empty and --root flag is not set")
	}

	c, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeDirect,
		Endpoints: []client.Endpoint{
			{
				HostPort:     nodeDetails.HttpBinding,
				ClientDomain: nodeDetails.ClientDomain,
			},
		},
		ApiKey:     apiKey,
		SkipVerify: cfg.ClientSkipVerify,
		Logger:     clientLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client for node %s (%s): %w", nodeToConnect, nodeDetails.HttpBinding, err)
	}
	return c, nil
}

func main() {
	flag.Parse()

	if scriptPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: insio --script <path_to_script.js> [flags]\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	var err error
	clusterCfg, err = loadConfig(configPath)
	if err != nil {
		logger.Error("Failed to load cluster configuration", "error", err)
		os.Exit(1)
	}

	cli, err := getClient(clusterCfg, targetNode)
	if err != nil {
		logger.Error("Failed to initialize API client", "error", err)
		os.Exit(1)
	}

	scriptContent, err := os.ReadFile(scriptPath)
	if err != nil {
		logger.Error("Failed to read script file", "path", scriptPath, "error", err)
		os.Exit(1)
	}

	ovmLogger := logger.WithGroup("ovm")
	vm, err := ovm.New(&ovm.Config{
		Logger:          ovmLogger,
		SetupCtx:        context.Background(),
		InsiClient:      cli,
		DoAddApiKeyMgmt: true,
		DoAddConsole:    true,
		DoAddOS:         true,
	})
	if err != nil {
		logger.Error("Failed to initialize OVM", "error", err)
		os.Exit(1)
	}

	logger.Info("Executing script", "path", scriptPath)
	if err := vm.Execute(context.Background(), string(scriptContent)); err != nil {
		logger.Error("Script execution failed", "error", err)
		os.Exit(1)
	}

	logger.Info("Script executed successfully")
}
