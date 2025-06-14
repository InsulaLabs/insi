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

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: insio <command> [flags]\n")
	fmt.Fprintf(os.Stderr, "   or: insio --script <path_to_script.js> [flags]\n")
	fmt.Fprintf(os.Stderr, "\nCommands:\n")
	fmt.Fprintf(os.Stderr, "  ping\t\tPing the target node to check connectivity.\n")
	fmt.Fprintf(os.Stderr, "  verify\tVerify the API key has connectivity to the target node.\n")
	fmt.Fprintf(os.Stderr, "\nFlags:\n")
	flag.PrintDefaults()
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

func handlePing(c *client.Client, args []string) {
	if len(args) > 0 {
		logger.Error("ping: does not take arguments")
		printUsage()
		os.Exit(1)
	}
	resp, err := c.Ping()
	if err != nil {
		logger.Error("Ping failed", "error", err)
		os.Exit(1)
	}
	fmt.Println("Ping Response:")
	for k, v := range resp {
		fmt.Printf("  %s: %s\n", k, v)
	}
}

func handleVerify(c *client.Client, args []string) {
	if len(args) > 0 {
		logger.Error("verify: does not take arguments")
		printUsage()
		os.Exit(1)
	}
	logger.Info("Attempting to verify API key with a ping...")
	resp, err := c.Ping()
	if err != nil {
		logger.Error("API key verification failed", "error", err)
		os.Exit(1)
	}

	logger.Info("API Key Verified Successfully!")
	fmt.Println("Ping Response:")
	for k, v := range resp {
		fmt.Printf("  %s: %s\n", k, v)
	}
}

func main() {
	flag.Parse()

	args := flag.Args()
	command := ""
	if len(args) > 0 {
		command = args[0]
	}

	if scriptPath == "" && command == "" {
		printUsage()
		os.Exit(1)
	}

	if scriptPath != "" && command != "" {
		logger.Error("Cannot use --script flag with a command")
		printUsage()
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

	if command != "" {
		switch command {
		case "ping":
			handlePing(cli, args[1:])
		case "verify":
			handleVerify(cli, args[1:])
		default:
			logger.Error("Unknown command", "command", command)
			printUsage()
			os.Exit(1)
		}
		return
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
