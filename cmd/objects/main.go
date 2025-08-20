package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/fatih/color"
	"gopkg.in/yaml.v3"
)

var (
	logger         *slog.Logger
	configPath     string
	clusterCfg     *config.Cluster
	targetNode     string
	useRootKey     bool
	publicAddress  string
	privateAddress string
	maxSize        int
	useCache       bool
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
	flag.StringVar(&publicAddress, "public", "", "Public address of the node to connect to. Defaults to INSI_PUBLIC_ADDRESS environment variable.")
	flag.StringVar(&privateAddress, "private", "", "Private address of the node to connect to. Defaults to INSI_PRIVATE_ADDRESS environment variable.")
	flag.IntVar(&maxSize, "max-size", 1000, "Maximum number of entries to load for the object map")
	flag.BoolVar(&useCache, "cache", false, "Use cache endpoints instead of values endpoints. Defaults to false (uses values).")
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

func processAddress(addr string) string {
	if strings.HasPrefix(addr, "https://") {
		addr = strings.TrimPrefix(addr, "https://")
	} else if strings.HasPrefix(addr, "http://") {
		addr = strings.TrimPrefix(addr, "http://")
	}

	if !strings.Contains(addr, ":") {
		addr = addr + ":443"
	}

	return addr
}

func getClientNoConfig() (*client.Client, error) {
	pubAddr := publicAddress
	if pubAddr == "" {
		pubAddr = os.Getenv("INSI_PUBLIC_ADDRESS")
	}

	privAddr := privateAddress
	if privAddr == "" {
		privAddr = os.Getenv("INSI_PRIVATE_ADDRESS")
	}

	if pubAddr != "" && privAddr == "" {
		privAddr = pubAddr
	} else if privAddr != "" && pubAddr == "" {
		pubAddr = privAddr
	} else if pubAddr == "" && privAddr == "" {
		return nil, fmt.Errorf("no address provided: use --public, --private, or set INSI_PUBLIC_ADDRESS/INSI_PRIVATE_ADDRESS environment variables")
	}

	pubAddr = processAddress(pubAddr)
	privAddr = processAddress(privAddr)

	apiKey := os.Getenv("INSI_API_KEY")
	if apiKey == "" && !useRootKey {
		return nil, fmt.Errorf("INSI_API_KEY environment variable is not set and --root flag is not provided")
	}

	if useRootKey {
		instanceSecret := os.Getenv("INSI_INSTANCE_SECRET")
		if instanceSecret == "" {
			return nil, fmt.Errorf("INSI_INSTANCE_SECRET environment variable is required when using --root without config")
		}
		secretHash := sha256.New()
		secretHash.Write([]byte(instanceSecret))
		apiKey = hex.EncodeToString(secretHash.Sum(nil))
		apiKey = base64.StdEncoding.EncodeToString([]byte(apiKey))
	}

	extractDomain := func(addr string) string {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return addr
		}
		return host
	}

	clientDomain := extractDomain(pubAddr)

	skipVerify := false
	if skipVerifyEnv := os.Getenv("INSI_SKIP_VERIFY"); skipVerifyEnv == "true" {
		skipVerify = true
	}

	clientLogger := logger.WithGroup("client")

	c, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeDirect,
		Endpoints: []client.Endpoint{
			{
				PublicBinding:  pubAddr,
				PrivateBinding: privAddr,
				ClientDomain:   clientDomain,
			},
		},
		ApiKey:     apiKey,
		SkipVerify: skipVerify,
		Logger:     clientLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	return c, nil
}

func getClient(cfg *config.Cluster, targetNodeID string) (*client.Client, error) {
	nodeToConnect := targetNodeID
	if nodeToConnect == "" {
		if cfg.DefaultLeader == "" {
			return nil, fmt.Errorf("targetNodeID is empty and no DefaultLeader is set in config")
		}
		nodeToConnect = cfg.DefaultLeader
		logger.Debug("No target node specified, using DefaultLeader", "node_id", color.CyanString(nodeToConnect))
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
				PublicBinding:  nodeDetails.PublicBinding,
				PrivateBinding: nodeDetails.PrivateBinding,
				ClientDomain:   nodeDetails.ClientDomain,
			},
		},
		ApiKey:     apiKey,
		SkipVerify: cfg.ClientSkipVerify,
		Logger:     clientLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client for node %s (%s): %w", nodeToConnect, nodeDetails.PublicBinding, err)
	}
	return c, nil
}

func main() {
	flag.Parse()

	configExplicitlyProvided := configPath != "cluster.yaml"
	addressProvided := publicAddress != "" || privateAddress != "" ||
		os.Getenv("INSI_PUBLIC_ADDRESS") != "" || os.Getenv("INSI_PRIVATE_ADDRESS") != ""
	if configExplicitlyProvided && addressProvided {
		logger.Error("Cannot use both --config flag and address flags/env vars simultaneously")
		fmt.Fprintf(os.Stderr, "%s Cannot use --config flag with --public/--private or INSI_PUBLIC_ADDRESS/INSI_PRIVATE_ADDRESS\n", color.RedString("Error:"))
		os.Exit(1)
	}

	var err error
	var cli *client.Client
	var useConfigMode bool

	if addressProvided {
		useConfigMode = false
	} else {
		useConfigMode = true
		clusterCfg, err = loadConfig(configPath)
		if err != nil {
			logger.Error("Failed to load cluster configuration", "error", err)
			os.Exit(1)
		}
	}

	args := flag.Args()
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}

	prefix := args[0]
	command := args[1]
	cmdArgs := args[2:]

	if prefix == "" {
		logger.Error("Prefix cannot be empty")
		printUsage()
		os.Exit(1)
	}

	if useConfigMode {
		cli, err = getClient(clusterCfg, targetNode)
		if err != nil {
			logger.Error("Failed to initialize API client from config", "error", err)
			os.Exit(1)
		}
	} else {
		cli, err = getClientNoConfig()
		if err != nil {
			logger.Error("Failed to initialize API client without config", "error", err)
			os.Exit(1)
		}
	}

	ctx := context.Background()
	objectLogger := logger.WithGroup("object").WithGroup(prefix)

	remoteMap, err := client.RemoteMapFromPrefix(ctx, prefix, objectLogger, cli, maxSize, useCache)
	if err != nil {
		logger.Error("Failed to create remote map", "prefix", prefix, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	switch command {
	case "get":
		handleGet(remoteMap, cmdArgs)
	case "set":
		handleSet(remoteMap, cmdArgs)
	case "set-unique":
		handleSetUnique(remoteMap, cmdArgs)
	case "delete":
		handleDelete(remoteMap, cmdArgs)
	case "list":
		handleList(remoteMap, cmdArgs)
	case "sync":
		handleSync(remoteMap, cmdArgs)
	default:
		logger.Error("Unknown command", "command", command, "prefix", prefix)
		fmt.Fprintf(os.Stderr, "%s Unknown command '%s' for prefix '%s'\n", color.RedString("Error:"), color.CyanString(command), color.CyanString(prefix))
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: objects [flags] <prefix> <command> [args...]\n")
	fmt.Fprintf(os.Stderr, "\nConnection Modes:\n")
	fmt.Fprintf(os.Stderr, "  Config mode:   Use --config with a cluster.yaml file (default)\n")
	fmt.Fprintf(os.Stderr, "  Direct mode:   Use --public/--private or env vars (no config file)\n")
	fmt.Fprintf(os.Stderr, "\nEnvironment Variables:\n")
	fmt.Fprintf(os.Stderr, "  INSI_API_KEY          API key for authentication\n")
	fmt.Fprintf(os.Stderr, "  INSI_PUBLIC_ADDRESS   Public address (e.g., red.insulalabs.io or https://red.insulalabs.io:443)\n")
	fmt.Fprintf(os.Stderr, "  INSI_PRIVATE_ADDRESS  Private address (defaults to public if not set)\n")
	fmt.Fprintf(os.Stderr, "  INSI_INSTANCE_SECRET  Instance secret (required for --root without config)\n")
	fmt.Fprintf(os.Stderr, "  INSI_SKIP_VERIFY      Set to 'true' to skip TLS verification\n")
	fmt.Fprintf(os.Stderr, "\nFlags:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\n%s: Cannot use --config together with --public/--private\n", color.YellowString("Note"))
	fmt.Fprintf(os.Stderr, "\nCommands:\n")
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("get"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("set"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("set-unique"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("list"), color.CyanString("[with-prefix]"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("sync"), color.CyanString("[quick]"))
}

func handleGet(rm *client.RemoteMap, args []string) {
	if len(args) != 1 {
		logger.Error("get: requires <key>")
		printUsage()
		os.Exit(1)
	}
	key := args[0]
	value, err := rm.GetEntry(key)
	if err != nil {
		if errors.Is(err, client.ErrFieldNotFound) {
			fmt.Fprintf(os.Stderr, "%s Key '%s' not found.\n", color.RedString("Error:"), color.CyanString(key))
			os.Exit(1)
		} else {
			logger.Error("Get failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	fmt.Println(value)
}

func handleSet(rm *client.RemoteMap, args []string) {
	if len(args) != 2 {
		logger.Error("set: requires <key> <value>")
		printUsage()
		os.Exit(1)
	}
	key, value := args[0], args[1]
	err := rm.UpdateExistingEntry(key, value)
	if err != nil {
		if errors.Is(err, client.ErrFieldNotFound) {
			fmt.Fprintf(os.Stderr, "%s Key '%s' not found. Use 'set-unique' to create new entries.\n", color.RedString("Error:"), color.CyanString(key))
		} else {
			logger.Error("Set failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleSetUnique(rm *client.RemoteMap, args []string) {
	if len(args) != 2 {
		logger.Error("set-unique: requires <key> <value>")
		printUsage()
		os.Exit(1)
	}
	key, value := args[0], args[1]
	err := rm.CreateUniqueEntry(key, value)
	if err != nil {
		if errors.Is(err, client.ErrConflict) {
			fmt.Fprintf(os.Stderr, "%s Key '%s' already exists.\n", color.RedString("Conflict:"), color.CyanString(key))
		} else {
			logger.Error("Set-unique failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleDelete(rm *client.RemoteMap, args []string) {
	if len(args) != 1 {
		logger.Error("delete: requires <key>")
		printUsage()
		os.Exit(1)
	}
	key := args[0]
	err := rm.DeleteExistingEntry(key)
	if err != nil {
		if errors.Is(err, client.ErrFieldNotFound) {
			fmt.Fprintf(os.Stderr, "%s Key '%s' not found.\n", color.RedString("Error:"), color.CyanString(key))
		} else {
			logger.Error("Delete failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleList(rm *client.RemoteMap, args []string) {
	withPrefix := false
	if len(args) > 0 && args[0] == "with-prefix" {
		withPrefix = true
	}

	entries := rm.GetAllEntries(withPrefix)
	if len(entries) == 0 {
		color.HiYellow("No entries found.")
		return
	}

	for key, value := range entries {
		fmt.Printf("%s: %s\n", color.CyanString(key), value)
	}
}

func handleSync(rm *client.RemoteMap, args []string) {
	quick := false
	if len(args) > 0 && args[0] == "quick" {
		quick = true
	}

	ctx := context.Background()
	err := rm.SyncPull(ctx, quick)
	if err != nil {
		logger.Error("Sync failed", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	color.HiGreen("OK")
}
