package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/db/models"
	"github.com/fatih/color"
	"gopkg.in/yaml.v3"
)

var (
	logger         *slog.Logger
	configPath     string
	clusterCfg     *config.Cluster
	targetNode     string // --target flag
	useRootKey     bool   // --root flag
	publicAddress  string // --public flag
	privateAddress string // --private flag
)

func init() {
	// Initialize logger
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(os.Stderr, logOpts)
	logger = slog.New(handler)

	flag.StringVar(&configPath, "config", "cluster.yaml", "Path to the cluster configuration file")
	flag.StringVar(&targetNode, "target", "", "Target node ID (e.g., node0, node1). Defaults to DefaultLeader in config.")
	flag.BoolVar(&useRootKey, "root", false, "Use the root key for the cluster. Defaults to false.")

	// if either are present, use them, otherwise use the config
	flag.StringVar(&publicAddress, "public", "", "Public address of the node to connect to. Defaults to INSI_PUBLIC_ADDRESS environment variable.")
	flag.StringVar(&privateAddress, "private", "", "Private address of the node to connect to. Defaults to INSI_PRIVATE_ADDRESS environment variable.")

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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// processAddress strips scheme and ensures port is present
func processAddress(addr string) string {
	// Strip https:// or http:// prefix if present
	if strings.HasPrefix(addr, "https://") {
		addr = strings.TrimPrefix(addr, "https://")
	} else if strings.HasPrefix(addr, "http://") {
		addr = strings.TrimPrefix(addr, "http://")
	}

	// If no port is specified, add default HTTPS port
	if !strings.Contains(addr, ":") {
		addr = addr + ":443"
	}

	return addr
}

func getClientNoConfig() (*client.Client, error) {
	// Get addresses from flags or environment variables
	pubAddr := publicAddress
	if pubAddr == "" {
		pubAddr = os.Getenv("INSI_PUBLIC_ADDRESS")
	}

	privAddr := privateAddress
	if privAddr == "" {
		privAddr = os.Getenv("INSI_PRIVATE_ADDRESS")
	}

	// If only one address is provided, use it for both
	if pubAddr != "" && privAddr == "" {
		privAddr = pubAddr
	} else if privAddr != "" && pubAddr == "" {
		pubAddr = privAddr
	} else if pubAddr == "" && privAddr == "" {
		return nil, fmt.Errorf("no address provided: use --public, --private, or set INSI_PUBLIC_ADDRESS/INSI_PRIVATE_ADDRESS environment variables")
	}

	pubAddr = processAddress(pubAddr)
	privAddr = processAddress(privAddr)

	// Get API key
	apiKey := os.Getenv("INSI_API_KEY")
	if apiKey == "" && !useRootKey {
		return nil, fmt.Errorf("INSI_API_KEY environment variable is not set and --root flag is not provided")
	}

	// For root key without config, we need the instance secret
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

	// Extract domain from address if present (format: domain:port or ip:port)
	extractDomain := func(addr string) string {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			// If splitting fails, the whole thing might be the host
			return addr
		}
		return host
	}

	clientDomain := extractDomain(pubAddr)

	// Check if TLS verification should be skipped (default to false for security)
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

	// Config is explicitly provided if:
	// 1. User specified a non-default config path via --config
	// 2. The default config file exists and user provided address flags
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

	args := flag.Args() // Get non-flag arguments
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]
	cmdArgs := args[1:]

	if command != "join" {
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
	}

	switch command {
	case "get":
		handleGet(cli, cmdArgs)
	case "set":
		handleSet(cli, cmdArgs)
	case "setnx":
		handleSetNX(cli, cmdArgs)
	case "cas":
		handleCAS(cli, cmdArgs)
	case "bump":
		handleBump(cli, cmdArgs)
	case "delete":
		handleDelete(cli, cmdArgs)
	case "iterate":
		handleIterate(cli, cmdArgs)
	case "cache":
		handleCache(cli, cmdArgs)
	case "join":
		handleJoin(cmdArgs) // Special handling as it targets a specific leader
	case "ping":
		handlePing(cli, cmdArgs)
	case "publish":
		handlePublish(cli, cmdArgs)
	case "subscribe":
		handleSubscribe(cli, cmdArgs)
	case "purge":
		handlePurge(cli, cmdArgs)
	case "api":
		handleApi(cli, cmdArgs)
	case "blob":
		handleBlob(cli, cmdArgs)
	case "alias":
		handleAlias(cli, cmdArgs)
	case "admin":
		handleAdmin(cli, cmdArgs)
	default:
		logger.Error("Unknown command", "command", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: insic [flags] <command> [args...]\n")
	fmt.Fprintf(os.Stderr, "\nConnection Modes:\n")
	fmt.Fprintf(os.Stderr, "  Config mode:   Use --config with a cluster.yaml file (default)\n")
	fmt.Fprintf(os.Stderr, "  Direct mode:   Use --public/--private or env vars (no config file)\n")
	fmt.Fprintf(os.Stderr, "\nEnvironment Variables:\n")
	fmt.Fprintf(os.Stderr, "  INSI_API_KEY          API key for authentication\n")
	fmt.Fprintf(os.Stderr, "  INSI_PUBLIC_ADDRESS   Public address (e.g., db-0.insula.dev or https://db-0.insula.dev:443)\n")
	fmt.Fprintf(os.Stderr, "  INSI_PRIVATE_ADDRESS  Private address (defaults to public if not set)\n")
	fmt.Fprintf(os.Stderr, "  INSI_INSTANCE_SECRET  Instance secret (required for --root without config)\n")
	fmt.Fprintf(os.Stderr, "  INSI_SKIP_VERIFY      Set to 'true' to skip TLS verification\n")
	fmt.Fprintf(os.Stderr, "\nFlags:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\n%s: Cannot use --config together with --public/--private\n", color.YellowString("Note"))
	fmt.Fprintf(os.Stderr, "\nCommands:\n")
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("get"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("set"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("setnx"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("cas"), color.CyanString("<key>"), color.CyanString("<old_value>"), color.CyanString("<new_value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("bump"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("iterate"), color.CyanString("prefix"), color.CyanString("<prefix>"), color.CyanString("[offset]"), color.CyanString("[limit]"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("cache"), color.CyanString("set"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("cache"), color.CyanString("get"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("cache"), color.CyanString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("cache"), color.CyanString("setnx"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("cache"), color.CyanString("cas"), color.CyanString("<key>"), color.CyanString("<old_value>"), color.CyanString("<new_value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s %s\n", color.GreenString("cache"), color.CyanString("iterate"), color.CyanString("prefix"), color.CyanString("<prefix>"), color.CyanString("[offset]"), color.CyanString("[limit]"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("join"), color.CyanString("<leaderNodeID>"), color.CyanString("<followerNodeID>"))
	fmt.Fprintf(os.Stderr, "  %s\n", color.GreenString("ping"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("publish"), color.CyanString("<topic>"), color.CyanString("<data>"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("subscribe"), color.CyanString("<topic>"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("purge"), color.YellowString("Disconnect all event subscriptions for current API key"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("batchset"), color.CyanString("<filepath.json>"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("batchdelete"), color.CyanString("<filepath.json>"))

	// API Key Commands (Note: These typically require the --root flag)
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("api"), color.CyanString("add"), color.CyanString("<key_name>"), color.YellowString("--root flag usually required"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("api"), color.CyanString("delete"), color.CyanString("<key_value>"), color.YellowString("--root flag usually required"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("api"), color.CyanString("verify"), color.CyanString("<key_value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("api"), color.CyanString("limits"), color.CyanString("[key_value]"), color.YellowString("Get limits. If key provided, gets for that key (--root required)."))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("api"), color.CyanString("set-limits"), color.CyanString("<key_value>"), color.CyanString("--disk N --mem N --events N --subs N --rps-data N --rps-event N"), color.YellowString("--root flag usually required"))

	// Alias commands
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("alias"), color.CyanString("add"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("alias"), color.CyanString("delete"), color.CyanString("<alias_key>"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("alias"), color.CyanString("list"))

	// Blob Commands
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("blob"), color.CyanString("upload"), color.CyanString("<key>"), color.CyanString("<filepath>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("blob"), color.CyanString("download"), color.CyanString("<key>"), color.CyanString("<output_path>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("blob"), color.CyanString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("blob"), color.CyanString("iterate"), color.CyanString("<prefix>"), color.CyanString("[offset]"), color.CyanString("[limit]"))

	// Admin commands
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("admin"), color.CyanString("ops"), color.YellowString("--root flag required. Get node ops/sec."))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("admin"), color.CyanString("insight"), color.CyanString("entity"), color.CyanString("<root_api_key>"), color.YellowString("--root flag required."))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s %s\n", color.GreenString("admin"), color.CyanString("insight"), color.CyanString("entities"), color.CyanString("[offset]"), color.CyanString("[limit]"), color.YellowString("--root flag required."))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("admin"), color.CyanString("insight"), color.CyanString("entity-by-alias"), color.CyanString("<alias>"), color.YellowString("--root flag required."))
}

func handlePublish(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("publish: requires <topic> <data>")
		printUsage()
		os.Exit(1)
	}
	topic := args[0]
	dataStr := args[1]

	var dataToPublish any
	var jsonData any
	if err := json.Unmarshal([]byte(dataStr), &jsonData); err == nil {
		dataToPublish = jsonData
	} else {
		dataToPublish = dataStr
	}

	err := c.PublishEvent(topic, dataToPublish)
	if err != nil {
		logger.Error("Publish failed", "topic", topic, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleSubscribe(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("subscribe: requires <topic>")
		printUsage()
		os.Exit(1)
	}
	topic := args[0]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("Received signal, requesting WebSocket closure...", "signal", sig.String())
		cancel()
	}()

	cb := func(data any) {
		payload, ok := data.(models.EventPayload)
		if !ok {
			// Fallback for unexpected data types
			fmt.Printf("Received event on topic '%s': %+v\n", color.CyanString(topic), data)
			return
		}
		// The topic in the payload is the prefixed one, which we don't need to show the user.
		// We already have the user-facing topic from the command argument.
		fmt.Printf("Received event on topic '%s': Data=%+v\n", color.CyanString(topic), payload.Data)
	}

	logger.Info("Attempting to subscribe to events", "topic", color.CyanString(topic))
	err := c.SubscribeToEvents(topic, ctx, cb)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("Subscription cancelled gracefully.", "topic", color.CyanString(topic))
		} else {
			logger.Error("Subscription failed", "topic", topic, "error", err)
			var subErr *client.ErrSubscriberLimitExceeded
			if errors.As(err, &subErr) {
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error: subscriber limit exceeded:"), subErr.Message)
			} else {
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
	}
	logger.Info("Subscription process finished.", "topic", color.CyanString(topic))
}

func handlePurge(c *client.Client, args []string) {
	if len(args) != 0 {
		logger.Error("purge: does not take arguments")
		printUsage()
		os.Exit(1)
	}

	logger.Info("Attempting to purge all event subscriptions for current API key")

	disconnectedCount, err := c.PurgeEventSubscriptions()
	if err != nil {
		logger.Error("Purge failed", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	if disconnectedCount == 0 {
		color.HiYellow("No active event subscriptions found to purge.")
	} else {
		color.HiGreen("Successfully purged %d event subscription(s).", disconnectedCount)
	}
}

// Placeholder for command handlers - to be implemented next
func handleGet(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("get: requires <key>")
		printUsage()
		os.Exit(1)
	}
	key := args[0]
	value, err := c.Get(key)
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
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

func handleSet(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("set: requires <key> <value>")
		printUsage()
		os.Exit(1)
	}
	key, value := args[0], args[1]
	err := c.Set(key, value)
	if err != nil {
		logger.Error("Set failed", "key", key, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleSetNX(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("setnx: requires <key> <value>")
		printUsage()
		os.Exit(1)
	}
	key, value := args[0], args[1]
	err := c.SetNX(key, value)
	if err != nil {
		if errors.Is(err, client.ErrConflict) {
			fmt.Fprintf(os.Stderr, "%s Key '%s' already exists.\n", color.RedString("Conflict:"), color.CyanString(key))
		} else {
			logger.Error("SetNX failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleCAS(c *client.Client, args []string) {
	if len(args) != 3 {
		logger.Error("cas: requires <key> <old_value> <new_value>")
		printUsage()
		os.Exit(1)
	}
	key, oldValue, newValue := args[0], args[1], args[2]
	err := c.CompareAndSwap(key, oldValue, newValue)
	if err != nil {
		if errors.Is(err, client.ErrConflict) {
			fmt.Fprintf(
				os.Stderr,
				"%s Compare-and-swap failed for key '%s'. The current value does not match the expected old value.\n",
				color.RedString("Conflict:"),
				color.CyanString(key),
			)
		} else {
			logger.Error("CAS failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleBump(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("bump: requires <key> <value>")
		printUsage()
		os.Exit(1)
	}
	key := args[0]
	valueStr := args[1]
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		logger.Error("bump: value must be an integer", "error", err)
		fmt.Fprintf(os.Stderr, "%s Value must be an integer: %v\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	err = c.Bump(key, value)
	if err != nil {
		logger.Error("Bump failed", "key", key, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleDelete(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("delete: requires <key>")
		printUsage()
		os.Exit(1)
	}
	key := args[0]
	err := c.Delete(key)
	if err != nil {
		logger.Error("Delete failed", "key", key, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleIterate(c *client.Client, args []string) {
	if len(args) < 2 {
		logger.Error("iterate: requires <type (prefix|tag)> <value> [offset] [limit]")
		printUsage()
		os.Exit(1)
	}
	iterType := args[0]
	value := args[1]
	offset, limit := 0, 100

	var err error
	if len(args) > 2 {
		offset, err = strconv.Atoi(args[2])
		if err != nil {
			logger.Error("iterate: invalid offset", "offset_str", args[2], "error", err)
			fmt.Fprintf(os.Stderr, "%s Invalid offset '%s': %v\n", color.RedString("Error:"), args[2], err)
			os.Exit(1)
		}
	}
	if len(args) > 3 {
		limit, err = strconv.Atoi(args[3])
		if err != nil {
			logger.Error("iterate: invalid limit", "limit_str", args[3], "error", err)
			fmt.Fprintf(os.Stderr, "%s Invalid limit '%s': %v\n", color.RedString("Error:"), args[3], err)
			os.Exit(1)
		}
	}

	var results []string
	switch iterType {
	case "prefix":
		results, err = c.IterateByPrefix(value, offset, limit)
	default:
		logger.Error("iterate: unknown type", "type", iterType)
		printUsage()
		os.Exit(1)
		return
	}

	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			logger.Warn("Iterate: No keys found matching criteria", "type", iterType, "value", value, "offset", offset, "limit", limit)
			color.HiRed("No keys found.")
		} else {
			logger.Error("Iterate failed", "type", iterType, "value", value, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	for _, item := range results {
		fmt.Println(item)
	}
}

func handleCache(c *client.Client, args []string) {
	if len(args) < 1 {
		logger.Error("cache: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}
	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "set":
		if len(subArgs) != 2 {
			logger.Error("cache set: requires <key> <value>")
			printUsage()
			os.Exit(1)
		}
		key, valStr := subArgs[0], subArgs[1]
		err := c.SetCache(key, valStr)
		if err != nil {
			logger.Error("Cache set failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		color.HiGreen("OK")
	case "get":
		if len(subArgs) != 1 {
			logger.Error("cache get: requires <key>")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]
		value, err := c.GetCache(key)
		if err != nil {
			if errors.Is(err, client.ErrKeyNotFound) {
				fmt.Fprintf(
					os.Stderr,
					"%s Key '%s' not found in cache.\n",
					color.RedString("Error:"),
					color.CyanString(key),
				)
			} else {
				logger.Error("Cache get failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
		fmt.Println(value)
	case "delete":
		if len(subArgs) != 1 {
			logger.Error("cache delete: requires <key>")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]
		err := c.DeleteCache(key)
		if err != nil {
			if errors.Is(err, client.ErrKeyNotFound) {
				logger.Info("Cache delete: Key not found, no action taken.", "key", key)
				fmt.Fprintf(
					os.Stderr,
					"%s Key '%s' not found in cache. Nothing to delete.\n",
					color.YellowString("Warning:"),
					color.CyanString(key),
				)
			} else {
				logger.Error("Cache delete failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
		color.HiGreen("OK")
	case "setnx":
		if len(subArgs) != 2 {
			logger.Error("cache setnx: requires <key> <value>")
			printUsage()
			os.Exit(1)
		}
		key, value := subArgs[0], subArgs[1]
		err := c.SetCacheNX(key, value)
		if err != nil {
			if errors.Is(err, client.ErrConflict) {
				fmt.Fprintf(os.Stderr, "%s Key '%s' already exists in cache.\n", color.RedString("Conflict:"), color.CyanString(key))
			} else {
				logger.Error("Cache SetNX failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
		color.HiGreen("OK")
	case "cas":
		if len(subArgs) != 3 {
			logger.Error("cache cas: requires <key> <old_value> <new_value>")
			printUsage()
			os.Exit(1)
		}
		key, oldValue, newValue := subArgs[0], subArgs[1], subArgs[2]
		err := c.CompareAndSwapCache(key, oldValue, newValue)
		if err != nil {
			if errors.Is(err, client.ErrConflict) {
				fmt.Fprintf(
					os.Stderr,
					"%s Cache compare-and-swap failed for key '%s'.\n",
					color.RedString("Precondition Failed:"),
					color.CyanString(key),
				)
			} else {
				logger.Error("Cache CAS failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
		color.HiGreen("OK")
	case "iterate":
		handleCacheIterate(c, subArgs)
	default:
		logger.Error("cache: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleCacheIterate(c *client.Client, args []string) {
	if len(args) < 2 {
		logger.Error("cache iterate: requires <type (prefix)> <value> [offset] [limit]")
		printUsage()
		os.Exit(1)
	}
	iterType := args[0]
	value := args[1]
	offset, limit := 0, 100

	var err error
	if len(args) > 2 {
		offset, err = strconv.Atoi(args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Invalid offset '%s': %v\n", color.RedString("Error:"), args[2], err)
			os.Exit(1)
		}
	}
	if len(args) > 3 {
		limit, err = strconv.Atoi(args[3])
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Invalid limit '%s': %v\n", color.RedString("Error:"), args[3], err)
			os.Exit(1)
		}
	}

	var results []string
	switch iterType {
	case "prefix":
		results, err = c.IterateCacheByPrefix(value, offset, limit)
	default:
		logger.Error("cache iterate: unknown type", "type", iterType)
		printUsage()
		os.Exit(1)
		return
	}

	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			color.HiRed("No keys found in cache.")
		} else {
			logger.Error("Cache iterate failed", "type", iterType, "value", value, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	for _, item := range results {
		fmt.Println(item)
	}
}

func handleJoin(args []string) {
	if len(args) != 2 {
		logger.Error("join: requires <leaderNodeID> <followerNodeID>")
		printUsage()
		os.Exit(1)
	}
	leaderNodeID := args[0]
	followerNodeID := args[1]

	// Join command requires config file
	if clusterCfg == nil {
		logger.Error("join command requires a config file")
		fmt.Fprintf(os.Stderr, "%s join command requires --config with a valid cluster configuration file\n", color.RedString("Error:"))
		os.Exit(1)
	}

	logger.Info("Join command initiated", "leader_node", leaderNodeID, "follower_node", followerNodeID)

	// Create a client specifically for the leader node
	leaderClient, err := getClient(clusterCfg, leaderNodeID)
	if err != nil {
		logger.Error("Failed to create client for leader node", "leader_node", leaderNodeID, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	followerDetails, ok := clusterCfg.Nodes[followerNodeID]
	if !ok {
		logger.Error(
			"Follower node ID not found in configuration",
			"follower_node_id", followerNodeID,
		)
		fmt.Fprintf(
			os.Stderr,
			"%s Follower node ID '%s' not found in configuration.\n",
			color.RedString("Error:"),
			color.CyanString(followerNodeID),
		)
		os.Exit(1)
	}

	logger.Info(
		"Attempting to join follower",
		"follower_id", color.CyanString(followerNodeID),
		"follower_raft_addr", followerDetails.RaftBinding,
		"via_leader", color.CyanString(leaderNodeID),
	)

	err = leaderClient.Join(followerNodeID, followerDetails.PrivateBinding)
	if err != nil {
		logger.Error("Join failed", "leader_node", leaderNodeID, "follower_node", followerNodeID, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	logger.Info(
		"Join successful",
		"leader_node", color.CyanString(leaderNodeID),
		"follower_node", color.CyanString(followerNodeID),
	)
	color.HiGreen("OK")
}

func handlePing(c *client.Client, args []string) {
	if len(args) != 0 {
		logger.Error("ping: does not take arguments")
		printUsage()
		os.Exit(1)
	}
	resp, err := c.Ping()
	if err != nil {
		logger.Error("Ping failed", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	fmt.Println("Ping Response:")
	for k, v := range resp {
		fmt.Printf("  %s: %s\n", color.CyanString(k), v)
	}
}

// --- API Key Command Handlers ---

func handleApi(c *client.Client, args []string) {
	if len(args) < 1 {
		logger.Error("api: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}
	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "add":
		// Enforce --root for add
		if !useRootKey {
			logger.Error("api add requires the --root flag to be set.")
			fmt.Fprintf(os.Stderr, "%s api add requires --root flag.\n", color.RedString("Error:"))
			os.Exit(1)
		}
		handleApiAdd(c, subArgs)
	case "delete":
		// Enforce --root for delete
		if !useRootKey {
			logger.Error("api delete requires the --root flag to be set.")
			fmt.Fprintf(os.Stderr, "%s api delete requires --root flag.\n", color.RedString("Error:"))
			os.Exit(1)
		}
		handleApiDelete(c, subArgs)
	case "verify":
		// --root is not required for verify, as we are creating a new client with the provided key
		handleApiVerify(subArgs)
	case "limits":
		if len(subArgs) == 0 {
			handleApiGetMyLimits(c, subArgs)
		} else {
			if !useRootKey {
				logger.Error("api limits <key_value> requires the --root flag to be set.")
				fmt.Fprintf(os.Stderr, "%s api limits <key_value> requires --root flag.\n", color.RedString("Error:"))
				os.Exit(1)
			}
			handleApiGetLimits(c, subArgs)
		}
	case "set-limits":
		// Enforce --root for set-limits
		if !useRootKey {
			logger.Error("api set-limits requires the --root flag to be set.")
			fmt.Fprintf(os.Stderr, "%s api set-limits requires --root flag.\n", color.RedString("Error:"))
			os.Exit(1)
		}
		handleApiSetLimits(c, subArgs)
	default:
		logger.Error("api: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleApiAdd(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("api add: requires <key_name>")
		printUsage()
		os.Exit(1)
	}
	keyName := args[0]

	resp, err := c.CreateAPIKey(keyName)
	if err != nil {
		logger.Error("API key creation failed", "key_name", keyName, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	logger.Info("API key created successfully", "key_name", resp.KeyName, "key", resp.Key)
	fmt.Printf("API Key Name: %s\n", color.CyanString(resp.KeyName))
	fmt.Printf("API Key:      %s\n", color.GreenString(resp.Key))
	color.HiGreen("OK")
}

func handleApiDelete(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("api delete: requires <key_value>")
		printUsage()
		os.Exit(1)
	}
	keyValue := args[0] // This is the actual "insi_..." key string

	err := c.DeleteAPIKey(keyValue)
	if err != nil {
		logger.Error("API key deletion failed", "key_value", keyValue, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	logger.Info("API key deleted successfully", "key_value", keyValue)
	color.HiGreen("OK")
}

func handleApiVerify(args []string) {
	if len(args) != 1 {
		logger.Error("api verify: requires <key_value>")
		printUsage()
		os.Exit(1)
	}
	apiKeyToVerify := args[0]

	var verifyCli *client.Client
	var err error
	verifyClientLogger := logger.WithGroup("verify_client")

	if clusterCfg != nil {
		// Config mode
		nodeToConnect := targetNode
		if nodeToConnect == "" {
			if clusterCfg.DefaultLeader == "" {
				logger.Error(
					"api verify: targetNode is empty and no DefaultLeader is set in config",
				)
				fmt.Fprintf(
					os.Stderr,
					"%s Target node must be specified via --target or DefaultLeader in config.\n",
					color.RedString("Error:"),
				)
				os.Exit(1)
			}
			nodeToConnect = clusterCfg.DefaultLeader
			logger.Info(
				"No target node specified for verify, using DefaultLeader", "node_id", color.CyanString(nodeToConnect),
			)
		}

		nodeDetails, ok := clusterCfg.Nodes[nodeToConnect]
		if !ok {
			logger.Error(
				"api verify: node ID not found in configuration", "node_id", nodeToConnect,
			)
			fmt.Fprintf(
				os.Stderr,
				"%s Node ID '%s' not found in configuration.\n",
				color.RedString("Error:"),
				color.CyanString(nodeToConnect),
			)
			os.Exit(1)
		}

		verifyCli, err = client.NewClient(&client.Config{
			ConnectionType: client.ConnectionTypeDirect,
			Endpoints: []client.Endpoint{
				{
					PublicBinding:  nodeDetails.PublicBinding,
					PrivateBinding: nodeDetails.PrivateBinding,
					ClientDomain:   nodeDetails.ClientDomain,
				},
			},
			ApiKey:     apiKeyToVerify,
			SkipVerify: clusterCfg.ClientSkipVerify,
			Logger:     verifyClientLogger,
		})
	} else {
		pubAddr := publicAddress
		if pubAddr == "" {
			pubAddr = os.Getenv("INSI_PUBLIC_ADDRESS")
		}

		privAddr := privateAddress
		if privAddr == "" {
			privAddr = os.Getenv("INSI_PRIVATE_ADDRESS")
		}

		// If only one address is provided, use it for both
		if pubAddr != "" && privAddr == "" {
			privAddr = pubAddr
		} else if privAddr != "" && pubAddr == "" {
			pubAddr = privAddr
		} else if pubAddr == "" && privAddr == "" {
			logger.Error("api verify: no address provided in non-config mode")
			fmt.Fprintf(os.Stderr, "%s No address provided: use --public, --private, or set INSI_PUBLIC_ADDRESS/INSI_PRIVATE_ADDRESS\n", color.RedString("Error:"))
			os.Exit(1)
		}

		pubAddr = processAddress(pubAddr)
		privAddr = processAddress(privAddr)

		// Extract domain from address
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

		verifyCli, err = client.NewClient(&client.Config{
			ConnectionType: client.ConnectionTypeDirect,
			Endpoints: []client.Endpoint{
				{
					PublicBinding:  pubAddr,
					PrivateBinding: privAddr,
					ClientDomain:   clientDomain,
				},
			},
			ApiKey:     apiKeyToVerify,
			SkipVerify: skipVerify,
			Logger:     verifyClientLogger,
		})
	}

	if err != nil {
		logger.Error("api verify: failed to create client for verification", "error", err)
		fmt.Fprintf(os.Stderr, "%s Failed to create client for verification: %v\n", color.RedString("Error:"), err)
		color.HiRed("Verification FAILED")
		os.Exit(1)
	}

	logger.Info("Attempting to verify API key with a ping...")
	pingResp, err := verifyCli.Ping()
	if err != nil {
		logger.Error("API key verification failed: Ping request failed", "key_value", apiKeyToVerify, "error", err)
		fmt.Fprintf(os.Stderr, "%s API key verification failed. Ping error: %v\n", color.RedString("Error:"), err)
		color.HiRed("Verification FAILED")
		os.Exit(1)
	}

	logger.Info("API key verification successful: Ping responded", "key_value", apiKeyToVerify, "response", pingResp)
	color.HiGreen("API Key Verified Successfully!")
	fmt.Println("Ping Response:")
	for k, v := range pingResp {
		fmt.Printf("  %s: %s\n", color.CyanString(k), v)
	}
}

func handleApiGetMyLimits(c *client.Client, args []string) {
	if len(args) != 0 {
		logger.Error("api limits: does not take arguments for self")
		printUsage()
		os.Exit(1)
	}

	resp, err := c.GetLimits()
	if err != nil {
		logger.Error("Failed to get API key limits", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	fmt.Println(color.CyanString("Maximum Limits (for current key):"))
	if resp.MaxLimits.BytesOnDisk != nil {
		fmt.Printf("  Bytes on Disk:     %d\n", *resp.MaxLimits.BytesOnDisk)
	}
	if resp.MaxLimits.BytesInMemory != nil {
		fmt.Printf("  Bytes in Memory:   %d\n", *resp.MaxLimits.BytesInMemory)
	}
	if resp.MaxLimits.EventsEmitted != nil {
		fmt.Printf("  Events per Second: %d\n", *resp.MaxLimits.EventsEmitted)
	}
	if resp.MaxLimits.Subscribers != nil {
		fmt.Printf("  Subscribers:       %d\n", *resp.MaxLimits.Subscribers)
	}
	if resp.MaxLimits.RPSDataLimit != nil {
		fmt.Printf("  RPS Data Limit:    %d\n", *resp.MaxLimits.RPSDataLimit)
	}
	if resp.MaxLimits.RPSEventLimit != nil {
		fmt.Printf("  RPS Event Limit:   %d\n", *resp.MaxLimits.RPSEventLimit)
	}
	fmt.Println(color.CyanString("\nCurrent Usage (for current key):"))
	if resp.CurrentUsage.BytesOnDisk != nil {
		fmt.Printf("  Bytes on Disk:     %d\n", *resp.CurrentUsage.BytesOnDisk)
	}
	if resp.CurrentUsage.BytesInMemory != nil {
		fmt.Printf("  Bytes in Memory:   %d\n", *resp.CurrentUsage.BytesInMemory)
	}
	if resp.CurrentUsage.EventsEmitted != nil {
		fmt.Printf("  Events per Second: %d\n", *resp.CurrentUsage.EventsEmitted)
	}
	if resp.CurrentUsage.Subscribers != nil {
		fmt.Printf("  Subscribers:       %d\n", *resp.CurrentUsage.Subscribers)
	}
}

func handleApiGetLimits(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("api get-limits: requires <key_value>")
		printUsage()
		os.Exit(1)
	}
	apiKey := args[0]

	resp, err := c.GetLimitsForKey(apiKey)
	if err != nil {
		logger.Error("Failed to get API key limits for specific key", "key", apiKey, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	var keyIdentifier string
	if len(apiKey) > 12 { // "insi_" prefix + some chars
		keyIdentifier = apiKey[:8] + "..." + apiKey[len(apiKey)-4:]
	} else {
		keyIdentifier = apiKey
	}

	fmt.Printf("Limits for key %s\n", color.CyanString(keyIdentifier))

	fmt.Println(color.CyanString("Maximum Limits:"))
	if resp.MaxLimits.BytesOnDisk != nil {
		fmt.Printf("  Bytes on Disk:     %d\n", *resp.MaxLimits.BytesOnDisk)
	}
	if resp.MaxLimits.BytesInMemory != nil {
		fmt.Printf("  Bytes in Memory:   %d\n", *resp.MaxLimits.BytesInMemory)
	}
	if resp.MaxLimits.EventsEmitted != nil {
		fmt.Printf("  Events per Second: %d\n", *resp.MaxLimits.EventsEmitted)
	}
	if resp.MaxLimits.Subscribers != nil {
		fmt.Printf("  Subscribers:       %d\n", *resp.MaxLimits.Subscribers)
	}
	if resp.MaxLimits.RPSDataLimit != nil {
		fmt.Printf("  RPS Data Limit:    %d\n", *resp.MaxLimits.RPSDataLimit)
	}
	if resp.MaxLimits.RPSEventLimit != nil {
		fmt.Printf("  RPS Event Limit:   %d\n", *resp.MaxLimits.RPSEventLimit)
	}

	fmt.Println(color.CyanString("\nCurrent Usage:"))
	if resp.CurrentUsage.BytesOnDisk != nil {
		fmt.Printf("  Bytes on Disk:     %d\n", *resp.CurrentUsage.BytesOnDisk)
	}
	if resp.CurrentUsage.BytesInMemory != nil {
		fmt.Printf("  Bytes in Memory:   %d\n", *resp.CurrentUsage.BytesInMemory)
	}
	if resp.CurrentUsage.EventsEmitted != nil {
		fmt.Printf("  Events per Second: %d\n", *resp.CurrentUsage.EventsEmitted)
	}
	if resp.CurrentUsage.Subscribers != nil {
		fmt.Printf("  Subscribers:       %d\n", *resp.CurrentUsage.Subscribers)
	}
}

func handleApiSetLimits(c *client.Client, args []string) {
	setLimitsCmd := flag.NewFlagSet("set-limits", flag.ExitOnError)
	disk := setLimitsCmd.Int64("disk", -1, "Max bytes on disk")
	mem := setLimitsCmd.Int64("mem", -1, "Max bytes in memory")
	events := setLimitsCmd.Int64("events", -1, "Max events per second")
	subs := setLimitsCmd.Int64("subs", -1, "Max subscribers")
	rpsData := setLimitsCmd.Int64("rps-data", -1, "Data operations per second limit (e.g. set, get, delete)")
	rpsEvent := setLimitsCmd.Int64("rps-event", -1, "Event operations per second limit (e.g. publish, subscribe)")

	if len(args) < 1 {
		logger.Error("api set-limits: requires <key_value> and flags")
		setLimitsCmd.Usage()
		os.Exit(1)
	}
	apiKey := args[0]
	if err := setLimitsCmd.Parse(args[1:]); err != nil {
		logger.Error("api set-limits: error parsing flags", "error", err)
		os.Exit(1)
	}

	if len(setLimitsCmd.Args()) > 0 {
		logger.Error("api set-limits: unknown arguments provided", "unknown_args", setLimitsCmd.Args())
		setLimitsCmd.Usage()
		os.Exit(1)
	}

	limits := models.Limits{}
	if *disk != -1 {
		limits.BytesOnDisk = disk
	}
	if *mem != -1 {
		limits.BytesInMemory = mem
	}
	if *events != -1 {
		limits.EventsEmitted = events
	}
	if *subs != -1 {
		limits.Subscribers = subs
	}
	if *rpsData != -1 {
		limits.RPSDataLimit = rpsData
	}
	if *rpsEvent != -1 {
		limits.RPSEventLimit = rpsEvent
	}

	err := c.SetLimits(apiKey, limits)
	if err != nil {
		logger.Error("Failed to set API key limits", "key", apiKey, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	color.HiGreen("OK")
}

// --- Blob Command Handlers ---

func handleBlob(c *client.Client, args []string) {
	if len(args) < 1 {
		logger.Error("blob: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}
	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "upload":
		handleBlobUpload(c, subArgs)
	case "download":
		handleBlobDownload(c, subArgs)
	case "delete":
		handleBlobDelete(c, subArgs)
	case "iterate":
		handleBlobIterate(c, subArgs)
	default:
		logger.Error("blob: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleBlobUpload(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("blob upload: requires <key> <filepath>")
		printUsage()
		os.Exit(1)
	}
	key, filePath := args[0], args[1]

	file, err := os.Open(filePath)
	if err != nil {
		logger.Error("Failed to open file for blob upload", "path", filePath, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	defer file.Close()

	ctx := context.Background()
	err = c.UploadBlob(ctx, key, file, filepath.Base(filePath))
	if err != nil {
		logger.Error("Blob upload failed", "key", key, "path", filePath, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleBlobDownload(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("blob download: requires <key> <output_path>")
		printUsage()
		os.Exit(1)
	}
	key, outputPath := args[0], args[1]

	ctx := context.Background()
	reader, err := c.GetBlob(ctx, key)
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			fmt.Fprintf(os.Stderr, "%s Blob '%s' not found.\n", color.RedString("Error:"), color.CyanString(key))
			os.Exit(1)
		} else {
			logger.Error("GetBlob failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	defer reader.Close()

	outFile, err := os.Create(outputPath)
	if err != nil {
		logger.Error("Failed to create output file for blob download", "path", outputPath, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, reader)
	if err != nil {
		logger.Error("Failed to write blob data to file", "path", outputPath, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleBlobDelete(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("blob delete: requires <key>")
		printUsage()
		os.Exit(1)
	}
	key := args[0]
	err := c.DeleteBlob(key)
	if err != nil {
		logger.Error("Blob delete failed", "key", key, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleBlobIterate(c *client.Client, args []string) {
	if len(args) < 1 {
		logger.Error("blob iterate: requires <prefix> [offset] [limit]")
		printUsage()
		os.Exit(1)
	}
	prefix := args[0]
	offset, limit := 0, 100

	var err error
	if len(args) > 1 {
		offset, err = strconv.Atoi(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Invalid offset '%s': %v\n", color.RedString("Error:"), args[1], err)
			os.Exit(1)
		}
	}
	if len(args) > 2 {
		limit, err = strconv.Atoi(args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Invalid limit '%s': %v\n", color.RedString("Error:"), args[2], err)
			os.Exit(1)
		}
	}

	results, err := c.IterateBlobKeysByPrefix(prefix, offset, limit)
	if err != nil {
		logger.Error("Blob iterate failed", "prefix", prefix, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	if len(results) == 0 {
		color.HiYellow("No blob keys found matching prefix.")
		return
	}

	for _, item := range results {
		fmt.Println(item)
	}
}

// --- Alias Command Handlers ---
func handleAlias(c *client.Client, args []string) {
	if len(args) < 1 {
		logger.Error("alias: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}
	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "add":
		handleAliasAdd(c, subArgs)
	case "delete":
		handleAliasDelete(c, subArgs)
	case "list":
		handleAliasList(c, subArgs)
	default:
		logger.Error("alias: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleAliasAdd(c *client.Client, args []string) {
	if len(args) != 0 {
		logger.Error("alias add: does not take arguments")
		printUsage()
		os.Exit(1)
	}

	resp, err := c.SetAlias()
	if err != nil {
		logger.Error("Alias creation failed", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	fmt.Printf("Alias Key: %s\n", color.GreenString(resp.Alias))
	color.HiGreen("OK")
}

func handleAliasDelete(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("alias delete: requires <alias_key>")
		printUsage()
		os.Exit(1)
	}
	aliasKey := args[0]

	err := c.DeleteAlias(aliasKey)
	if err != nil {
		logger.Error("Alias deletion failed", "alias", aliasKey, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	color.HiGreen("OK")
}

func handleAliasList(c *client.Client, args []string) {
	if len(args) != 0 {
		logger.Error("alias list: does not take arguments")
		printUsage()
		os.Exit(1)
	}

	resp, err := c.ListAliases()
	if err != nil {
		logger.Error("Failed to list aliases", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	if len(resp.Aliases) == 0 {
		fmt.Println("No aliases found for the current key.")
		return
	}

	fmt.Println(color.CyanString("Aliases:"))
	for _, alias := range resp.Aliases {
		fmt.Printf("  - %s\n", alias)
	}
}

func handleAdmin(c *client.Client, args []string) {
	if len(args) < 1 {
		logger.Error("admin: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}
	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "ops":
		if !useRootKey {
			logger.Error("admin ops requires the --root flag to be set.")
			fmt.Fprintf(os.Stderr, "%s admin ops requires --root flag.\n", color.RedString("Error:"))
			os.Exit(1)
		}
		handleAdminOps(c, subArgs)
	case "insight":
		if !useRootKey {
			logger.Error("admin insight requires the --root flag to be set.")
			fmt.Fprintf(os.Stderr, "%s admin insight requires --root flag.\n", color.RedString("Error:"))
			os.Exit(1)
		}
		handleAdminInsight(c, subArgs)
	default:
		logger.Error("admin: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleAdminOps(c *client.Client, args []string) {
	if len(args) != 0 {
		logger.Error("admin ops: does not take arguments")
		printUsage()
		os.Exit(1)
	}

	resp, err := c.GetOpsPerSecond()
	if err != nil {
		logger.Error("Failed to get ops per second", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	fmt.Println(color.CyanString("Operations Per Second:"))
	fmt.Printf("  System:       %.2f\n", resp.OP_System)
	fmt.Printf("  Value Store:  %.2f\n", resp.OP_VS)
	fmt.Printf("  Cache:        %.2f\n", resp.OP_Cache)
	fmt.Printf("  Events:       %.2f\n", resp.OP_Events)
	fmt.Printf("  Subscribers:  %.2f\n", resp.OP_Subscribers)
	fmt.Printf("  Blobs:        %.2f\n", resp.OP_Blobs)
}

func handleAdminInsight(c *client.Client, args []string) {
	if len(args) < 1 {
		logger.Error("admin insight: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}
	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "entity":
		handleAdminInsightEntity(c, subArgs)
	case "entities":
		handleAdminInsightEntities(c, subArgs)
	case "entity-by-alias":
		handleAdminInsightEntityByAlias(c, subArgs)
	default:
		logger.Error("admin insight: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleAdminInsightEntity(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("admin insight entity: requires <root_api_key>")
		printUsage()
		os.Exit(1)
	}
	rootApiKey := args[0]
	entity, err := c.GetEntity(rootApiKey)
	if err != nil {
		logger.Error("Failed to get entity", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	printEntity(entity)
}

func handleAdminInsightEntities(c *client.Client, args []string) {
	offset, limit := 0, 10
	var err error
	if len(args) > 0 {
		offset, err = strconv.Atoi(args[0])
		if err != nil {
			logger.Error("admin insight entities: invalid offset", "offset_str", args[0], "error", err)
			fmt.Fprintf(os.Stderr, "%s Invalid offset '%s': %v\n", color.RedString("Error:"), args[0], err)
			os.Exit(1)
		}
	}
	if len(args) > 1 {
		limit, err = strconv.Atoi(args[1])
		if err != nil {
			logger.Error("admin insight entities: invalid limit", "limit_str", args[1], "error", err)
			fmt.Fprintf(os.Stderr, "%s Invalid limit '%s': %v\n", color.RedString("Error:"), args[1], err)
			os.Exit(1)
		}
	}

	entities, err := c.GetEntities(offset, limit)
	if err != nil {
		logger.Error("Failed to get entities", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	if len(entities) == 0 {
		color.HiYellow("No entities found.")
		return
	}

	for i, entity := range entities {
		e := entity
		printEntity(&e)
		if i < len(entities)-1 {
			fmt.Println(color.GreenString("--------------------"))
		}
	}
}

func handleAdminInsightEntityByAlias(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("admin insight entity-by-alias: requires <alias>")
		printUsage()
		os.Exit(1)
	}
	alias := args[0]
	entity, err := c.GetEntityByAlias(alias)
	if err != nil {
		logger.Error("Failed to get entity by alias", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	printEntity(entity)
}

func printEntity(entity *models.Entity) {
	fmt.Println(color.CyanString("Entity Details:"))
	fmt.Printf("  Root API Key:    %s\n", entity.RootApiKey)
	fmt.Printf("  Key UUID:        %s\n", entity.KeyUUID)
	fmt.Printf("  Data Scope UUID: %s\n", entity.DataScopeUUID)

	if len(entity.Aliases) > 0 {
		fmt.Println("  Aliases:")
		for _, alias := range entity.Aliases {
			fmt.Printf("    - %s\n", alias)
		}
	} else {
		fmt.Println("  Aliases:         None")
	}

	if entity.Usage.MaxLimits != nil {
		fmt.Println(color.CyanString("\n  Maximum Limits:"))
		if entity.Usage.MaxLimits.BytesOnDisk != nil {
			fmt.Printf("    Bytes on Disk:     %d\n", *entity.Usage.MaxLimits.BytesOnDisk)
		}
		if entity.Usage.MaxLimits.BytesInMemory != nil {
			fmt.Printf("    Bytes in Memory:   %d\n", *entity.Usage.MaxLimits.BytesInMemory)
		}
		if entity.Usage.MaxLimits.EventsEmitted != nil {
			fmt.Printf("    Events Emitted:    %d\n", *entity.Usage.MaxLimits.EventsEmitted)
		}
		if entity.Usage.MaxLimits.Subscribers != nil {
			fmt.Printf("    Subscribers:       %d\n", *entity.Usage.MaxLimits.Subscribers)
		}
		if entity.Usage.MaxLimits.RPSDataLimit != nil {
			fmt.Printf("    RPS Data Limit:    %d\n", *entity.Usage.MaxLimits.RPSDataLimit)
		}
		if entity.Usage.MaxLimits.RPSEventLimit != nil {
			fmt.Printf("    RPS Event Limit:   %d\n", *entity.Usage.MaxLimits.RPSEventLimit)
		}
	}

	if entity.Usage.CurrentUsage != nil {
		fmt.Println(color.CyanString("\n  Current Usage:"))
		if entity.Usage.CurrentUsage.BytesOnDisk != nil {
			fmt.Printf("    Bytes on Disk:     %d\n", *entity.Usage.CurrentUsage.BytesOnDisk)
		}
		if entity.Usage.CurrentUsage.BytesInMemory != nil {
			fmt.Printf("    Bytes in Memory:   %d\n", *entity.Usage.CurrentUsage.BytesInMemory)
		}
		if entity.Usage.CurrentUsage.EventsEmitted != nil {
			fmt.Printf("    Events Emitted:    %d\n", *entity.Usage.CurrentUsage.EventsEmitted)
		}
		if entity.Usage.CurrentUsage.Subscribers != nil {
			fmt.Printf("    Subscribers:       %d\n", *entity.Usage.CurrentUsage.Subscribers)
		}
	}
	fmt.Println()
}
