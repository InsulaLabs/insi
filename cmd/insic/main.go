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
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/db/models"
	"github.com/fatih/color"
	"gopkg.in/yaml.v3"
)

var (
	logger     *slog.Logger
	configPath string
	clusterCfg *config.Cluster
	targetNode string // Added for --target flag
	useRootKey bool   // Added for --root flag
)

func init() {
	// Initialize logger
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo, // Default level, can be configured further
	}
	handler := slog.NewTextHandler(os.Stderr, logOpts)
	logger = slog.New(handler)

	flag.StringVar(&configPath, "config", "cluster.yaml", "Path to the cluster configuration file")
	flag.StringVar(&targetNode, "target", "", "Target node ID (e.g., node0, node1). Defaults to DefaultLeader in config.") // Added target flag
	flag.BoolVar(&useRootKey, "root", false, "Use the root key for the cluster. Defaults to false.")
}

func loadConfig(path string) (*config.Cluster, error) {
	// logger.Info("Loading configuration", "path", path) // Reduced verbosity
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg config.Cluster
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config data from %s: %w", path, err)
	}
	// logger.Info("Configuration loaded successfully") // Reduced verbosity
	return &cfg, nil
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

	// clientLogger.Info("Client is using instanceSecret for token generation", "secret_value", cfg.InstanceSecret) // Too verbose for default

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
	// logger.Info("Client created successfully", "target_node", nodeToConnect, "hostport", nodeDetails.HttpBinding) // Reduced verbosity
	return c, nil
}

func main() {
	flag.Parse() // Parse command-line flags first

	var err error
	clusterCfg, err = loadConfig(configPath)
	if err != nil {
		logger.Error("Failed to load cluster configuration", "error", err)
		os.Exit(1)
	}

	args := flag.Args() // Get non-flag arguments
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]
	cmdArgs := args[1:]

	// Default client (usually to DefaultLeader)
	// For 'join', a specific client will be created.
	var cli *client.Client
	if command != "join" { // 'join' command handles its client creation specifically
		cli, err = getClient(clusterCfg, targetNode)
		if err != nil {
			logger.Error("Failed to initialize default API client", "error", err)
			os.Exit(1)
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
	case "api":
		handleApi(cli, cmdArgs)
	case "object":
		handleObject(cli, cmdArgs)
	default:
		logger.Error("Unknown command", "command", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: insic [flags] <command> [args...]\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	flag.PrintDefaults() // Uses default formatting, consider customizing if needed
	fmt.Fprintf(os.Stderr, "\nCommands:\n")
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("get"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("set"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("setnx"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("cas"), color.CyanString("<key>"), color.CyanString("<old_value>"), color.CyanString("<new_value>"))
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
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("object"), color.CyanString("set"), color.CyanString("<key>"), color.CyanString("<filepath>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("object"), color.CyanString("get"), color.CyanString("<key>"), color.CyanString("<output_filepath>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("object"), color.CyanString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("object"), color.CyanString("list"), color.CyanString("[prefix]"), color.CyanString("[offset]"), color.CyanString("[limit]"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("batchset"), color.CyanString("<filepath.json>"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("batchdelete"), color.CyanString("<filepath.json>"))

	// API Key Commands (Note: These typically require the --root flag)
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("api"), color.CyanString("add"), color.CyanString("<key_name>"), color.YellowString("--root flag usually required"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("api"), color.CyanString("delete"), color.CyanString("<key_value>"), color.YellowString("--root flag usually required"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("api"), color.CyanString("verify"), color.CyanString("<key_value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("api"), color.CyanString("limits"), color.YellowString("uses key from env or --root"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("api"), color.CyanString("set-limits"), color.CyanString("<key_value>"), color.CyanString("--disk N --mem N --events N --subs N"), color.YellowString("--root flag usually required"))
	// Object Commands
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("object"), color.CyanString("upload"), color.CyanString("<filepath>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("object"), color.CyanString("download"), color.CyanString("<uuid>"), color.CyanString("<output_path>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("object"), color.CyanString("hash"), color.CyanString("<uuid>"))
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
	// Try to unmarshal the data argument as JSON.
	// If it fails, we assume it's a plain string.
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
	// logger.Info("Publish successful", "topic", topic) // Redundant with "OK"
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

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("Received signal, requesting WebSocket closure...", "signal", sig.String())
		cancel() // Cancel the context to signal SubscribeToEvents to close
	}()

	cb := func(data any) {
		fmt.Printf("Received event on topic '%s': %+v\n", color.CyanString(topic), data)
	}

	logger.Info("Attempting to subscribe to events", "topic", color.CyanString(topic))
	err := c.SubscribeToEvents(topic, ctx, cb)
	if err != nil {
		// context.Canceled is an expected error on graceful shutdown, others are not.
		if err == context.Canceled {
			logger.Info("Subscription cancelled gracefully.", "topic", color.CyanString(topic))
		} else {
			logger.Error("Subscription failed", "topic", topic, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			// os.Exit(1) // Exiting here might be too abrupt if there's a non-critical error during teardown.
		}
	}
	logger.Info("Subscription process finished.", "topic", color.CyanString(topic))
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
			// logger.Error("Get failed: Key not found", "key", key) // Already handled by specific message
			fmt.Fprintf(os.Stderr, "%s Key '%s' not found.\n", color.RedString("Error:"), color.CyanString(key))
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
	// logger.Info("Set successful", "key", key) // Redundant
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
			fmt.Fprintf(os.Stderr, "%s Compare-and-swap failed for key '%s'. The current value does not match the expected old value.\n", color.RedString("Conflict:"), color.CyanString(key))
		} else {
			logger.Error("CAS failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
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
		// The client.Delete method already returns nil if the key is not found (matching server behavior).
		// So, specific client.ErrKeyNotFound check might not be triggered here unless client.Delete changes.
		// However, maintaining consistency in error logging if other types of errors occur.
		logger.Error("Delete failed", "key", key, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	// logger.Info("Delete successful", "key", key) // Redundant
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
	offset, limit := 0, 100 // Defaults

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
	// logger.Info("Iterate successful", "type", iterType, "value", value, "offset", offset, "limit", limit, "count", len(results))
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
				// logger.Error("Cache get failed: Key not found", "key", key) // Already handled
				fmt.Fprintf(os.Stderr, "%s Key '%s' not found in cache.\n", color.RedString("Error:"), color.CyanString(key))
			} else {
				logger.Error("Cache get failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err) // May include "not found" type errors from client
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
		key := subArgs[0] // redefine key for this scope
		err := c.DeleteCache(key)
		if err != nil {
			if errors.Is(err, client.ErrKeyNotFound) {
				logger.Info("Cache delete: Key not found, no action taken.", "key", key) // Info level as it's not strictly an error
				fmt.Fprintf(os.Stderr, "%s Key '%s' not found in cache. Nothing to delete.\n", color.YellowString("Warning:"), color.CyanString(key))
				// os.Exit(0) // Or exit successfully
			} else {
				logger.Error("Cache delete failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1) // Exit with error unless it was ErrKeyNotFound and we decided to exit 0 above
		}
		// logger.Info("Cache delete successful", "key", key) // Redundant
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
				fmt.Fprintf(os.Stderr, "%s Cache compare-and-swap failed for key '%s'.\n", color.RedString("Precondition Failed:"), color.CyanString(key))
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
	offset, limit := 0, 100 // Defaults

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
		logger.Error("Follower node ID not found in configuration", "follower_node_id", followerNodeID)
		fmt.Fprintf(os.Stderr, "%s Follower node ID '%s' not found in configuration.\n", color.RedString("Error:"), color.CyanString(followerNodeID))
		os.Exit(1)
	}

	// The Join method in the client expects the Raft address of the follower
	logger.Info("Attempting to join follower", "follower_id", color.CyanString(followerNodeID), "follower_raft_addr", followerDetails.RaftBinding, "via_leader", color.CyanString(leaderNodeID))

	err = leaderClient.Join(followerNodeID, followerDetails.RaftBinding)
	if err != nil {
		logger.Error("Join failed", "leader_node", leaderNodeID, "follower_node", followerNodeID, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	logger.Info("Join successful", "leader_node", color.CyanString(leaderNodeID), "follower_node", color.CyanString(followerNodeID))
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
	// logger.Info("Ping successful") // Redundant with response
	// Pretty print the map
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
		handleApiLimits(c, subArgs)
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

	// We need to create a new client instance with the provided API key.
	// Re-use logic from getClient for node details but override API key.
	nodeToConnect := targetNode // Use the global targetNode flag
	if nodeToConnect == "" {
		if clusterCfg.DefaultLeader == "" {
			logger.Error("api verify: targetNode is empty and no DefaultLeader is set in config")
			fmt.Fprintf(os.Stderr, "%s Target node must be specified via --target or DefaultLeader in config.\n", color.RedString("Error:"))
			os.Exit(1)
		}
		nodeToConnect = clusterCfg.DefaultLeader
		logger.Info("No target node specified for verify, using DefaultLeader", "node_id", color.CyanString(nodeToConnect))
	}

	nodeDetails, ok := clusterCfg.Nodes[nodeToConnect]
	if !ok {
		logger.Error("api verify: node ID not found in configuration", "node_id", nodeToConnect)
		fmt.Fprintf(os.Stderr, "%s Node ID '%s' not found in configuration.\n", color.RedString("Error:"), color.CyanString(nodeToConnect))
		os.Exit(1)
	}

	verifyClientLogger := logger.WithGroup("verify_client")

	verifyCli, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeDirect,
		Endpoints: []client.Endpoint{
			{
				HostPort:     nodeDetails.HttpBinding,
				ClientDomain: nodeDetails.ClientDomain,
			},
		},
		ApiKey:     apiKeyToVerify, // Use the key passed as argument
		SkipVerify: clusterCfg.ClientSkipVerify,
		Logger:     verifyClientLogger,
	})
	if err != nil {
		logger.Error("api verify: failed to create client for verification", "target_node", nodeToConnect, "error", err)
		fmt.Fprintf(os.Stderr, "%s Failed to create client for verification: %v\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	logger.Info("Attempting to verify API key with a ping...", "target_node", nodeToConnect)
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

func handleApiLimits(c *client.Client, args []string) {
	if len(args) != 0 {
		logger.Error("api limits: does not take arguments")
		printUsage()
		os.Exit(1)
	}

	resp, err := c.GetLimits()
	if err != nil {
		logger.Error("Failed to get API key limits", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	fmt.Println(color.CyanString("Maximum Limits:"))
	fmt.Printf("  Bytes on Disk:     %d\n", *resp.MaxLimits.BytesOnDisk)
	fmt.Printf("  Bytes in Memory:   %d\n", *resp.MaxLimits.BytesInMemory)
	fmt.Printf("  Events per Second: %d\n", *resp.MaxLimits.EventsPerSecond)
	fmt.Printf("  Subscribers:       %d\n", *resp.MaxLimits.Subscribers)
	fmt.Println(color.CyanString("\nCurrent Usage:"))
	fmt.Printf("  Bytes on Disk:     %d\n", *resp.Current.BytesOnDisk)
	fmt.Printf("  Bytes in Memory:   %d\n", *resp.Current.BytesInMemory)
	fmt.Printf("  Events per Second: %d\n", *resp.Current.EventsPerSecond)
	fmt.Printf("  Subscribers:       %d\n", *resp.Current.Subscribers)
}

func handleApiSetLimits(c *client.Client, args []string) {
	setLimitsCmd := flag.NewFlagSet("set-limits", flag.ExitOnError)
	disk := setLimitsCmd.Int64("disk", -1, "Max bytes on disk")
	mem := setLimitsCmd.Int64("mem", -1, "Max bytes in memory")
	events := setLimitsCmd.Int64("events", -1, "Max events per second")
	subs := setLimitsCmd.Int64("subs", -1, "Max subscribers")

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
		limits.EventsPerSecond = events
	}
	if *subs != -1 {
		limits.Subscribers = subs
	}

	err := c.SetLimits(apiKey, limits)
	if err != nil {
		logger.Error("Failed to set API key limits", "key", apiKey, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	color.HiGreen("OK")
}

// --- Object Command Handlers ---
func handleObject(c *client.Client, args []string) {
	if len(args) < 1 {
		logger.Error("object: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}
	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "upload":
		handleObjectUpload(c, subArgs)
	case "download":
		handleObjectDownload(c, subArgs)
	case "hash":
		handleObjectHash(c, subArgs)
	default:
		logger.Error("object: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleObjectUpload(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("object upload: requires <filepath>")
		printUsage()
		os.Exit(1)
	}
	filePath := args[0]
	resp, err := c.ObjectUpload(filePath)
	if err != nil {
		logger.Error("Object upload failed", "file", filePath, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	fmt.Printf("Object uploaded successfully:\n")
	fmt.Printf("  ObjectID: %s\n", color.CyanString(resp.ObjectID))
	if resp.Message != "" {
		fmt.Printf("  Message: %s\n", resp.Message)
	}
}

func handleObjectDownload(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("object download: requires <uuid> <output_path>")
		printUsage()
		os.Exit(1)
	}
	uuid := args[0]
	outputPath := args[1]
	err := c.ObjectDownload(uuid, outputPath)
	if err != nil {
		logger.Error("Object download failed", "uuid", uuid, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
}

func handleObjectHash(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("object hash: requires <uuid>")
		printUsage()
		os.Exit(1)
	}
	uuid := args[0]
	resp, err := c.ObjectGetHash(uuid)
	if err != nil {
		logger.Error("Object hash failed", "uuid", uuid, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	fmt.Printf("Object Hash:\n")
	fmt.Printf("  ObjectID: %s\n", color.CyanString(resp.ObjectID))
	fmt.Printf("  SHA256:   %s\n", color.GreenString(resp.Sha256))
}
