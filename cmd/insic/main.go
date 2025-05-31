package main

import (
	"context"
	"crypto/sha256"
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
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/internal/config" // Assuming this is the correct path
	"github.com/InsulaLabs/insi/models"
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
		logger.Info("No target node specified, using DefaultLeader", "node_id", color.CyanString(nodeToConnect))
	}

	nodeDetails, ok := cfg.Nodes[nodeToConnect]
	if !ok {
		return nil, fmt.Errorf("node ID '%s' not found in configuration", nodeToConnect)
	}

	clientLogger := logger.WithGroup("client")

	// clientLogger.Info("Client is using instanceSecret for token generation", "secret_value", cfg.InstanceSecret) // Too verbose for default

	var apiKey string

	if useRootKey {
		secretHash := sha256.New()
		secretHash.Write([]byte(cfg.InstanceSecret))
		apiKey = hex.EncodeToString(secretHash.Sum(nil))
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
	if command != "join" && command != "api" { // 'join' and 'api verify' command handles its client creation specifically
		cli, err = getClient(clusterCfg, targetNode) // Use targetNode from flag
		if err != nil {
			logger.Error("Failed to initialize default API client", "error", err)
			os.Exit(1)
		}
	} else if command == "api" && len(args) > 1 && args[1] != "verify" { // For api add/delete etc.
		cli, err = getClient(clusterCfg, targetNode)
		if err != nil {
			logger.Error("Failed to initialize default API client for API operation", "error", err)
			os.Exit(1)
		}
	}

	switch command {
	case "get":
		handleGet(cli, cmdArgs)
	case "set":
		handleSet(cli, cmdArgs)
	case "delete":
		handleDelete(cli, cmdArgs)
	case "iterate":
		handleIterate(cli, cmdArgs)
	case "cache":
		handleCache(cli, cmdArgs)
	case "join":
		handleJoin(cmdArgs) // Special handling as it targets a specific leader
	case "api":
		handleApi(cli, cmdArgs)
	case "ping":
		handlePing(cli, cmdArgs)
	case "publish":
		handlePublish(cli, cmdArgs)
	case "subscribe":
		handleSubscribe(cli, cmdArgs)
	case "etok": // New top-level command for etok operations
		handleEtok(cli, cmdArgs)
	case "object": // New top-level command for object operations
		handleObject(cli, cmdArgs)
	case "batchset":
		handleBatchSet(cli, cmdArgs)
	case "batchdelete":
		handleBatchDelete(cli, cmdArgs)
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
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("iterate"), color.CyanString("prefix"), color.CyanString("<prefix>"), color.CyanString("[offset]"), color.CyanString("[limit]"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("cache"), color.CyanString("set"), color.CyanString("<key>"), color.CyanString("<value>"), color.CyanString("<ttl (e.g., 60s, 5m, 1h)>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("cache"), color.CyanString("get"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("cache"), color.CyanString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("join"), color.CyanString("<leaderNodeID>"), color.CyanString("<followerNodeID>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("api"), color.CyanString("add"), color.CyanString("<entity_name>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("api"), color.CyanString("delete"), color.CyanString("<api_key_value>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("api"), color.CyanString("verify"), color.CyanString("<api_key_value>"))
	fmt.Fprintf(os.Stderr, "  %s\n", color.GreenString("ping"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("publish"), color.CyanString("<topic>"), color.CyanString("<data>"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("subscribe"), color.CyanString("<topic>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s %s\n", color.GreenString("etok"), color.CyanString("request"), color.YellowString("--scopes"), color.CyanString("<json_scopes>"), color.YellowString("--ttl"), color.CyanString("<duration>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s %s\n", color.GreenString("etok"), color.CyanString("verify"), color.YellowString("--token"), color.CyanString("<token>"), color.YellowString("--scopes"), color.CyanString("<json_scopes>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("object"), color.CyanString("set"), color.CyanString("<key>"), color.CyanString("<filepath>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("object"), color.CyanString("get"), color.CyanString("<key>"), color.CyanString("<output_filepath>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("object"), color.CyanString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("object"), color.CyanString("list"), color.CyanString("[prefix]"), color.CyanString("[offset]"), color.CyanString("[limit]"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("batchset"), color.CyanString("<filepath.json>"))
	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("batchdelete"), color.CyanString("<filepath.json>"))
}

func handlePublish(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("publish: requires <topic> <data>")
		printUsage()
		os.Exit(1)
	}
	topic := args[0]
	data := args[1]
	err := c.PublishEvent(topic, data)
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
		if len(subArgs) != 3 {
			logger.Error("cache set: requires <key> <value> <ttl>")
			printUsage()
			os.Exit(1)
		}
		key, valStr, ttlStr := subArgs[0], subArgs[1], subArgs[2]
		ttl, err := time.ParseDuration(ttlStr)
		if err != nil {
			logger.Error("cache set: invalid TTL format", "ttl_str", ttlStr, "error", err)
			fmt.Fprintf(os.Stderr, "%s Invalid TTL format. Use format like '60s', '5m', '1h'.\n", color.RedString("Error:"))
			os.Exit(1)
		}
		err = c.SetCache(key, valStr, ttl)
		if err != nil {
			logger.Error("Cache set failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		// logger.Info("Cache set successful", "key", key, "ttl", ttl) // Redundant
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
	default:
		logger.Error("cache: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
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
		handleAddApiKey(c, subArgs)
	case "delete":
		handleDeleteApiKey(c, subArgs)
	case "verify":
		handleVerifyApiKey(clusterCfg, subArgs)
	default:
		logger.Error("api: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleAddApiKey(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("api add: requires <entity_name>")
		printUsage()
		os.Exit(1)
	}
	entityName := args[0]
	apiKey, err := c.NewAPIKey(entityName)
	if err != nil {
		logger.Error("API key creation failed", "entity", entityName, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	logger.Info("API key created successfully", "entity", entityName)
	fmt.Printf("API Key: %s\n", color.CyanString(apiKey))
}

func handleDeleteApiKey(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("api delete: requires <api_key_value>")
		printUsage()
		os.Exit(1)
	}
	apiKey := args[0]
	err := c.DeleteAPIKey(apiKey)
	if err != nil {
		logger.Error("API key deletion failed", "key", apiKey, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	// logger.Info("API key deleted successfully", "key", apiKey) // Redundant
	color.HiGreen("OK")
}

func handleVerifyApiKey(cfg *config.Cluster, args []string) {
	if len(args) != 1 {
		logger.Error("api verify: requires <api_key_value>")
		printUsage()
		os.Exit(1)
	}
	apiKeyToVerify := args[0]

	// Create a new client instance specifically for this verification,
	// using the provided API key as the authToken.
	nodeToConnect := targetNode // Use the global targetNode flag first
	if nodeToConnect == "" {    // If --target was not specified
		if cfg.DefaultLeader == "" {
			logger.Error("Cannot verify API key: --target flag not set and DefaultLeader not set in config and no specific node given for verification.")
			fmt.Fprintf(os.Stderr, "%s Cannot verify API key. No target node specified via --target and no DefaultLeader in config.\n", color.RedString("Error:"))
			os.Exit(1)
		}
		nodeToConnect = cfg.DefaultLeader
		logger.Info("No --target specified for API key verification, using DefaultLeader", "node_id", color.CyanString(nodeToConnect))
	} else {
		logger.Info("Using --target for API key verification", "node_id", color.CyanString(nodeToConnect))
	}

	nodeDetails, ok := cfg.Nodes[nodeToConnect]
	if !ok {
		logger.Error("Cannot verify API key: Target node details not found in configuration.", "node_id", nodeToConnect)
		fmt.Fprintf(os.Stderr, "%s Node ID '%s' (from --target or DefaultLeader) not found in configuration.\n", color.RedString("Error:"), color.CyanString(nodeToConnect))
		os.Exit(1)
	}

	verificationClientLogger := logger.WithGroup("api-verify-client")
	verificationClient, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeDirect,
		Endpoints: []client.Endpoint{
			{
				HostPort:     nodeDetails.HttpBinding,
				ClientDomain: nodeDetails.ClientDomain,
			},
		},
		ApiKey:     apiKeyToVerify,
		SkipVerify: cfg.ClientSkipVerify,
		Logger:     verificationClientLogger,
	})
	if err != nil {
		// This typically means the apiKeyToVerify was not in the correct format (e.g. hex encoded sha256)
		// or hostPort was invalid, NewClient would catch empty apiKeyToVerify but good to be mindful.
		logger.Error("Failed to create client for API key verification", "error", err)
		fmt.Fprintf(os.Stderr, "%s Could not initialize client for verification. Ensure API key is valid and server is reachable.\n", color.RedString("Error:"))
		os.Exit(1)
	}

	resp, err := verificationClient.Ping()
	if err != nil {
		logger.Error("API key verification ping failed", "key", apiKeyToVerify, "error", err)
		fmt.Fprintf(os.Stderr, "API Key '%s' is %s. Error: %s\n", color.CyanString(apiKeyToVerify), color.RedString("NOT valid or server is unreachable"), err)
		os.Exit(1)
	}

	logger.Info("API key verification ping successful", "key", apiKeyToVerify)
	fmt.Printf("API Key '%s' IS %s.\n", color.CyanString(apiKeyToVerify), color.HiGreenString("valid"))
	fmt.Println("Ping Response from server:")
	for k, v := range resp {
		fmt.Printf("  %s: %s\n", color.CyanString(k), v)
	}
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

// --- ETOK Command Handling ---

func handleEtok(c *client.Client, args []string) {
	if len(args) < 1 {
		logger.Error("etok: requires a sub-command (request|verify)")
		printUsage()
		os.Exit(1)
	}

	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "request":
		handleEtokRequest(c, subArgs)
	case "verify":
		handleEtokVerify(c, subArgs)
	default:
		logger.Error("etok: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func parseScopes(scopesStr string) (map[string]string, error) {
	var scopes map[string]string
	if err := json.Unmarshal([]byte(scopesStr), &scopes); err != nil {
		return nil, fmt.Errorf("invalid JSON format for scopes: %w. Expected map, e.g., {\"read\":\"data\"}", err)
	}
	return scopes, nil
}

func handleEtokRequest(c *client.Client, args []string) {
	fs := flag.NewFlagSet("etok request", flag.ExitOnError)
	scopesStr := fs.String("scopes", "", "JSON string of scopes (e.g., '{\"resource\":\"id\", \"action\":\"read\"}')")
	ttlStr := fs.String("ttl", "", "Token time-to-live (e.g., '1m', '1h')")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: insic etok request --scopes <json_scopes> --ttl <duration>\n")
		fs.PrintDefaults()
	}
	fs.Parse(args)

	if *scopesStr == "" {
		logger.Error("etok request: --scopes is required")
		fs.Usage()
		os.Exit(1)
	}
	if *ttlStr == "" {
		logger.Error("etok request: --ttl is required")
		fs.Usage()
		os.Exit(1)
	}

	scopes, err := parseScopes(*scopesStr)
	if err != nil {
		logger.Error("etok request: failed to parse scopes", "error", err)
		fmt.Fprintf(os.Stderr, "%s parsing scopes: %v\n", color.RedString("Error"), err)
		os.Exit(1)
	}

	ttl, err := time.ParseDuration(*ttlStr)
	if err != nil {
		logger.Error("etok request: invalid TTL format", "ttl_str", *ttlStr, "error", err)
		fmt.Fprintf(os.Stderr, "%s Invalid TTL format for --ttl: %v. Use format like '60s', '5m', '1h'.\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	token, err := c.RequestScopeToken(scopes, ttl)
	if err != nil {
		if errors.Is(err, client.ErrTokenNotFound) { // Should not happen on request, but good practice
			logger.Error("Etok request failed: Token not found", "error", err)
			fmt.Fprintf(os.Stderr, "%s Token not found during request processing.\n", color.RedString("Error:"))
		} else if errors.Is(err, client.ErrTokenInvalid) { // Should not happen on request
			logger.Error("Etok request failed: Token invalid", "error", err)
			fmt.Fprintf(os.Stderr, "%s Token invalid during request processing.\n", color.RedString("Error:"))
		} else {
			logger.Error("Etok request failed", "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}

	logger.Info("Etok request successful")
	fmt.Printf("Token: %s\n", color.CyanString(token))
}

func handleEtokVerify(c *client.Client, args []string) {
	fs := flag.NewFlagSet("etok verify", flag.ExitOnError)
	tokenStr := fs.String("token", "", "The etok token string to verify")
	scopesStr := fs.String("scopes", "{}", "JSON string of scopes being requested/asserted (e.g., '{\"resource\":\"id\"}'). Defaults to empty map for general validity check.")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: insic etok verify --token <token> --scopes <json_scopes>\n")
		fs.PrintDefaults()
	}
	fs.Parse(args)

	if *tokenStr == "" {
		logger.Error("etok verify: --token is required")
		fs.Usage()
		os.Exit(1)
	}

	scopes, err := parseScopes(*scopesStr)
	if err != nil {
		logger.Error("etok verify: failed to parse scopes", "error", err)
		fmt.Fprintf(os.Stderr, "%s parsing scopes for --scopes: %v\n", color.RedString("Error"), err)
		os.Exit(1)
	}

	verified, err := c.VerifyScopeToken(*tokenStr, scopes)
	if err != nil {
		// This error from client might already indicate not verified (e.g. 401 from server)
		// or specific token errors like ErrTokenNotFound or ErrTokenInvalid
		if errors.Is(err, client.ErrTokenNotFound) {
			logger.Error("Etok verification failed: Token not found", "token", *tokenStr, "scopes_requested", scopes, "error", err)
			fmt.Fprintf(os.Stderr, "Token '%s' was %s. Error: %s\n", color.CyanString(*tokenStr), color.RedString("not found"), err)
		} else if errors.Is(err, client.ErrTokenInvalid) {
			logger.Error("Etok verification failed: Token invalid", "token", *tokenStr, "scopes_requested", scopes, "error", err)
			fmt.Fprintf(os.Stderr, "Token '%s' is %s. Error: %s\n", color.CyanString(*tokenStr), color.RedString("invalid"), err)
		} else {
			logger.Error("Etok verification failed or token invalid", "token", *tokenStr, "scopes_requested", scopes, "error", err)
			fmt.Fprintf(os.Stderr, "Token '%s' is %s for the requested scopes. Error: %s\n", color.CyanString(*tokenStr), color.RedString("NOT valid"), err)
		}
		os.Exit(1) // Exit because the verification process itself encountered an error.
	}

	if verified {
		logger.Info("Etok verification successful", "token", *tokenStr, "scopes_requested", scopes)
		fmt.Printf("Token '%s' IS %s for the requested scopes.\n", color.CyanString(*tokenStr), color.HiGreenString("valid"))
	} else {
		// This case might be less common if server returns non-200 for invalid tokens, caught by `err != nil` above.
		// However, if server returns 200 OK with `{"verified": false}`, this handles it.
		logger.Warn("Etok verification returned false", "token", *tokenStr, "scopes_requested", scopes)
		fmt.Fprintf(os.Stderr, "Token '%s' is %s for the requested scopes (server indicated not verified).\n", color.CyanString(*tokenStr), color.RedString("NOT valid"))
		os.Exit(1) // Exit with error code for not verified.
	}
}

func handleObject(c *client.Client, args []string) {
	if len(args) < 1 {
		logger.Error("object: requires a sub-command (set|get|delete|list)")
		printUsage()
		os.Exit(1)
	}

	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "set":
		handleObjectSet(c, subArgs)
	case "get":
		handleObjectGet(c, subArgs)
	case "delete":
		handleObjectDelete(c, subArgs)
	case "list":
		handleObjectList(c, subArgs)
	default:
		logger.Error("object: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleObjectSet(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("object set: requires <key> <filepath>")
		printUsage()
		os.Exit(1)
	}
	key := args[0]
	filePath := args[1]

	objectData, err := os.ReadFile(filePath)
	if err != nil {
		logger.Error("object set: failed to read file", "filepath", filePath, "error", err)
		fmt.Fprintf(os.Stderr, "%s reading file %s: %v\n", color.RedString("Error"), color.CyanString(filePath), err)
		os.Exit(1)
	}

	err = c.SetObject(key, objectData)
	if err != nil {
		logger.Error("object set: failed to set object", "key", key, "error", err)
		fmt.Fprintf(os.Stderr, "%s setting object %s: %v\n", color.RedString("Error"), color.CyanString(key), err)
		os.Exit(1)
	}

	color.HiGreen("OK")
}

func handleObjectGet(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("object get: requires <key> <output_filepath>")
		printUsage()
		os.Exit(1)
	}
	key := args[0]
	outputFilePath := args[1]

	objectData, err := c.GetObject(key)
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			logger.Error("object get: key not found", "key", key)
			fmt.Fprintf(os.Stderr, "%s Object with key '%s' not found.\n", color.RedString("Error:"), color.CyanString(key))
		} else {
			logger.Error("object get: failed to get object", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s getting object %s: %v\n", color.RedString("Error"), color.CyanString(key), err)
		}
		os.Exit(1)
	}

	err = os.WriteFile(outputFilePath, objectData, 0644)
	if err != nil {
		logger.Error("object get: failed to write output file", "filepath", outputFilePath, "error", err)
		fmt.Fprintf(os.Stderr, "%s writing to output file %s: %v\n", color.RedString("Error"), color.CyanString(outputFilePath), err)
		os.Exit(1)
	}

	fmt.Printf("Object data for key '%s' written to %s\n", color.CyanString(key), color.GreenString(outputFilePath))
}

func handleObjectDelete(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("object delete: requires <key>")
		printUsage()
		os.Exit(1)
	}
	key := args[0]
	err := c.DeleteObject(key)
	if err != nil {
		// The client.DeleteObject method already returns nil if the key is not found (matching server behavior).
		// So, specific client.ErrKeyNotFound check might not be triggered here unless client.DeleteObject changes.
		// However, maintaining consistency in error logging if other types of errors occur.
		logger.Error("Object delete failed", "key", key, "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	// logger.Info("Object delete successful", "key", key) // Redundant
	color.HiGreen("OK")
}

func handleObjectList(c *client.Client, args []string) {
	prefix := ""
	offset, limit := 0, 100 // Defaults

	if len(args) > 0 {
		prefix = args[0]
	}
	if len(args) > 1 {
		offset, _ = strconv.Atoi(args[1])
	}
	if len(args) > 2 {
		limit, _ = strconv.Atoi(args[2])
	}

	keys, err := c.GetObjectList(prefix, offset, limit)
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			logger.Warn("Object list: No keys found matching criteria", "prefix", prefix, "offset", offset, "limit", limit)
			color.HiRed("No keys found.")
		} else {
			logger.Error("Object list failed", "prefix", prefix, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	// logger.Info("Object list successful", "prefix", prefix, "offset", offset, "limit", limit, "count", len(keys))
	for _, item := range keys {
		fmt.Println(item)
	}
}

func handleBatchSet(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("batchset: requires <filepath.json>")
		printUsage()
		os.Exit(1)
	}
	filePath := args[0]
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		logger.Error("batchset: failed to read file", "filepath", filePath, "error", err)
		fmt.Fprintf(os.Stderr, "%s reading file %s: %v\n", color.RedString("Error"), color.CyanString(filePath), err)
		os.Exit(1)
	}

	var items []models.KVPayload
	if err := json.Unmarshal(fileData, &items); err != nil {
		logger.Error("batchset: failed to unmarshal JSON from file", "filepath", filePath, "error", err)
		fmt.Fprintf(os.Stderr, "%s unmarshaling JSON from %s: %v\n\t\tExpected format: [ { \"key\": \"k1\", \"value\": \"v1\" }, { \"key\": \"k2\", \"value\": \"v2\" } ]\n",
			color.RedString("Error"), color.CyanString(filePath), err)
		os.Exit(1)
	}

	err = c.BatchSet(items)
	if err != nil {
		logger.Error("Batch set failed", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	// logger.Info("Batch set successful") // Redundant
	color.HiGreen("OK")
}

func handleBatchDelete(c *client.Client, args []string) {
	if len(args) != 1 {
		logger.Error("batchdelete: requires <filepath.json>")
		printUsage()
		os.Exit(1)
	}
	filePath := args[0]
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		logger.Error("batchdelete: failed to read file", "filepath", filePath, "error", err)
		fmt.Fprintf(os.Stderr, "%s reading file %s: %v\n", color.RedString("Error"), color.CyanString(filePath), err)
		os.Exit(1)
	}

	var keys []string
	if err := json.Unmarshal(fileData, &keys); err != nil {
		logger.Error("batchdelete: failed to unmarshal JSON from file", "filepath", filePath, "error", err)
		fmt.Fprintf(os.Stderr, "%s unmarshaling JSON from %s: %v\n\t\tExpected format: [ \"key1\", \"key2\", \"key3\" ]\n",
			color.RedString("Error"), color.CyanString(filePath), err)
		os.Exit(1)
	}

	if len(keys) == 0 {
		logger.Error("batchdelete: no keys found in JSON file", "filepath", filePath)
		fmt.Fprintf(os.Stderr, "%s No keys to delete in %s.\n", color.RedString("Error"), color.CyanString(filePath))
		os.Exit(1)
	}

	err = c.BatchDelete(keys)
	if err != nil {
		logger.Error("batchdelete: failed to delete batch", "error", err)
		fmt.Fprintf(os.Stderr, "%s deleting batch: %v\n", color.RedString("Error"), err)
		os.Exit(1)
	}

	color.HiGreen("OK (%d keys processed)", len(keys))
}
