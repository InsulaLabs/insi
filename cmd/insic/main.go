package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	logger.Info("Loading configuration", "path", path)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg config.Cluster
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config data from %s: %w", path, err)
	}
	logger.Info("Configuration loaded successfully")
	return &cfg, nil
}

func getClient(cfg *config.Cluster, targetNodeID string) (*client.Client, error) {
	nodeToConnect := targetNodeID
	if nodeToConnect == "" {
		if cfg.DefaultLeader == "" {
			return nil, fmt.Errorf("targetNodeID is empty and no DefaultLeader is set in config")
		}
		nodeToConnect = cfg.DefaultLeader
		logger.Info("No target node specified, using DefaultLeader", "node_id", nodeToConnect)
	}

	nodeDetails, ok := cfg.Nodes[nodeToConnect]
	if !ok {
		return nil, fmt.Errorf("node ID '%s' not found in configuration", nodeToConnect)
	}

	clientLogger := logger.WithGroup("client")

	clientLogger.Info("Client is using instanceSecret for token generation", "secret_value", cfg.InstanceSecret)

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
		HostPort:     nodeDetails.HttpBinding,
		ApiKey:       apiKey,
		SkipVerify:   cfg.ClientSkipVerify,
		ClientDomain: nodeDetails.ClientDomain,
		Logger:       clientLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client for node %s (%s): %w", nodeToConnect, nodeDetails.HttpBinding, err)
	}
	logger.Info("Client created successfully", "target_node", nodeToConnect, "hostport", nodeDetails.HttpBinding)
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
		cli, err = getClient(clusterCfg, targetNode) // Use targetNode from flag
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
	case "delete":
		handleDelete(cli, cmdArgs)
	case "tag":
		handleTag(cli, cmdArgs)
	case "untag":
		handleUntag(cli, cmdArgs)
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
	default:
		logger.Error("Unknown command", "command", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: insic [flags] <command> [args...]\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nCommands:\n")
	fmt.Fprintf(os.Stderr, "  get <key>\n")
	fmt.Fprintf(os.Stderr, "  set <key> <value>\n")
	fmt.Fprintf(os.Stderr, "  delete <key>\n")
	fmt.Fprintf(os.Stderr, "  tag <key> <tag>\n")
	fmt.Fprintf(os.Stderr, "  untag <key> <tag>\n")
	fmt.Fprintf(os.Stderr, "  iterate prefix <prefix> [offset] [limit]\n")
	fmt.Fprintf(os.Stderr, "  iterate tag <tag> [offset] [limit]\n")
	fmt.Fprintf(os.Stderr, "  cache set <key> <value> <ttl (e.g., 60s, 5m, 1h)>\n")
	fmt.Fprintf(os.Stderr, "  cache get <key>\n")
	fmt.Fprintf(os.Stderr, "  cache delete <key>\n")
	fmt.Fprintf(os.Stderr, "  join <leaderNodeID> <followerNodeID>\n")
	fmt.Fprintf(os.Stderr, "  api add <entity_name>\n")
	fmt.Fprintf(os.Stderr, "  api delete <api_key_value>\n")
	fmt.Fprintf(os.Stderr, "  api verify <api_key_value>\n")
	fmt.Fprintf(os.Stderr, "  ping\n")
	fmt.Fprintf(os.Stderr, "  publish <topic> <data>\n")
	fmt.Fprintf(os.Stderr, "  subscribe <topic>\n")
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
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	logger.Info("Publish successful", "topic", topic)
	fmt.Println("OK")
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
		fmt.Printf("Received event on topic '%s': %+v\n", topic, data)
	}

	logger.Info("Attempting to subscribe to events", "topic", topic)
	err := c.SubscribeToEvents(topic, ctx, cb)
	if err != nil {
		// context.Canceled is an expected error on graceful shutdown, others are not.
		if err == context.Canceled {
			logger.Info("Subscription cancelled gracefully.", "topic", topic)
		} else {
			logger.Error("Subscription failed", "topic", topic, "error", err)
			fmt.Println("Error:", err)
			// os.Exit(1) // Exiting here might be too abrupt if there's a non-critical error during teardown.
		}
	}
	logger.Info("Subscription process finished.", "topic", topic)
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
		logger.Error("Get failed", "key", key, "error", err)
		fmt.Println("Error:", err)
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
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	logger.Info("Set successful", "key", key)
	fmt.Println("OK")
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
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	logger.Info("Delete successful", "key", key)
	fmt.Println("OK")
}

func handleTag(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("tag: requires <key> <tag>")
		printUsage()
		os.Exit(1)
	}
	key, tag := args[0], args[1]
	err := c.Tag(key, tag)
	if err != nil {
		logger.Error("Tag failed", "key", key, "tag", tag, "error", err)
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	logger.Info("Tag successful", "key", key, "tag", tag)
	fmt.Println("OK")
}

func handleUntag(c *client.Client, args []string) {
	if len(args) != 2 {
		logger.Error("untag: requires <key> <tag>")
		printUsage()
		os.Exit(1)
	}
	key, tag := args[0], args[1]
	err := c.Untag(key, tag)
	if err != nil {
		logger.Error("Untag failed", "key", key, "tag", tag, "error", err)
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	logger.Info("Untag successful", "key", key, "tag", tag)
	fmt.Println("OK")
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
			os.Exit(1)
		}
	}
	if len(args) > 3 {
		limit, err = strconv.Atoi(args[3])
		if err != nil {
			logger.Error("iterate: invalid limit", "limit_str", args[3], "error", err)
			os.Exit(1)
		}
	}

	var results []string
	switch iterType {
	case "prefix":
		results, err = c.IterateByPrefix(value, offset, limit)
	case "tag":
		results, err = c.IterateByTag(value, offset, limit)
	default:
		logger.Error("iterate: unknown type", "type", iterType)
		printUsage()
		os.Exit(1)
		return
	}

	if err != nil {
		logger.Error("Iterate failed", "type", iterType, "value", value, "error", err)
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	logger.Info("Iterate successful", "type", iterType, "value", value, "offset", offset, "limit", limit, "count", len(results))
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
			fmt.Println("Error: Invalid TTL format. Use format like '60s', '5m', '1h'.")
			os.Exit(1)
		}
		err = c.SetCache(key, valStr, ttl)
		if err != nil {
			logger.Error("Cache set failed", "key", key, "error", err)
			fmt.Println("Error:", err)
			os.Exit(1)
		}
		logger.Info("Cache set successful", "key", key, "ttl", ttl)
		fmt.Println("OK")
	case "get":
		if len(subArgs) != 1 {
			logger.Error("cache get: requires <key>")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]
		value, err := c.GetCache(key)
		if err != nil {
			logger.Error("Cache get failed", "key", key, "error", err)
			fmt.Println("Error:", err) // May include "not found" type errors from client
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
			logger.Error("Cache delete failed", "key", key, "error", err)
			fmt.Println("Error:", err)
			os.Exit(1)
		}
		logger.Info("Cache delete successful", "key", key)
		fmt.Println("OK")
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
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	followerDetails, ok := clusterCfg.Nodes[followerNodeID]
	if !ok {
		logger.Error("Follower node ID not found in configuration", "follower_node_id", followerNodeID)
		fmt.Printf("Error: Follower node ID '%s' not found in configuration.\n", followerNodeID)
		os.Exit(1)
	}

	// The Join method in the client expects the Raft address of the follower
	logger.Info("Attempting to join follower", "follower_id", followerNodeID, "follower_raft_addr", followerDetails.RaftBinding, "via_leader", leaderNodeID)

	err = leaderClient.Join(followerNodeID, followerDetails.RaftBinding)
	if err != nil {
		logger.Error("Join failed", "leader_node", leaderNodeID, "follower_node", followerNodeID, "error", err)
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	logger.Info("Join successful", "leader_node", leaderNodeID, "follower_node", followerNodeID)
	fmt.Println("OK")
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
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	logger.Info("API key created successfully", "entity", entityName)
	fmt.Printf("API Key: %s\n", apiKey)
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
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	logger.Info("API key deleted successfully", "key", apiKey)
	fmt.Println("OK")
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
			fmt.Println("Error: Cannot verify API key. No target node specified via --target and no DefaultLeader in config.")
			os.Exit(1)
		}
		nodeToConnect = cfg.DefaultLeader
		logger.Info("No --target specified for API key verification, using DefaultLeader", "node_id", nodeToConnect)
	} else {
		logger.Info("Using --target for API key verification", "node_id", nodeToConnect)
	}

	nodeDetails, ok := cfg.Nodes[nodeToConnect]
	if !ok {
		logger.Error("Cannot verify API key: Target node details not found in configuration.", "node_id", nodeToConnect)
		fmt.Printf("Error: Node ID '%s' (from --target or DefaultLeader) not found in configuration.\\n", nodeToConnect)
		os.Exit(1)
	}

	verificationClientLogger := logger.WithGroup("api-verify-client")
	verificationClient, err := client.NewClient(&client.Config{
		HostPort:     nodeDetails.HttpBinding,
		ApiKey:       apiKeyToVerify,
		SkipVerify:   cfg.ClientSkipVerify,
		ClientDomain: nodeDetails.ClientDomain,
		Logger:       verificationClientLogger,
	})
	if err != nil {
		// This typically means the apiKeyToVerify was not in the correct format (e.g. hex encoded sha256)
		// or hostPort was invalid, NewClient would catch empty apiKeyToVerify but good to be mindful.
		logger.Error("Failed to create client for API key verification", "error", err)
		fmt.Println("Error: Could not initialize client for verification. Ensure API key is valid and server is reachable.")
		os.Exit(1)
	}

	resp, err := verificationClient.Ping()
	if err != nil {
		logger.Error("API key verification ping failed", "key", apiKeyToVerify, "error", err)
		fmt.Printf("API Key '%s' is NOT valid or server is unreachable. Error: %s\n", apiKeyToVerify, err)
		os.Exit(1)
	}

	logger.Info("API key verification ping successful", "key", apiKeyToVerify)
	fmt.Printf("API Key '%s' IS valid.\n", apiKeyToVerify)
	fmt.Println("Ping Response from server:")
	for k, v := range resp {
		fmt.Printf("  %s: %s\n", k, v)
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
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	logger.Info("Ping successful")
	// Pretty print the map
	fmt.Println("Ping Response:")
	for k, v := range resp {
		fmt.Printf("  %s: %s\n", k, v)
	}
}
