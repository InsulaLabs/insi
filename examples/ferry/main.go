package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/ferry"
	"github.com/fatih/color"
	"gopkg.in/yaml.v3"
)

type FerryConfig struct {
	ApiKeyEnv  string   `yaml:"api_key_env"`
	Endpoints  []string `yaml:"endpoints"`
	SkipVerify bool     `yaml:"skip_verify"`
	Timeout    string   `yaml:"timeout"`
	Domain     string   `yaml:"domain,omitempty"`
}

var (
	logger       *slog.Logger
	configPath   string
	generateFlag string
)

func init() {
	// Initialize logger
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(os.Stderr, logOpts)
	logger = slog.New(handler)

	flag.StringVar(&configPath, "config", "", "Path to the ferry configuration file (defaults to ferry.yaml, then FERRY_CONFIG env)")
	flag.StringVar(&generateFlag, "generate", "", "Generate config from comma-separated endpoints")
}

func main() {
	flag.Parse()

	// Handle config generation
	if generateFlag != "" {
		generateConfig(generateFlag)
		return
	}

	// Load config
	cfg, err := loadConfig()
	if err != nil {
		logger.Error("Failed to load configuration", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	// Convert to ferry config
	ferryConfig := &ferry.Config{
		ApiKey:     os.Getenv(cfg.ApiKeyEnv),
		Endpoints:  cfg.Endpoints,
		SkipVerify: cfg.SkipVerify,
		Domain:     cfg.Domain,
	}

	// Parse timeout
	if cfg.Timeout != "" {
		timeout, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			logger.Error("Invalid timeout format", "timeout", cfg.Timeout, "error", err)
			fmt.Fprintf(os.Stderr, "%s Invalid timeout format: %v\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		ferryConfig.Timeout = timeout
	} else {
		ferryConfig.Timeout = 30 * time.Second
	}

	if ferryConfig.ApiKey == "" {
		logger.Error("API key not found", "env_var", cfg.ApiKeyEnv)
		fmt.Fprintf(os.Stderr, "%s API key environment variable %s is not set\n", color.RedString("Error:"), cfg.ApiKeyEnv)
		os.Exit(1)
	}

	// Create ferry instance
	f, err := ferry.New(logger, ferryConfig)
	if err != nil {
		logger.Error("Failed to create ferry instance", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	// Parse commands
	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]
	cmdArgs := args[1:]

	switch command {
	case "values":
		handleValues(f, cmdArgs)
	case "cache":
		handleCache(f, cmdArgs)
	case "events":
		handleEvents(f, cmdArgs)
	case "ping":
		handlePing(f, cmdArgs)
	case "blob":
		handleBlob(f, cmdArgs)
	default:
		logger.Error("Unknown command", "command", command)
		printUsage()
		os.Exit(1)
	}
}

func generateConfig(endpoints string) {
	// Split endpoints by comma
	endpointList := strings.Split(endpoints, ",")
	for i, ep := range endpointList {
		endpointList[i] = strings.TrimSpace(ep)
	}

	// Create config
	cfg := FerryConfig{
		ApiKeyEnv:  "INSI_API_KEY",
		Endpoints:  endpointList,
		SkipVerify: false,
		Timeout:    "30s",
	}

	// Marshal to YAML
	data, err := yaml.Marshal(&cfg)
	if err != nil {
		logger.Error("Failed to marshal config", "error", err)
		fmt.Fprintf(os.Stderr, "%s Failed to marshal config: %v\n", color.RedString("Error:"), err)
		os.Exit(1)
	}

	// Print to stdout
	fmt.Print(string(data))
}

func loadConfig() (*FerryConfig, error) {
	// Determine config path
	if configPath == "" {
		// Check ferry.yaml
		if _, err := os.Stat("ferry.yaml"); err == nil {
			configPath = "ferry.yaml"
		} else {
			// Check FERRY_CONFIG env
			if envPath := os.Getenv("FERRY_CONFIG"); envPath != "" {
				configPath = envPath
			} else {
				return nil, fmt.Errorf("no config file found: checked ferry.yaml and FERRY_CONFIG env var")
			}
		}
	}

	// Read config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	// Parse YAML
	var cfg FerryConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	// Set defaults
	if cfg.ApiKeyEnv == "" {
		cfg.ApiKeyEnv = "INSI_API_KEY"
	}

	return &cfg, nil
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: ferry [flags] <command> [args...]\n")
	fmt.Fprintf(os.Stderr, "\nFlags:\n")
	flag.PrintDefaults()

	fmt.Fprintf(os.Stderr, "\n%s\n", color.CyanString("Configuration:"))
	fmt.Fprintf(os.Stderr, "  Ferry looks for configuration in this order:\n")
	fmt.Fprintf(os.Stderr, "  1. --config flag (if specified)\n")
	fmt.Fprintf(os.Stderr, "  2. ferry.yaml in current directory\n")
	fmt.Fprintf(os.Stderr, "  3. FERRY_CONFIG environment variable\n")
	fmt.Fprintf(os.Stderr, "  API key is read from the environment variable specified in config (default: INSI_API_KEY)\n")

	fmt.Fprintf(os.Stderr, "\n%s\n", color.CyanString("Commands:"))

	// Config generation
	fmt.Fprintf(os.Stderr, "\n%s\n", color.YellowString("Configuration:"))
	fmt.Fprintf(os.Stderr, "  %s --generate %s\n", color.GreenString("ferry"), color.CyanString("\"endpoint1,endpoint2,...\""))
	fmt.Fprintf(os.Stderr, "    Generate a ferry.yaml configuration file from comma-separated endpoints\n")
	fmt.Fprintf(os.Stderr, "    Example: ferry --generate \"red.insulalabs.io:443,blue.insulalabs.io:443\" > ferry.yaml\n")

	// Ping
	fmt.Fprintf(os.Stderr, "\n%s\n", color.YellowString("Connectivity:"))
	fmt.Fprintf(os.Stderr, "  %s\n", color.GreenString("ping"))
	fmt.Fprintf(os.Stderr, "    Test connectivity to the configured endpoints\n")

	// Values operations
	fmt.Fprintf(os.Stderr, "\n%s\n", color.YellowString("Value Store Operations:"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("values"), color.CyanString("get"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "    Retrieve a value by key\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("values"), color.CyanString("set"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "    Store a value with the given key\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("values"), color.CyanString("setnx"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "    Set value only if key doesn't exist (set-if-not-exists)\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("values"), color.CyanString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "    Delete a key-value pair\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("values"), color.CyanString("cas"), color.CyanString("<key>"), color.CyanString("<old_value>"), color.CyanString("<new_value>"))
	fmt.Fprintf(os.Stderr, "    Compare-and-swap: update value only if current value matches old_value\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("values"), color.CyanString("bump"), color.CyanString("<key>"), color.CyanString("<increment>"))
	fmt.Fprintf(os.Stderr, "    Atomically increment a numeric value by the given amount\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("values"), color.CyanString("iterate"), color.CyanString("<prefix>"), color.CyanString("[offset]"), color.CyanString("[limit]"))
	fmt.Fprintf(os.Stderr, "    List keys matching the given prefix (default: offset=0, limit=100)\n")

	// Cache operations
	fmt.Fprintf(os.Stderr, "\n%s\n", color.YellowString("Cache Operations (volatile storage):"))
	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("cache"), color.CyanString("get"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "    Retrieve a cached value by key\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("cache"), color.CyanString("set"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "    Store a value in cache with the given key\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("cache"), color.CyanString("setnx"), color.CyanString("<key>"), color.CyanString("<value>"))
	fmt.Fprintf(os.Stderr, "    Set value only if key doesn't exist (set-if-not-exists)\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("cache"), color.CyanString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "    Delete a cached key-value pair\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("cache"), color.CyanString("cas"), color.CyanString("<key>"), color.CyanString("<old_value>"), color.CyanString("<new_value>"))
	fmt.Fprintf(os.Stderr, "    Compare-and-swap: update cached value only if current value matches old_value\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("cache"), color.CyanString("iterate"), color.CyanString("<prefix>"), color.CyanString("[offset]"), color.CyanString("[limit]"))
	fmt.Fprintf(os.Stderr, "    List cached keys matching the given prefix (default: offset=0, limit=100)\n")

	// Events operations
	fmt.Fprintf(os.Stderr, "\n%s\n", color.YellowString("Event Operations:"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("events"), color.CyanString("publish"), color.CyanString("<topic>"), color.CyanString("<data>"))
	fmt.Fprintf(os.Stderr, "    Publish data to a topic (data can be string or JSON)\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("events"), color.CyanString("subscribe"), color.CyanString("<topic>"))
	fmt.Fprintf(os.Stderr, "    Subscribe to a topic and print received events (blocks until Ctrl+C)\n")

	fmt.Fprintf(os.Stderr, "  %s %s\n", color.GreenString("events"), color.CyanString("purge"))
	fmt.Fprintf(os.Stderr, "    Disconnect all event subscriptions for the current API key across all nodes in the cluster\n")

	// Blob operations
	fmt.Fprintf(os.Stderr, "\n%s\n", color.YellowString("Blob Storage Operations:"))
	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("blob"), color.CyanString("upload"), color.CyanString("<key>"), color.CyanString("<file>"))
	fmt.Fprintf(os.Stderr, "    Upload a file as a blob with the given key\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s\n", color.GreenString("blob"), color.CyanString("download"), color.CyanString("<key>"), color.CyanString("[output_file]"))
	fmt.Fprintf(os.Stderr, "    Download a blob by key (outputs to stdout if no output file specified)\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s\n", color.GreenString("blob"), color.CyanString("delete"), color.CyanString("<key>"))
	fmt.Fprintf(os.Stderr, "    Delete a blob by key\n")

	fmt.Fprintf(os.Stderr, "  %s %s %s %s %s\n", color.GreenString("blob"), color.CyanString("iterate"), color.CyanString("<prefix>"), color.CyanString("[offset]"), color.CyanString("[limit]"))
	fmt.Fprintf(os.Stderr, "    List blob keys matching the given prefix (default: offset=0, limit=100)\n")

	// Examples
	fmt.Fprintf(os.Stderr, "\n%s\n", color.CyanString("Examples:"))
	fmt.Fprintf(os.Stderr, "  # Generate configuration\n")
	fmt.Fprintf(os.Stderr, "  ferry --generate \"red.insulalabs.io:443,blue.insulalabs.io:443,green.insulalabs.io:443\" > ferry.yaml\n")
	fmt.Fprintf(os.Stderr, "  \n")
	fmt.Fprintf(os.Stderr, "  # Test connectivity\n")
	fmt.Fprintf(os.Stderr, "  ferry ping\n")
	fmt.Fprintf(os.Stderr, "  \n")
	fmt.Fprintf(os.Stderr, "  # Value store operations\n")
	fmt.Fprintf(os.Stderr, "  ferry values set user:123 '{\"name\":\"Alice\",\"age\":30}'\n")
	fmt.Fprintf(os.Stderr, "  ferry values get user:123\n")
	fmt.Fprintf(os.Stderr, "  ferry values setnx lock:process \"locked\"\n")
	fmt.Fprintf(os.Stderr, "  ferry values bump counter 1\n")
	fmt.Fprintf(os.Stderr, "  ferry values iterate user: 0 50\n")
	fmt.Fprintf(os.Stderr, "  \n")
	fmt.Fprintf(os.Stderr, "  # Cache operations\n")
	fmt.Fprintf(os.Stderr, "  ferry cache set session:abc \"active\"\n")
	fmt.Fprintf(os.Stderr, "  ferry cache setnx lock:resource \"locked\"\n")
	fmt.Fprintf(os.Stderr, "  \n")
	fmt.Fprintf(os.Stderr, "  # Event pub/sub\n")
	fmt.Fprintf(os.Stderr, "  ferry events publish notifications '{\"type\":\"alert\",\"message\":\"Hello\"}'\n")
	fmt.Fprintf(os.Stderr, "  ferry events subscribe notifications\n")
	fmt.Fprintf(os.Stderr, "  \n")
	fmt.Fprintf(os.Stderr, "  # Blob storage\n")
	fmt.Fprintf(os.Stderr, "  ferry blob upload document:123 report.pdf\n")
	fmt.Fprintf(os.Stderr, "  ferry blob download document:123 downloaded-report.pdf\n")
	fmt.Fprintf(os.Stderr, "  ferry blob iterate document: 0 50\n")
	fmt.Fprintf(os.Stderr, "  \n")
	fmt.Fprintf(os.Stderr, "  # Using custom config\n")
	fmt.Fprintf(os.Stderr, "  ferry --config prod-ferry.yaml values get mykey\n")

	fmt.Fprintf(os.Stderr, "\n%s\n", color.CyanString("Notes:"))
	fmt.Fprintf(os.Stderr, "  - Value store provides persistent storage with strong consistency\n")
	fmt.Fprintf(os.Stderr, "  - Cache provides fast volatile storage (data may be evicted)\n")
	fmt.Fprintf(os.Stderr, "  - Blob storage provides persistent storage for large binary objects\n")
	fmt.Fprintf(os.Stderr, "  - Events enable real-time pub/sub messaging between clients\n")
	fmt.Fprintf(os.Stderr, "  - All operations use the ferry package which provides automatic retries and error handling\n")
}

func handlePing(f *ferry.Ferry, args []string) {
	if len(args) != 0 {
		logger.Error("ping: does not take arguments")
		printUsage()
		os.Exit(1)
	}

	err := f.Ping(3, 1*time.Second)
	if err != nil {
		logger.Error("Ping failed", "error", err)
		fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
	color.HiGreen("OK - Ping successful")
}

func handleValues(f *ferry.Ferry, args []string) {
	if len(args) < 1 {
		logger.Error("values: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}

	ctx := context.Background()
	vc := ferry.GetValueController[string](f, "")

	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "get":
		if len(subArgs) != 1 {
			logger.Error("values get: requires <key>")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]
		value, err := vc.Get(ctx, key)
		if err != nil {
			if err == ferry.ErrKeyNotFound {
				fmt.Fprintf(os.Stderr, "%s Key '%s' not found.\n", color.RedString("Error:"), color.CyanString(key))
			} else {
				logger.Error("Get failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
		fmt.Println(value)

	case "set":
		if len(subArgs) != 2 {
			logger.Error("values set: requires <key> <value>")
			printUsage()
			os.Exit(1)
		}
		key, value := subArgs[0], subArgs[1]
		err := vc.Set(ctx, key, value)
		if err != nil {
			logger.Error("Set failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		color.HiGreen("OK")

	case "setnx":
		if len(subArgs) != 2 {
			logger.Error("values setnx: requires <key> <value>")
			printUsage()
			os.Exit(1)
		}
		key, value := subArgs[0], subArgs[1]
		err := vc.SetNX(ctx, key, value)
		if err != nil {
			if err == ferry.ErrConflict {
				fmt.Fprintf(os.Stderr, "%s Key '%s' already exists.\n", color.RedString("Conflict:"), color.CyanString(key))
			} else {
				logger.Error("SetNX failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
		color.HiGreen("OK")

	case "delete":
		if len(subArgs) != 1 {
			logger.Error("values delete: requires <key>")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]
		err := vc.Delete(ctx, key)
		if err != nil {
			logger.Error("Delete failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		color.HiGreen("OK")

	case "cas":
		if len(subArgs) != 3 {
			logger.Error("values cas: requires <key> <old_value> <new_value>")
			printUsage()
			os.Exit(1)
		}
		key, oldValue, newValue := subArgs[0], subArgs[1], subArgs[2]
		err := vc.CompareAndSwap(ctx, key, oldValue, newValue)
		if err != nil {
			if err == ferry.ErrConflict {
				fmt.Fprintf(os.Stderr, "%s Compare-and-swap failed for key '%s'.\n", color.RedString("Conflict:"), color.CyanString(key))
			} else {
				logger.Error("CAS failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
		color.HiGreen("OK")

	case "bump":
		if len(subArgs) != 2 {
			logger.Error("values bump: requires <key> <value>")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]
		value, err := strconv.Atoi(subArgs[1])
		if err != nil {
			logger.Error("bump: value must be an integer", "error", err)
			fmt.Fprintf(os.Stderr, "%s Value must be an integer: %v\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		err = vc.Bump(ctx, key, value)
		if err != nil {
			logger.Error("Bump failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		color.HiGreen("OK")

	case "iterate":
		handleValuesIterate(ctx, vc, subArgs)

	default:
		logger.Error("values: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleValuesIterate(ctx context.Context, vc ferry.ValueController[string], args []string) {
	if len(args) < 1 {
		logger.Error("values iterate: requires <prefix> [offset] [limit]")
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

	results, err := vc.IterateByPrefix(ctx, prefix, offset, limit)
	if err != nil {
		if err == ferry.ErrKeyNotFound {
			color.HiRed("No keys found.")
		} else {
			logger.Error("Iterate failed", "prefix", prefix, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	for _, item := range results {
		fmt.Println(item)
	}
}

func handleCache(f *ferry.Ferry, args []string) {
	if len(args) < 1 {
		logger.Error("cache: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}

	ctx := context.Background()
	cc := ferry.GetCacheController[string](f, "")

	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "get":
		if len(subArgs) != 1 {
			logger.Error("cache get: requires <key>")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]
		value, err := cc.Get(ctx, key)
		if err != nil {
			if err == ferry.ErrKeyNotFound {
				fmt.Fprintf(os.Stderr, "%s Key '%s' not found in cache.\n", color.RedString("Error:"), color.CyanString(key))
			} else {
				logger.Error("Cache get failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
		fmt.Println(value)

	case "set":
		if len(subArgs) != 2 {
			logger.Error("cache set: requires <key> <value>")
			printUsage()
			os.Exit(1)
		}
		key, value := subArgs[0], subArgs[1]
		err := cc.Set(ctx, key, value)
		if err != nil {
			logger.Error("Cache set failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
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
		err := cc.SetNX(ctx, key, value)
		if err != nil {
			if err == ferry.ErrConflict {
				fmt.Fprintf(os.Stderr, "%s Key '%s' already exists in cache.\n", color.RedString("Conflict:"), color.CyanString(key))
			} else {
				logger.Error("Cache SetNX failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
		color.HiGreen("OK")

	case "delete":
		if len(subArgs) != 1 {
			logger.Error("cache delete: requires <key>")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]
		err := cc.Delete(ctx, key)
		if err != nil {
			if err == ferry.ErrKeyNotFound {
				fmt.Fprintf(os.Stderr, "%s Key '%s' not found in cache. Nothing to delete.\n", color.YellowString("Warning:"), color.CyanString(key))
			} else {
				logger.Error("Cache delete failed", "key", key, "error", err)
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
		err := cc.CompareAndSwap(ctx, key, oldValue, newValue)
		if err != nil {
			if err == ferry.ErrConflict {
				fmt.Fprintf(os.Stderr, "%s Cache compare-and-swap failed for key '%s'.\n", color.RedString("Conflict:"), color.CyanString(key))
			} else {
				logger.Error("Cache CAS failed", "key", key, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			}
			os.Exit(1)
		}
		color.HiGreen("OK")

	case "iterate":
		handleCacheIterate(ctx, cc, subArgs)

	default:
		logger.Error("cache: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleCacheIterate(ctx context.Context, cc ferry.CacheController[string], args []string) {
	if len(args) < 1 {
		logger.Error("cache iterate: requires <prefix> [offset] [limit]")
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

	results, err := cc.IterateByPrefix(ctx, prefix, offset, limit)
	if err != nil {
		if err == ferry.ErrKeyNotFound {
			color.HiRed("No keys found in cache.")
		} else {
			logger.Error("Cache iterate failed", "prefix", prefix, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	for _, item := range results {
		fmt.Println(item)
	}
}

func handleEvents(f *ferry.Ferry, args []string) {
	if len(args) < 1 {
		logger.Error("events: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}

	ctx := context.Background()
	events := ferry.GetEvents(f)

	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "publish":
		if len(subArgs) != 2 {
			logger.Error("events publish: requires <topic> <data>")
			printUsage()
			os.Exit(1)
		}
		topic := subArgs[0]
		dataStr := subArgs[1]

		// Try to parse as JSON, otherwise use as string
		var dataToPublish any
		var jsonData any
		if err := json.Unmarshal([]byte(dataStr), &jsonData); err == nil {
			dataToPublish = jsonData
		} else {
			dataToPublish = dataStr
		}

		publisher := events.GetPublisher(topic)
		err := publisher.Publish(ctx, dataToPublish)
		if err != nil {
			logger.Error("Publish failed", "topic", topic, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		color.HiGreen("OK")

	case "subscribe":
		if len(subArgs) != 1 {
			logger.Error("events subscribe: requires <topic>")
			printUsage()
			os.Exit(1)
		}
		topic := subArgs[0]

		// Setup signal handling
		sigCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		go func() {
			sig := <-sigChan
			logger.Info("Received signal, stopping subscription...", "signal", sig.String())
			cancel()
		}()

		// Create handler
		handler := func(event models.EventPayload) {
			fmt.Printf("[%s] Received event on topic '%s': %+v\n",
				time.Now().Format("15:04:05"),
				color.CyanString(topic),
				event.Data)
		}

		subscriber := events.GetSubscriber(topic)
		logger.Info("Subscribing to events", "topic", color.CyanString(topic))
		err := subscriber.Subscribe(sigCtx, handler)
		if err != nil {
			if err == context.Canceled {
				logger.Info("Subscription cancelled gracefully.", "topic", color.CyanString(topic))
			} else if err == ferry.ErrSubscriberLimitExceeded {
				fmt.Fprintf(os.Stderr, "%s Subscriber limit exceeded\n", color.RedString("Error:"))
				os.Exit(1)
			} else {
				logger.Error("Subscription failed", "topic", topic, "error", err)
				fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
				os.Exit(1)
			}
		}
		logger.Info("Subscription ended.", "topic", color.CyanString(topic))

	case "purge":
		if len(subArgs) != 0 {
			logger.Error("events purge: does not take arguments")
			printUsage()
			os.Exit(1)
		}

		logger.Info("Purging all event subscriptions for current API key across all nodes")
		disconnectedCount, err := events.PurgeAllNodes(ctx)
		if err != nil {
			logger.Error("Purge failed", "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			os.Exit(1)
		}

		if disconnectedCount == 0 {
			color.HiYellow("No active event subscriptions found to purge.")
		} else {
			color.HiGreen("Successfully purged %d event subscription(s) across all nodes.", disconnectedCount)
		}

	default:
		logger.Error("events: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleBlob(f *ferry.Ferry, args []string) {
	if len(args) < 1 {
		logger.Error("blob: requires <sub-command> [args...]")
		printUsage()
		os.Exit(1)
	}

	ctx := context.Background()
	bc := ferry.GetBlobController(f)

	subCommand := args[0]
	subArgs := args[1:]

	switch subCommand {
	case "upload":
		if len(subArgs) != 2 {
			logger.Error("blob upload: requires <key> <file>")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]
		filePath := subArgs[1]

		// Open the file
		file, err := os.Open(filePath)
		if err != nil {
			logger.Error("Failed to open file", "file", filePath, "error", err)
			fmt.Fprintf(os.Stderr, "%s Failed to open file: %v\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		defer file.Close()

		// Get file info for the filename
		fileInfo, err := file.Stat()
		if err != nil {
			logger.Error("Failed to stat file", "file", filePath, "error", err)
			fmt.Fprintf(os.Stderr, "%s Failed to stat file: %v\n", color.RedString("Error:"), err)
			os.Exit(1)
		}

		err = bc.Upload(ctx, key, file, fileInfo.Name())
		if err != nil {
			logger.Error("Upload failed", "key", key, "file", filePath, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		color.HiGreen("OK - Uploaded %s as blob key '%s'", filePath, key)

	case "download":
		if len(subArgs) < 1 || len(subArgs) > 2 {
			logger.Error("blob download: requires <key> [output_file]")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]

		if len(subArgs) == 2 {
			outputPath := subArgs[1]

			reader, err := bc.Download(ctx, key)
			if err != nil {
				if err == ferry.ErrKeyNotFound {
					fmt.Fprintf(os.Stderr, "%s Blob key '%s' not found.\n", color.RedString("Error:"), color.CyanString(key))
				} else {
					logger.Error("Download failed", "key", key, "error", err)
					fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
				}
				os.Exit(1)
			}
			defer reader.Close()

			file, err := os.Create(outputPath)
			if err != nil {
				logger.Error("Failed to create output file", "file", outputPath, "error", err)
				fmt.Fprintf(os.Stderr, "%s Failed to create output file: %v\n", color.RedString("Error:"), err)
				os.Exit(1)
			}
			defer file.Close()

			_, err = io.Copy(file, reader)
			if err != nil {
				logger.Error("Failed to write output file", "file", outputPath, "error", err)
				fmt.Fprintf(os.Stderr, "%s Failed to write output file: %v\n", color.RedString("Error:"), err)
				os.Exit(1)
			}
			color.HiGreen("OK - Downloaded blob key '%s' to %s", key, outputPath)
		} else {
			reader, err := bc.Download(ctx, key)
			if err != nil {
				if err == ferry.ErrKeyNotFound {
					fmt.Fprintf(os.Stderr, "%s Blob key '%s' not found.\n", color.RedString("Error:"), color.CyanString(key))
				} else {
					logger.Error("Download failed", "key", key, "error", err)
					fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
				}
				os.Exit(1)
			}
			defer reader.Close()

			_, err = io.Copy(os.Stdout, reader)
			if err != nil {
				logger.Error("Failed to write to stdout", "error", err)
				fmt.Fprintf(os.Stderr, "%s Failed to write to stdout: %v\n", color.RedString("Error:"), err)
				os.Exit(1)
			}
		}

	case "delete":
		if len(subArgs) != 1 {
			logger.Error("blob delete: requires <key>")
			printUsage()
			os.Exit(1)
		}
		key := subArgs[0]
		err := bc.Delete(ctx, key)
		if err != nil {
			logger.Error("Delete failed", "key", key, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
		color.HiGreen("OK - Deleted blob key '%s'", key)

	case "iterate":
		handleBlobIterate(ctx, bc, subArgs)

	default:
		logger.Error("blob: unknown sub-command", "sub_command", subCommand)
		printUsage()
		os.Exit(1)
	}
}

func handleBlobIterate(ctx context.Context, bc ferry.BlobController, args []string) {
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

	results, err := bc.IterateByPrefix(ctx, prefix, offset, limit)
	if err != nil {
		if err == ferry.ErrKeyNotFound {
			color.HiRed("No blob keys found.")
		} else {
			logger.Error("Iterate failed", "prefix", prefix, "error", err)
			fmt.Fprintf(os.Stderr, "%s %s\n", color.RedString("Error:"), err)
		}
		os.Exit(1)
	}
	for _, item := range results {
		fmt.Println(item)
	}
}
