package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/ovm"
	"github.com/InsulaLabs/insi/runtime"
	"github.com/InsulaLabs/insi/service/objects"
	"github.com/InsulaLabs/insi/service/status"
)

var (
	logger *slog.Logger
)

func init() {
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(os.Stderr, logOpts)
	logger = slog.New(handler)
}

func printGlobalUsage() {
	fmt.Fprintf(os.Stderr, "Usage: insio <command> [arguments]\n")
	fmt.Fprintf(os.Stderr, "A unified tool for starting an Insula server and interacting with it.\n\n")
	fmt.Fprintf(os.Stderr, "Available commands:\n")
	fmt.Fprintf(os.Stderr, "  server    Start an Insula server node (maintains insid functionality).\n")
	fmt.Fprintf(os.Stderr, "  ping      Ping the target node to check connectivity.\n")
	fmt.Fprintf(os.Stderr, "  verify    Verify an API key has connectivity to the target node.\n")
	fmt.Fprintf(os.Stderr, "  run       Execute a Javascript file using the OVM.\n")
	fmt.Fprintf(os.Stderr, "\nUse \"insio <command> -h\" for more information about a specific command.\n")
}

func startServer(args []string) {
	log.SetOutput(io.Discard)

	rt, err := runtime.New(args, "cluster.yaml")
	if err != nil {
		logger.Error("Failed to initialize runtime", "error", err)
		os.Exit(1)
	}

	rt.WithService(status.New(logger.WithGroup("status-service")))
	objectsDir := filepath.Join(rt.GetHomeDir(), "services", "objects")
	if err := os.MkdirAll(objectsDir, 0755); err != nil {
		logger.Error("Failed to create objects directory", "error", err)
		os.Exit(1)
	}
	rt.WithService(objects.New(logger.WithGroup("objects-service"), objectsDir))

	if err := rt.Run(); err != nil {
		logger.Error("Runtime exited with error", "error", err)
		os.Exit(1)
	}

	rt.Wait()
	logger.Info("Server has shutdown.")
}

func getClient(configPath, targetNode string, useRootKey bool) (*client.Client, *config.Cluster, error) {
	clusterCfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load cluster configuration: %w", err)
	}

	nodeToConnect := targetNode
	if nodeToConnect == "" {
		nodeToConnect = clusterCfg.DefaultLeader
		logger.Debug("No target node specified, using DefaultLeader", "node_id", nodeToConnect)
	}

	nodeDetails, ok := clusterCfg.Nodes[nodeToConnect]
	if !ok {
		return nil, nil, fmt.Errorf("node ID '%s' not found in configuration", nodeToConnect)
	}

	var apiKey string
	if useRootKey {
		if clusterCfg.InstanceSecret == "" {
			return nil, nil, fmt.Errorf("InstanceSecret not defined, cannot generate root API key")
		}
		secretHash := sha256.New()
		secretHash.Write([]byte(clusterCfg.InstanceSecret))
		apiKey = hex.EncodeToString(secretHash.Sum(nil))
		apiKey = base64.StdEncoding.EncodeToString([]byte(apiKey))
	} else {
		apiKey = os.Getenv("INSI_API_KEY")
	}

	if apiKey == "" {
		return nil, nil, fmt.Errorf("API key is empty and --root flag is not set (or INSI_API_KEY env var)")
	}

	c, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeDirect,
		Endpoints: []client.Endpoint{
			{HostPort: nodeDetails.HttpBinding, ClientDomain: nodeDetails.ClientDomain},
		},
		ApiKey:     apiKey,
		SkipVerify: clusterCfg.ClientSkipVerify,
		Logger:     logger.WithGroup("client"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create client: %w", err)
	}
	return c, clusterCfg, nil
}

func main() {
	if len(os.Args) < 2 {
		printGlobalUsage()
		os.Exit(1)
	}

	// Client-related flags
	clientFlags := flag.NewFlagSet("client", flag.ExitOnError)
	configPath := clientFlags.String("config", "cluster.yaml", "Path to cluster configuration.")
	targetNode := clientFlags.String("target", "", "Target node ID. Defaults to DefaultLeader.")
	useRootKey := clientFlags.Bool("root", false, "Use the root key for authentication.")

	switch os.Args[1] {
	case "server":
		// The server command passes all subsequent arguments directly to the runtime
		startServer(os.Args[2:])
	case "run":
		runCmd := flag.NewFlagSet("run", flag.ExitOnError)
		runCmd.Usage = func() {
			fmt.Fprintf(os.Stderr, "Usage: insio run <script_path> [flags]\n")
			fmt.Fprintf(os.Stderr, "Executes a script against a target node.\n\n")
			fmt.Fprintf(os.Stderr, "Flags:\n")
			runCmd.PrintDefaults()
			clientFlags.PrintDefaults() // Show shared client flags
		}

		// Attach client flags to run command
		runCmd.StringVar(configPath, "config", "cluster.yaml", "Path to cluster configuration.")
		runCmd.StringVar(targetNode, "target", "", "Target node ID. Defaults to DefaultLeader.")
		runCmd.BoolVar(useRootKey, "root", false, "Use the root key for authentication.")
		runCmd.Parse(os.Args[2:])

		if runCmd.NArg() != 1 {
			runCmd.Usage()
			os.Exit(1)
		}
		scriptPath := runCmd.Arg(0)

		cli, _, err := getClient(*configPath, *targetNode, *useRootKey)
		if err != nil {
			logger.Error("Failed to initialize API client", "error", err)
			os.Exit(1)
		}

		scriptContent, err := os.ReadFile(scriptPath)
		if err != nil {
			logger.Error("Failed to read script file", "path", scriptPath, "error", err)
			os.Exit(1)
		}

		vm, err := ovm.New(&ovm.Config{
			Logger:       logger.WithGroup("ovm"),
			SetupCtx:     context.Background(),
			InsiClient:   cli,
			DoAddAdmin:   true,
			DoAddConsole: true,
			DoAddOS:      true,
			DoAddTest:    true,
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

	case "ping", "verify":
		cmd := os.Args[1]
		cmdFlags := flag.NewFlagSet(cmd, flag.ExitOnError)
		cmdFlags.Usage = func() {
			fmt.Fprintf(os.Stderr, "Usage: insio %s [flags]\n\n", cmd)
			cmdFlags.PrintDefaults()
		}
		// Re-declare flags for this command
		configPath := cmdFlags.String("config", "cluster.yaml", "Path to cluster configuration.")
		targetNode := cmdFlags.String("target", "", "Target node ID. Defaults to DefaultLeader.")
		useRootKey := cmdFlags.Bool("root", false, "Use the root key for authentication.")
		cmdFlags.Parse(os.Args[2:])

		cli, _, err := getClient(*configPath, *targetNode, *useRootKey)
		if err != nil {
			logger.Error("Failed to initialize API client", "error", err)
			os.Exit(1)
		}
		logger.Info(fmt.Sprintf("Attempting to %s target node...", cmd))
		resp, err := cli.Ping()
		if err != nil {
			logger.Error(fmt.Sprintf("%s failed", cmd), "error", err)
			os.Exit(1)
		}
		logger.Info(fmt.Sprintf("%s Successful!", cmd))
		fmt.Println("Ping Response:")
		for k, v := range resp {
			fmt.Printf("  %s: %s\n", k, v)
		}

	default:
		fmt.Fprintf(os.Stderr, "Error: Unknown command \"%s\"\n", os.Args[1])
		printGlobalUsage()
		os.Exit(1)
	}
}
