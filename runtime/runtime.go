package runtime

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/internal/service"
	"github.com/InsulaLabs/insi/internal/tkv"
	"github.com/InsulaLabs/insula/security/badge"
	"gopkg.in/yaml.v3"
)

// Runtime manages the execution of insid, handling configuration,
// signal processing, and the lifecycle of node instances.
type Runtime struct {
	appCtx     context.Context
	appCancel  context.CancelFunc
	logger     *slog.Logger
	clusterCfg *config.Cluster
	configFile string
	asNodeId   string
	hostMode   bool
	rawArgs    []string // To allow flag parsing within New
}

// New creates a new Runtime instance.
// It initializes the application context, sets up signal handling,
// parses command-line flags, and loads the cluster configuration.
func New(args []string, defaultConfigFile string) (*Runtime, error) {
	r := &Runtime{
		rawArgs: args,
	}

	r.appCtx, r.appCancel = context.WithCancel(context.Background())
	r.logger = slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", "insidRuntime")

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		r.logger.Info("Received signal, initiating shutdown...", "signal", sig)
		r.appCancel()
	}()

	var genConfigFile string
	// Parse flags
	fs := flag.NewFlagSet("runtime", flag.ContinueOnError)
	fs.StringVar(&r.configFile, "config", defaultConfigFile, "Path to the cluster configuration file.")
	fs.StringVar(&r.asNodeId, "as", "", "Node ID to run as (e.g., node0). Mutually exclusive with --host.")
	fs.BoolVar(&r.hostMode, "host", false, "Run instances for all nodes in the config. Mutually exclusive with --as.")
	fs.StringVar(&genConfigFile, "new-cfg", "", "Generate a new cluster configuration file to a given path.")

	if err := fs.Parse(r.rawArgs); err != nil {
		return nil, fmt.Errorf("failed to parse flags: %w", err)
	}

	if genConfigFile != "" {
		cfg, err := config.GenerateConfig(genConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to generate configuration: %w", err)
		}

		yamlData, err := yaml.Marshal(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal generated config to YAML: %w", err)
		}

		// Ensure the directory exists
		dir := filepath.Dir(genConfigFile)
		if dir != "." && dir != "" { // Avoid creating '.' or empty dir
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory for config file %s: %w", genConfigFile, err)
			}
		}

		if err := os.WriteFile(genConfigFile, yamlData, 0644); err != nil {
			return nil, fmt.Errorf("failed to write generated configuration to %s: %w", genConfigFile, err)
		}

		r.logger.Info("Successfully generated new configuration file", "path", genConfigFile)
		// After generating the config, the program's purpose is fulfilled.
		// We can indicate that no further runtime operations should proceed by returning a specific error
		// or by setting a flag that Run() can check. For now, returning a distinct error or nil if successful generation means exit.
		// A simple way is to ensure `Run()` is not called, or the caller of `New()` handles this.
		// For instance, the main function could check if genConfigFile was set and not proceed to r.Run() if so.
		// Let's make New return a special value or the main func checks. For now, we'll just log and the caller should handle it.
		// A common pattern is for New to return (nil, nil) if the operation (like config gen) completes successfully and implies exit.
		// Or, as done here, allow the Runtime object to be returned but the caller of New needs to check if genConfigFile was processed.
		// To make it explicit that the runtime should not continue, we can os.Exit here, or return a sentinel error.
		// For simplicity and to avoid os.Exit in library code, we'll assume the caller checks.
		// The linter error for `cfg` being unused will be resolved because it's now used by `yaml.Marshal(cfg)`.

		os.Exit(0)
	}

	if (r.asNodeId == "" && !r.hostMode) || (r.asNodeId != "" && r.hostMode) {
		fs.Usage()
		return nil, fmt.Errorf("either --as <nodeId> or --host must be specified, but not both")
	}

	var err error
	r.clusterCfg, err = config.LoadConfig(r.configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration from %s: %w", r.configFile, err)
	}

	return r, nil
}

// Run executes the runtime based on the parsed flags,
// either running as a single node or as a host managing multiple nodes.
func (r *Runtime) Run() error {
	if r.clusterCfg == nil {
		// This situation can occur if New() was called with the --new-cfg flag,
		// which completes its task by generating the configuration file. In that path,
		// r.clusterCfg is not loaded. Calling Run() subsequently is likely an unintentional
		// continuation by the caller.
		r.logger.Info("Runtime.Run called when clusterCfg is not loaded (e.g., after --new-cfg). No nodes to run. Aborting Run operation.")
		return nil // Indicate that Run completed as a no-op due to this condition.
	}

	if r.hostMode {
		return r.runAsHost()
	}
	return r.runAsNode(r.asNodeId)
}

// runAsNode runs the runtime as a specific node in the cluster.
func (r *Runtime) runAsNode(nodeId string) error {
	nodeSpecificCfg, ok := r.clusterCfg.Nodes[nodeId]
	if !ok {
		r.logger.Error("Node ID not found in configuration file", "node", nodeId, "available_nodes", getMapKeys(r.clusterCfg.Nodes))
		return fmt.Errorf("node ID %s not found in configuration", nodeId)
	}

	r.logger.Info("Starting in single node mode", "node", nodeId)
	r.startNodeInstance(nodeId, nodeSpecificCfg)

	<-r.appCtx.Done()
	r.logger.Info("Node service shutting down or completed for single node mode.", "node", nodeId)
	return nil
}

// runAsHost runs the runtime as a host, managing all nodes in the cluster.
func (r *Runtime) runAsHost() error {
	if len(r.clusterCfg.Nodes) == 0 {
		r.logger.Error("No nodes defined in the configuration file for host mode.")
		return fmt.Errorf("no nodes defined for host mode")
	}
	r.logger.Info("Running in --host mode. Starting instances for all configured nodes.", "count", len(r.clusterCfg.Nodes))

	var wg sync.WaitGroup
	for nodeId, nodeCfg := range r.clusterCfg.Nodes {
		wg.Add(1)
		go func(id string, cfg config.Node) {
			defer wg.Done()
			r.startNodeInstance(id, cfg)
		}(nodeId, nodeCfg)
	}

	go func() {
		wg.Wait()
		r.logger.Info("All node instances have completed.")
		// If all nodes complete naturally, we might want to initiate a shutdown.
		// However, typical server behavior is to keep running until signaled.
		// For now, rely on external signal or cancellation of appCtx.
	}()

	<-r.appCtx.Done()
	r.logger.Info("Shutdown signal received or all services completed. Exiting host mode.")
	return nil
}

// startNodeInstance sets up and runs a single node instance.
func (r *Runtime) startNodeInstance(nodeId string, nodeCfg config.Node) {
	nodeLogger := r.logger.With("node", nodeId)
	nodeLogger.Info("Starting node instance")

	if err := os.MkdirAll(r.clusterCfg.InsudbDir, os.ModePerm); err != nil {
		nodeLogger.Error("Failed to create insudbDir", "path", r.clusterCfg.InsudbDir, "error", err)
		// In a real scenario, might return error or panic depending on recovery strategy
		return
	}

	nodeDataRootPath := filepath.Join(r.clusterCfg.InsudbDir, nodeId)
	if err := os.MkdirAll(nodeDataRootPath, os.ModePerm); err != nil {
		nodeLogger.Error("Could not create node data root directory", "path", nodeDataRootPath, "error", err)
		return
	}

	b, err := loadOrCreateBadge(nodeId, nodeDataRootPath, nodeCfg.NodeSecret)
	if err != nil {
		nodeLogger.Error("Failed to load or create badge", "error", err)
		return
	}

	nodeDir := filepath.Join(nodeDataRootPath, nodeId) // This was originally nodeDataRootPath + "/" + *asNodeId
	if err := os.MkdirAll(nodeDir, os.ModePerm); err != nil {
		nodeLogger.Error("Failed to create node specific data directory", "path", nodeDir, "error", err)
		return
	}

	kvm, err := tkv.New(tkv.Config{
		Identity:  b,
		Logger:    nodeLogger.WithGroup("tkv"),
		Directory: nodeDir,
		AppCtx:    r.appCtx, // Use the runtime's app context
		CacheTTL:  r.clusterCfg.Cache.StandardTTL,
	})
	if err != nil {
		nodeLogger.Error("Failed to create KV manager", "error", err)
		return
	}
	defer kvm.Close()

	srvc, err := service.NewService(
		r.appCtx, // Use the runtime's app context
		nodeLogger.WithGroup("service"),
		&nodeCfg,
		b,
		kvm,
		r.clusterCfg,
		nodeId,
	)
	if err != nil {
		nodeLogger.Error("Failed to create service", "error", err)
		return
	}

	// Run the service. This should block until the service is done or context is cancelled.
	// We need to handle the completion of this service within the broader context
	// of the runtime (e.g., if one node fails, does it affect others in host mode?).
	// For now, each node runs somewhat independently until the main appCtx is cancelled.
	srvc.Run() // This blocks. Consider running in a goroutine if startNodeInstance needs to be non-blocking itself.

	nodeLogger.Info("Node instance shut down gracefully.")
}

func getMapKeys(m map[string]config.Node) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func loadOrCreateBadge(nodeId string, installDir string, secret string) (badge.Badge, error) {
	fileName := filepath.Join(installDir, fmt.Sprintf("%s.identity.encrypted", nodeId))

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		badge, err := badge.BuildBadge(
			badge.WithID(nodeId),
			badge.WithCurveSelector(badge.BadgeCurveSelectorP256),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to build badge: %w", err)
		}

		encryptedBadge, err := badge.EncryptBadge([]byte(secret))
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt badge: %w", err)
		}

		if err := os.WriteFile(fileName, encryptedBadge, 0600); err != nil { // More restrictive permissions
			return nil, fmt.Errorf("failed to write encrypted badge: %w", err)
		}
		return badge, nil
	}

	rawBadge, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read badge file %s: %w", fileName, err)
	}

	badge, err := badge.FromEncryptedBadge([]byte(secret), rawBadge)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt badge from %s: %w", fileName, err)
	}
	return badge, nil
}

// Wait for the runtime to complete its operations.
// This is typically when the application context is canceled.
func (r *Runtime) Wait() {
	<-r.appCtx.Done()
	r.logger.Info("Runtime has been shut down.")
}

// Stop gracefully shuts down the runtime by canceling its context.
func (r *Runtime) Stop() {
	r.logger.Info("Runtime stop requested.")
	r.appCancel()
}
