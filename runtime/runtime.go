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

	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net"
	"time"

	"github.com/InsulaLabs/insi/badge"
	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/db/core"
	"github.com/InsulaLabs/insi/db/tkv"
	"github.com/fatih/color"
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
	service    *core.Core

	rootApiKey string // Root API key generated from instance secret

	rtClients map[string]*client.Client

	currentLogLevel slog.Level
}

// New creates a new Runtime instance.
// It initializes the application context, sets up signal handling,
// parses command-line flags, and loads the cluster configuration.
func New(args []string, defaultConfigFile string) (*Runtime, error) {

	r := &Runtime{
		rawArgs:   args,
		rtClients: make(map[string]*client.Client),
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

		dir := filepath.Dir(genConfigFile)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory for config file %s: %w", genConfigFile, err)
			}
		}

		if err := os.WriteFile(genConfigFile, yamlData, 0644); err != nil {
			return nil, fmt.Errorf("failed to write generated configuration to %s: %w", genConfigFile, err)
		}

		r.logger.Info("Successfully generated new configuration file", "path", genConfigFile)
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

	// Set the log level
	if r.clusterCfg.Logging.Level != "" {
		switch r.clusterCfg.Logging.Level {
		case "debug":
			r.currentLogLevel = slog.LevelDebug
		case "info":
			r.currentLogLevel = slog.LevelInfo
		case "warn":
			r.currentLogLevel = slog.LevelWarn
		case "error":
			r.currentLogLevel = slog.LevelError
		default:
			color.HiYellow("Unknown logging level: %s, defaulting to info", r.clusterCfg.Logging.Level)
			r.currentLogLevel = slog.LevelInfo
		}
	}

	r.logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: r.currentLogLevel,
	})).With("service", "insidRuntime")

	// Generate the root API key from the instance secret
	// This key is used by runtime's internal clients and passed to services.
	if r.clusterCfg.InstanceSecret == "" {
		return nil, fmt.Errorf("InstanceSecret is not defined in the cluster configuration, cannot generate root API key")
	}
	h := sha256.New()
	h.Write([]byte(r.clusterCfg.InstanceSecret))
	r.rootApiKey = hex.EncodeToString(h.Sum(nil))

	r.rootApiKey = base64.StdEncoding.EncodeToString([]byte(r.rootApiKey))

	/*
		Create a series of clients for the runtime to isolate operations
		to single-use clients.
	*/
	for _, useCase := range []string{
		"set", "get", "delete", "iterate",
		"setObject", "getObject", "deleteObject", "iterateObject", "getObjectList",
		"setCache", "getCache", "deleteCache",
		"publishEvent",
	} {
		rtClient, err := r.GetClientForToken(r.rootApiKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create insi client: %w", err)
		}
		r.rtClients[useCase] = rtClient
	}

	return r, nil
}

func (r *Runtime) GetClientForToken(token string) (*client.Client, error) {
	getEndpoints := func() []client.Endpoint {
		eps := make([]client.Endpoint, 0, len(r.clusterCfg.Nodes))
		for nodeId, node := range r.clusterCfg.Nodes {
			ep := client.Endpoint{
				HostPort:     node.HttpBinding,
				ClientDomain: node.ClientDomain,
				Logger:       r.logger.With("service", "insiClient").With("node", nodeId),
			}
			eps = append(eps, ep)
		}
		return eps
	}
	rtClient, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeRandom,
		Logger:         r.logger.With("service", "insiClient"),
		ApiKey:         token,
		Endpoints:      getEndpoints(),
		SkipVerify:     r.clusterCfg.ClientSkipVerify,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create insi client: %w", err)
	}
	return rtClient, nil
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
	}()

	<-r.appCtx.Done()
	r.logger.Info("Shutdown signal received or all services completed. Exiting host mode.")
	return nil
}

// startNodeInstance sets up and runs a single node instance.
func (r *Runtime) startNodeInstance(nodeId string, nodeCfg config.Node) {
	nodeLogger := r.logger.With("node", nodeId)
	nodeLogger.Info("Starting node instance")

	if err := os.MkdirAll(r.clusterCfg.InsidHome, os.ModePerm); err != nil {
		nodeLogger.Error("Failed to create insidHome", "path", r.clusterCfg.InsidHome, "error", err)
		os.Exit(1)
	}

	keyPath := filepath.Join(r.clusterCfg.InsidHome, "keys")

	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		nodeLogger.Info("Creating keys directory", "path", keyPath)
		r.setupKeys(keyPath)
	}

	nodeDataRootPath := filepath.Join(r.clusterCfg.InsidHome, nodeId)
	if err := os.MkdirAll(nodeDataRootPath, os.ModePerm); err != nil {
		nodeLogger.Error("Could not create node data root directory", "path", nodeDataRootPath, "error", err)
		os.Exit(1)
	}

	b, err := loadOrCreateBadge(nodeId, nodeDataRootPath, nodeCfg.NodeSecret)
	if err != nil {
		nodeLogger.Error("Failed to load or create badge", "error", err)
		os.Exit(1)
	}

	nodeDir := filepath.Join(nodeDataRootPath, nodeId)
	if err := os.MkdirAll(nodeDir, os.ModePerm); err != nil {
		nodeLogger.Error("Failed to create node specific data directory", "path", nodeDir, "error", err)
		os.Exit(1)
	}

	kvm, err := tkv.New(tkv.Config{
		Identity:       b,
		Logger:         nodeLogger.WithGroup("tkv"),
		BadgerLogLevel: r.currentLogLevel,
		Directory:      nodeDir,
		AppCtx:         r.appCtx,
	})
	if err != nil {
		nodeLogger.Error("Failed to create KV manager", "error", err)
		os.Exit(1)
	}
	defer kvm.Close()

	r.service, err = core.New(
		r.appCtx, // Use the runtime's app context
		nodeLogger.WithGroup("service"),
		&nodeCfg,
		b,
		kvm,
		r.clusterCfg,
		nodeId,
		r.rootApiKey, // Pass the runtime's root API key to the service
	)
	if err != nil {
		nodeLogger.Error("Failed to create service", "error", err)
		os.Exit(1)
	}

	r.service.Run()
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

		if err := os.WriteFile(fileName, encryptedBadge, 0600); err != nil {
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

func (r *Runtime) GetRootApiKey() string {
	return r.rootApiKey
}

func (r *Runtime) GetHomeDir() string {
	return r.clusterCfg.InsidHome
}

func (r *Runtime) setupKeys(keyPath string) {

	if err := os.MkdirAll(keyPath, os.ModePerm); err != nil {
		r.logger.Error("Failed to create keys directory", "path", keyPath, "error", err)
		os.Exit(1)
	}

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		r.logger.Error("Failed to generate private key", "error", err)
		os.Exit(1)
	}

	notBefore := time.Now()
	notAfter := notBefore.AddDate(10, 0, 0)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"[ I N S I - L O C A L ]"},
			CommonName:   "insid-node",
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	// Initialize DNSNames and IPAddresses with defaults
	template.DNSNames = []string{"localhost"}
	template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}

	// Add IP addresses and DNS names from node configurations
	// This is important for the certificate to be valid for the node's bindings.
	for _, nodeCfg := range r.clusterCfg.Nodes {
		if nodeCfg.HttpBinding != "" {
			host, _, err := net.SplitHostPort(nodeCfg.HttpBinding)
			if err == nil {
				if ip := net.ParseIP(host); ip != nil {
					template.IPAddresses = append(template.IPAddresses, ip)
				} else if host != "" {
					template.DNSNames = append(template.DNSNames, host)
				}
			} else {
				if ip := net.ParseIP(nodeCfg.HttpBinding); ip != nil {
					template.IPAddresses = append(template.IPAddresses, ip)
				} else if nodeCfg.HttpBinding != "" {
					template.DNSNames = append(template.DNSNames, nodeCfg.HttpBinding)
				}
			}
		}

		if nodeCfg.ClientDomain != "" {
			clientHost, _, err := net.SplitHostPort(nodeCfg.ClientDomain)
			if err == nil {
				if ip := net.ParseIP(clientHost); ip != nil {
					template.IPAddresses = append(template.IPAddresses, ip)
				} else if clientHost != "" {
					template.DNSNames = append(template.DNSNames, clientHost)
				}
			} else { // Was likely just host
				if ip := net.ParseIP(nodeCfg.ClientDomain); ip != nil {
					template.IPAddresses = append(template.IPAddresses, ip)
				} else if nodeCfg.ClientDomain != "" {
					template.DNSNames = append(template.DNSNames, nodeCfg.ClientDomain)
				}
			}
		}
	}

	// Remove duplicates
	template.IPAddresses = removeDuplicateIPs(template.IPAddresses)
	template.DNSNames = removeDuplicateStrings(template.DNSNames)

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		r.logger.Error("Failed to create certificate", "error", err)
		os.Exit(1)
	}

	// Create PEM files for cert and key
	certOut, err := os.Create(filepath.Join(keyPath, "server.crt"))
	if err != nil {
		r.logger.Error("Failed to open server.crt for writing", "error", err)
		os.Exit(1)
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		r.logger.Error("Failed to write data to insid.crt", "error", err)
		os.Exit(1)
	}
	r.logger.Info("Generated insid.crt", "path", certOut.Name())

	keyOut, err := os.OpenFile(filepath.Join(keyPath, "server.key"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		r.logger.Error("Failed to open server.key for writing", "error", err)
		os.Exit(1)
	}
	defer keyOut.Close()
	privBytes := x509.MarshalPKCS1PrivateKey(priv)
	if err := pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privBytes}); err != nil {
		r.logger.Error("Failed to write data to server.key", "error", err)
		os.Exit(1)
	}

	// Sleep to ensure the files are written before we attempt to use them
	time.Sleep(1 * time.Second)
	r.logger.Info("Generated server.key", "path", keyOut.Name())
}

func removeDuplicateIPs(ips []net.IP) []net.IP {
	seen := make(map[string]bool)
	result := []net.IP{}
	for _, ip := range ips {
		if ip == nil {
			continue
		}
		ipStr := ip.String()
		if _, ok := seen[ipStr]; !ok {
			seen[ipStr] = true
			result = append(result, ip)
		}
	}
	return result
}

func removeDuplicateStrings(s []string) []string {
	seen := make(map[string]bool)
	result := []string{}
	for _, item := range s {
		if _, ok := seen[item]; !ok {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}
