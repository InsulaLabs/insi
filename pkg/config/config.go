package config

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v3"
)

const (
	RaftDataDirName     = "raft_data"
	BadgerValuesDirName = "values"
	BadgerTagsDirName   = "tags"
)

type Node struct {
	RaftBinding    string `yaml:"raftBinding"`
	PrivateBinding string `yaml:"privateBinding"`
	PublicBinding  string `yaml:"publicBinding"`
	NodeSecret     string `yaml:"nodeSecret"`
	ClientDomain   string `yaml:"clientDomain,omitempty"`
}

type Cache struct {
	StandardTTL time.Duration `yaml:"standard-ttl"`
	Keys        time.Duration `yaml:"keys"`
}

type TLS struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

type SessionsConfig struct {
	EventChannelSize         int `yaml:"eventChannelSize"`
	WebSocketReadBufferSize  int `yaml:"webSocketReadBufferSize"`
	WebSocketWriteBufferSize int `yaml:"webSocketWriteBufferSize"`
	MaxConnections           int `yaml:"maxConnections"`
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

type Cluster struct {
	InstanceSecret   string          `yaml:"instanceSecret"`     // on config load no two nodes should have the same instance secret
	DefaultLeader    string          `yaml:"defaultLeader"`      // if first time launch, non-leaders will auto-follow this leader - need to set this and
	ApexNode         string          `yaml:"apexNode,omitempty"` // The apex node that provides SSH access (separate from raft leader)
	Nodes            map[string]Node `yaml:"nodes"`
	TLS              TLS             `yaml:"tls"`
	ClientSkipVerify bool            `yaml:"clientSkipVerify"` // Across all nodes, if true, then their "join" clients will permit skip of TLS verification
	TrustedProxies   []string        `yaml:"trustedProxies,omitempty"`
	PermittedIPs     []string        `yaml:"permittedIPs,omitempty"`
	InsidHome        string          `yaml:"insiHome"`
	ServerMustUseTLS bool            `yaml:"serverMustUseTLS"` // Across all nodes, if true, then their "join" clients will permit skip of TLS verification
	Cache            Cache           `yaml:"cache"`
	RootPrefix       string          `yaml:"rootPrefix"`
	RateLimiters     RateLimiters    `yaml:"rateLimiters"`
	Sessions         SessionsConfig  `yaml:"sessions"`
	Logging          LoggingConfig   `yaml:"logging"`

	AdminSSHKeys []string `yaml:"adminSSHKeys,omitempty"`
	SSHPort      int      `yaml:"sshPort,omitempty"`
	HostKeyPath  string   `yaml:"hostKeyPath,omitempty"`
}

type RateLimiterConfig struct {
	Limit float64 `yaml:"limit"` // Requests per second
	Burst int     `yaml:"burst"` // Burst size
}

type RateLimiters struct {
	Values  RateLimiterConfig `yaml:"values"`
	Cache   RateLimiterConfig `yaml:"cache"`
	System  RateLimiterConfig `yaml:"system"`
	Default RateLimiterConfig `yaml:"default"`
	Events  RateLimiterConfig `yaml:"events"`
}

var (
	ErrConfigFileMissing                       = errors.New("config file is missing")
	ErrConfigFileUnreadable                    = errors.New("config file is unreadable")
	ErrConfigFileUnmarshallable                = errors.New("config file is unmarshallable")
	ErrInstanceSecretMissing                   = errors.New("instanceSecret is missing in config")
	ErrDefaultLeaderMissing                    = errors.New("defaultLeader is not set in config")
	ErrNodesMissing                            = errors.New("no nodes defined in config")
	ErrInsidHomeMissing                        = errors.New("insidHome is missing in config and is required for lock files and node data")
	ErrTLSMissing                              = errors.New("TLS configuration incomplete: both cert and key must be provided if one is specified")
	ErrDuplicateNodeSecret                     = errors.New("duplicate node secret in config - each node must contain a unique nodeSecret")
	ErrCacheKeysMissing                        = errors.New("cache.keys is missing in config")
	ErrCacheStandardTTLMissing                 = errors.New("cache.standardTTL is missing in config")
	ErrRootPrefixMissing                       = errors.New("rootPrefix is missing in config")
	ErrRateLimitersValuesLimitMissing          = errors.New("rateLimiters.values.limit is missing in config")
	ErrRateLimitersCacheLimitMissing           = errors.New("rateLimiters.cache.limit is missing in config")
	ErrRateLimitersSystemLimitMissing          = errors.New("rateLimiters.system.limit is missing in config")
	ErrRateLimitersDefaultLimitMissing         = errors.New("rateLimiters.default.limit is missing in config")
	ErrRateLimitersEventsLimitMissing          = errors.New("rateLimiters.events.limit is missing in config")
	ErrSessionsEventChannelSizeMissing         = errors.New("sessions.eventChannelSize is missing or invalid in config")
	ErrSessionsWebSocketReadBufferSizeMissing  = errors.New("sessions.webSocketReadBufferSize is missing or invalid in config")
	ErrSessionsWebSocketWriteBufferSizeMissing = errors.New("sessions.webSocketWriteBufferSize is missing or invalid in config")
	ErrSessionsMaxConnectionsMissing           = errors.New("sessions.maxConnections is missing or invalid in config")
	ErrHostKeyGeneration                       = errors.New("failed to generate SSH host key")
	ErrApexNodeNotFound                        = errors.New("apexNode specified but does not exist in nodes configuration")
	ErrApexNodeMissing                         = errors.New("apexNode must be specified when SSH configuration is present (sshPort, hostKeyPath, or adminSSHKeys)")
)

func generateHostKey(keyPath string) error {
	dir := filepath.Dir(keyPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}

	privateKeyPEM, err := ssh.MarshalPrivateKey(privateKey, "")
	if err != nil {
		return err
	}

	return os.WriteFile(keyPath, pem.EncodeToMemory(privateKeyPEM), 0600)
}

func LoadConfig(configFile string) (*Cluster, error) {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, ErrConfigFileUnreadable
	}

	var cfg Cluster
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, ErrConfigFileUnmarshallable
	}

	// Basic validation
	if cfg.InstanceSecret == "" {
		return nil, ErrInstanceSecretMissing
	}
	if cfg.DefaultLeader == "" && len(cfg.Nodes) > 0 {
		return nil, ErrDefaultLeaderMissing
	}
	if len(cfg.Nodes) == 0 {
		return nil, ErrNodesMissing
	}

	seenNodeSecrets := make(map[string]bool)
	for _, node := range cfg.Nodes {
		if node.RaftBinding == "" || node.PrivateBinding == "" || node.PublicBinding == "" {
			return nil, errors.New("raftBinding, privateBinding and publicBinding are required for each node")
		}
		if seenNodeSecrets[node.NodeSecret] {
			return nil, ErrDuplicateNodeSecret
		}
		seenNodeSecrets[node.NodeSecret] = true
	}

	if cfg.InsidHome == "" {
		return nil, ErrInsidHomeMissing
	}

	if cfg.ServerMustUseTLS && (cfg.TLS.Cert == "" || cfg.TLS.Key == "") {
		return nil, ErrTLSMissing
	}

	if cfg.TLS.Cert != "" && cfg.TLS.Key == "" ||
		cfg.TLS.Cert == "" && cfg.TLS.Key != "" {
		return nil, ErrTLSMissing
	}

	if cfg.Cache.Keys == 0 {
		return nil, ErrCacheKeysMissing
	}

	if cfg.Cache.StandardTTL == 0 {
		return nil, ErrCacheStandardTTLMissing
	}

	if cfg.RootPrefix == "" {
		return nil, ErrRootPrefixMissing
	}

	if cfg.RateLimiters.Values.Limit == 0 {
		return nil, ErrRateLimitersValuesLimitMissing
	}
	if cfg.RateLimiters.Cache.Limit == 0 {
		return nil, ErrRateLimitersCacheLimitMissing
	}
	if cfg.RateLimiters.System.Limit == 0 {
		return nil, ErrRateLimitersSystemLimitMissing
	}
	if cfg.RateLimiters.Default.Limit == 0 {
		return nil, ErrRateLimitersDefaultLimitMissing
	}
	if cfg.RateLimiters.Events.Limit == 0 {
		return nil, ErrRateLimitersEventsLimitMissing
	}
	if cfg.Sessions.EventChannelSize <= 0 {
		return nil, ErrSessionsEventChannelSizeMissing
	}
	if cfg.Sessions.WebSocketReadBufferSize <= 0 {
		return nil, ErrSessionsWebSocketReadBufferSizeMissing
	}
	if cfg.Sessions.WebSocketWriteBufferSize <= 0 {
		return nil, ErrSessionsWebSocketWriteBufferSizeMissing
	}
	if cfg.Sessions.MaxConnections <= 0 {
		return nil, ErrSessionsMaxConnectionsMissing
	}

	sshConfigured := cfg.SSHPort > 0 || cfg.HostKeyPath != "" || len(cfg.AdminSSHKeys) > 0
	if sshConfigured && cfg.ApexNode == "" {
		return nil, ErrApexNodeMissing
	}

	if cfg.ApexNode != "" {
		if _, ok := cfg.Nodes[cfg.ApexNode]; !ok {
			return nil, ErrApexNodeNotFound
		}
	}

	if cfg.HostKeyPath != "" {
		if _, err := os.Stat(cfg.HostKeyPath); os.IsNotExist(err) {
			if err := generateHostKey(cfg.HostKeyPath); err != nil {
				return nil, ErrHostKeyGeneration
			}
		}
	}

	return &cfg, nil
}

func GenerateConfig(configFile string) (*Cluster, error) {
	cfg := Cluster{
		InstanceSecret:   "please_change_this_secret_in_production_!!!",
		DefaultLeader:    "node0",
		ApexNode:         "node0",
		Nodes:            make(map[string]Node),
		ClientSkipVerify: false,
		TrustedProxies:   []string{"127.0.0.1", "::1"},
		PermittedIPs:     []string{},
		InsidHome:        "data/insi",
		ServerMustUseTLS: true,
		TLS: TLS{
			Cert: "keys/server.crt",
			Key:  "keys/server.key",
		},
		Cache: Cache{
			StandardTTL: 5 * time.Minute,
			Keys:        1 * time.Hour,
		},
		RootPrefix: "please_change_this_root_key_prefix!!!!!",
		RateLimiters: RateLimiters{
			Values:  RateLimiterConfig{Limit: 100.0, Burst: 200},
			Cache:   RateLimiterConfig{Limit: 100.0, Burst: 200},
			System:  RateLimiterConfig{Limit: 50.0, Burst: 100},
			Default: RateLimiterConfig{Limit: 100.0, Burst: 200},
			Events:  RateLimiterConfig{Limit: 200.0, Burst: 400},
		},
		Sessions: SessionsConfig{
			EventChannelSize:         1000,
			WebSocketReadBufferSize:  4096,
			WebSocketWriteBufferSize: 4096,
			MaxConnections:           100,
		},
		AdminSSHKeys: []string{},
		SSHPort:      2222,
		HostKeyPath:  "keys/ssh_host_key",
	}

	cfg.Nodes["node0"] = Node{
		RaftBinding:    "127.0.0.1:7000",
		PrivateBinding: "127.0.0.1:8080",
		PublicBinding:  "127.0.0.1:7001",
		NodeSecret:     "node0_secret_please_change_!!!",
		ClientDomain:   "localhost",
	}
	// Add more nodes if desired for a default multi-node setup example
	cfg.Nodes["node1"] = Node{
		RaftBinding:    "127.0.0.1:7002",
		PrivateBinding: "127.0.0.1:8081",
		PublicBinding:  "127.0.0.1:7003",
		NodeSecret:     "node1_secret_please_change_!!!",
		ClientDomain:   "localhost",
	}
	cfg.Nodes["node2"] = Node{
		RaftBinding:    "127.0.0.1:7004",
		PrivateBinding: "127.0.0.1:8082",
		PublicBinding:  "127.0.0.1:7005",
		NodeSecret:     "node2_secret_please_change_!!!",
		ClientDomain:   "localhost",
	}

	// The configFile argument is not used by this function to generate the content,
	// but its presence matches the function signature. The actual writing to a file
	// based on a command-line flag is handled in the runtime.
	return &cfg, nil
}
