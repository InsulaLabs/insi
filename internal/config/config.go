package config

import (
	"errors"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	RaftDataDirName     = "raft_data"
	BadgerValuesDirName = "values"
	BadgerTagsDirName   = "tags"
)

type Node struct {
	RaftBinding  string `yaml:"raftBinding"`
	HttpBinding  string `yaml:"httpBinding"`
	NodeSecret   string `yaml:"nodeSecret"`
	ClientDomain string `yaml:"clientDomain,omitempty"`
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

type Cluster struct {
	InstanceSecret   string          `yaml:"instanceSecret"` // on config load no two nodes should have the same instance secret
	DefaultLeader    string          `yaml:"defaultLeader"`  // if first time launch, non-leaders will auto-follow this leader - need to set this and
	Nodes            map[string]Node `yaml:"nodes"`
	TLS              TLS             `yaml:"tls"`
	ClientSkipVerify bool            `yaml:"clientSkipVerify"` // Across all nodes, if true, then their "join" clients will permit skip of TLS verification
	InsudbDir        string          `yaml:"insudbDir"`
	ServerMustUseTLS bool            `yaml:"serverMustUseTLS"` // Across all nodes, if true, then their "join" clients will permit skip of TLS verification
	Cache            Cache           `yaml:"cache"`
	RootPrefix       string          `yaml:"rootPrefix"`
	RateLimiters     RateLimiters    `yaml:"rateLimiters"`
	Sessions         SessionsConfig  `yaml:"sessions"`
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
	Objects RateLimiterConfig `yaml:"objects"`
}

var (
	ErrConfigFileMissing                       = errors.New("config file is missing")
	ErrConfigFileUnreadable                    = errors.New("config file is unreadable")
	ErrConfigFileUnmarshallable                = errors.New("config file is unmarshallable")
	ErrInstanceSecretMissing                   = errors.New("instanceSecret is missing in config")
	ErrDefaultLeaderMissing                    = errors.New("defaultLeader is not set in config")
	ErrNodesMissing                            = errors.New("no nodes defined in config")
	ErrInsudbDirMissing                        = errors.New("insudbDir is missing in config and is required for lock files and node data")
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
)

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
		if node.RaftBinding == "" || node.HttpBinding == "" {
			return nil, errors.New("raftBinding and httpBinding are required for each node")
		}
		if seenNodeSecrets[node.NodeSecret] {
			return nil, ErrDuplicateNodeSecret
		}
		seenNodeSecrets[node.NodeSecret] = true
	}

	if cfg.InsudbDir == "" {
		return nil, ErrInsudbDirMissing
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

	return &cfg, nil
}
