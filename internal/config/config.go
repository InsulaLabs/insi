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
	RaftBinding string `yaml:"raftBinding"`
	HttpBinding string `yaml:"httpBinding"`
	NodeSecret  string `yaml:"nodeSecret"`
}

type Cache struct {
	StandardTTL time.Duration `yaml:"standard-ttl"`
	Keys        time.Duration `yaml:"keys"`
}

type TLS struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
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
}

type RateLimiterConfig struct {
	Limit float64 `yaml:"limit"` // Requests per second
	Burst int     `yaml:"burst"` // Burst size
}

type RateLimiters struct {
	Values         RateLimiterConfig `yaml:"values"`
	Tags           RateLimiterConfig `yaml:"tags"`
	CacheEndpoints RateLimiterConfig `yaml:"cacheEndpoints"`
	System         RateLimiterConfig `yaml:"system"`
	Default        RateLimiterConfig `yaml:"default"`
}

var (
	ErrConfigFileMissing        = errors.New("config file is missing")
	ErrConfigFileUnreadable     = errors.New("config file is unreadable")
	ErrConfigFileUnmarshallable = errors.New("config file is unmarshallable")
	ErrInstanceSecretMissing    = errors.New("instanceSecret is missing in config")
	ErrDefaultLeaderMissing     = errors.New("defaultLeader is not set in config")
	ErrNodesMissing             = errors.New("no nodes defined in config")
	ErrInsudbDirMissing         = errors.New("insudbDir is missing in config and is required for lock files and node data")
	ErrTLSMissing               = errors.New("TLS configuration incomplete: both cert and key must be provided if one is specified")
	ErrDuplicateNodeSecret      = errors.New("duplicate node secret in config - each node must contain a unique nodeSecret")
	ErrCacheKeysMissing         = errors.New("cache.keys is missing in config")
	ErrCacheStandardTTLMissing  = errors.New("cache.standardTTL is missing in config")
	ErrRootPrefixMissing        = errors.New("rootPrefix is missing in config")
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

	return &cfg, nil
}
