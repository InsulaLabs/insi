package nerv

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/InsulaLabs/insi/pkg/client"
	"github.com/InsulaLabs/insi/pkg/config"
	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/interfaces"
	"github.com/fatih/color"
)

/*


	if c.cfg.ApexNode == "" {
		c.logger.Info("SSH server disabled - no apex node configured")
	} else if c.cfg.SSHPort > 0 && c.cfg.HostKeyPath != "" && c.fwi != nil && isApex {
		c.logger.Info("Starting SSH server on apex node", "apex_node", c.nodeName)
		if err := c.startSSHServer(); err != nil {
			c.logger.Error("Failed to start SSH server", "error", err)
		}
	} else if c.cfg.SSHPort > 0 && c.cfg.HostKeyPath != "" {
		c.logger.Info("SSH server not started - this node is not the apex", "current_node", c.nodeName, "apex_node", c.cfg.ApexNode)
	}

*/

type Nerv struct {
	fwi     fwi.FWI
	nodeId  string
	logger  *slog.Logger
	nodeCfg *config.Node
	cfg     *config.Cluster
}

var _ interfaces.SystemObserver = &Nerv{}

func New(nodeId string, logger *slog.Logger, cfg *config.Cluster, nodeCfg *config.Node) *Nerv {
	n := &Nerv{
		nodeId:  nodeId,
		logger:  logger,
		cfg:     cfg,
		nodeCfg: nodeCfg,
	}

	n.initializeFWI()

	return n
}

func (n *Nerv) initializeFWI() {
	endpoints := []client.Endpoint{}
	for _, node := range n.cfg.Nodes {
		endpoints = append(endpoints, client.Endpoint{
			PublicBinding:  node.PublicBinding,
			PrivateBinding: node.PrivateBinding,
			ClientDomain:   node.ClientDomain,
		})
	}

	rootApiKey := sha256.New()
	rootApiKey.Write([]byte(n.cfg.InstanceSecret))
	rootApiKeyHex := hex.EncodeToString(rootApiKey.Sum(nil))
	rootApiKeyBase64 := base64.StdEncoding.EncodeToString([]byte(rootApiKeyHex))

	n.logger.Info("Initializing FWI", "skip_verify", n.cfg.ClientSkipVerify, "endpoints_count", len(endpoints))

	fwi, err := fwi.NewFWI(&client.Config{
		Logger:         n.logger.WithGroup("fwi"),
		ConnectionType: client.ConnectionTypeRandom,
		ApiKey:         rootApiKeyBase64,
		SkipVerify:     n.cfg.ClientSkipVerify,
		Timeout:        time.Second * 30,
		Endpoints:      endpoints,
	}, n.logger.WithGroup("fwi"))

	if err != nil {
		n.logger.Error("Failed to initialize FWI after root keys verified", "error", err)
		return
	}

	n.fwi = fwi
	n.logger.Info("FWI initialized successfully after root keys verified", "skip_verify", n.cfg.ClientSkipVerify)
}

func (n *Nerv) OnCoreReady() {
	isAPex := n.cfg.ApexNode == n.nodeId

	if isAPex {
		if err := n.startSSHServer(); err != nil {
			n.logger.Error("Failed to start SSH server", "error", err)
		}
	} else {
		n.logger.Info("SSH server not started - this node is not the apex", "current_node", n.nodeCfg.RaftBinding, "apex_node", n.cfg.ApexNode)
	}

	if isAPex {
		color.HiCyan("APEX RUNNING %s", n.nodeCfg.PublicBinding)
	} else {
		color.HiYellow("NODE %s RUNNING", n.nodeCfg.PublicBinding)
	}
}
