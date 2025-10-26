package nerv

import (
	"log/slog"

	"github.com/InsulaLabs/insi/pkg/config"
	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/interfaces"
	"github.com/fatih/color"
)

type Nerv struct {
	fwi     fwi.FWI
	nodeId  string
	logger  *slog.Logger
	nodeCfg *config.Node
	cfg     *config.Cluster
}

var _ interfaces.SystemObserver = &Nerv{}

func New(nodeId string, logger *slog.Logger, cfg *config.Cluster, nodeCfg *config.Node) *Nerv {
	return &Nerv{
		nodeId:  nodeId,
		logger:  logger,
		cfg:     cfg,
		nodeCfg: nodeCfg,
	}
}

func (n *Nerv) OnCoreReady() {

	n.logger.Info("Nerv is ready to start services", "node_id", n.nodeId)

	n.initializeFWI()

	isAPex := n.cfg.ApexNode == n.nodeId

	if isAPex {
		n.logger.Info("Nerv is the apex node, starting apex services", "node_id", n.nodeId)
		n.apexServices()
	}

	n.logger.Info("Starting Nerv node services", "node_id", n.nodeId)
	n.nodeServices()
}

func (n *Nerv) apexServices() {
	if err := n.startSSHServer(); err != nil {
		n.logger.Error("Failed to start SSH server", "error", err)
	}
}

func (n *Nerv) nodeServices() {

	color.HiMagenta("Nerv node services started %s", n.nodeCfg.PublicBinding)
}
