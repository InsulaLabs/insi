package nerv

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/InsulaLabs/insi/pkg/config"
	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/interfaces"
	"github.com/InsulaLabs/insi/pkg/models"
	"github.com/fatih/color"
)

type Nerv struct {
	nodeId  string
	logger  *slog.Logger
	nodeCfg *config.Node
	cfg     *config.Cluster

	/*
		This is the root-available FWI interface that is used for admin sessions

	*/
	fwi fwi.FWI
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

func (n *Nerv) createAdminEntity(ctx context.Context) (fwi.Entity, error) {
	adminUserEntityName := fmt.Sprintf("nerv.%s", n.nodeId)

	adminEntity, err := n.fwi.CreateOrLoadEntity(ctx, adminUserEntityName, models.Limits{
		BytesOnDisk:   nervEntityLimits.BytesOnDisk,
		BytesInMemory: nervEntityLimits.BytesInMemory,
		EventsEmitted: nervEntityLimits.EventsEmitted,
		Subscribers:   nervEntityLimits.Subscribers,
	})
	if err != nil {
		n.logger.Error("Failed to create/load admin entity", "error", err)
		return nil, err
	}

	n.logger.Info("Admin entity created/loaded", "entity", adminEntity.GetName())

	return adminEntity, nil
}
