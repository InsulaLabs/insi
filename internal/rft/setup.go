package rft

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/InsulaLabs/insi/config"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type SetupConfig struct {
	IsDefaultLeader      bool
	Logger               *slog.Logger
	NodeDir              string
	NodeId               string
	RaftAdvertiseAddress string
	KvFsm                *kvFsm
	ClusterConfig        *config.Cluster
}

// setupRaft is modified to take clusterConfig and isDefaultLeader flag
func setupRaft(cfg *SetupConfig) (*raft.Raft, error) {

	raftDataPath := filepath.Join(cfg.NodeDir, config.RaftDataDirName)

	if err := os.MkdirAll(raftDataPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("could not create raft data directory %s: %w", raftDataPath, err)
	}

	boltDBPath := filepath.Join(raftDataPath, "bolt.db")
	store, err := raftboltdb.NewBoltStore(boltDBPath)
	if err != nil {
		return nil, fmt.Errorf("could not create bolt store at %s: %w", boltDBPath, err)
	}

	snapshotStorePath := filepath.Join(raftDataPath, "snapshots")
	if err := os.MkdirAll(snapshotStorePath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("could not create snapshot directory %s: %w", snapshotStorePath, err)
	}
	snapshots, err := raft.NewFileSnapshotStore(snapshotStorePath, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create snapshot store at %s: %w", snapshotStorePath, err)
	}

	parsedRaftAddr, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvertiseAddress)
	if err != nil {
		return nil, fmt.Errorf("could not resolve raft advertise address %s: %w", cfg.RaftAdvertiseAddress, err)
	}

	transport, err := raft.NewTCPTransport(cfg.RaftAdvertiseAddress, parsedRaftAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create tcp transport (advertise: %s): %w", cfg.RaftAdvertiseAddress, err)
	}
	cfg.Logger.Info(
		"Raft TCP transport created",
		"listening_on", transport.LocalAddr(),
		"advertising", cfg.RaftAdvertiseAddress,
	)

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeId)
	// raftCfg.LogLevel = "DEBUG" // Example: enable more detailed Raft logging if needed

	r, err := raft.NewRaft(raftCfg, cfg.KvFsm, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("could not create raft instance for node %s: %w", cfg.NodeId, err)
	}

	hasState, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing Raft state for node %s: %w", cfg.NodeId, err)
	}

	if !hasState {
		if cfg.IsDefaultLeader {
			bootstrapCfg := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raft.ServerID(cfg.NodeId),
						Address: raft.ServerAddress(cfg.RaftAdvertiseAddress), // Use the advertised address
					},
				},
			}
			bootstrapFuture := r.BootstrapCluster(bootstrapCfg)
			if err := bootstrapFuture.Error(); err != nil {
				return nil, fmt.Errorf("could not bootstrap cluster for node %s: %w", cfg.NodeId, err)
			}
			cfg.Logger.Info(
				"Raft cluster successfully bootstrapped for default leader node",
				"node_id", cfg.NodeId,
				"address", cfg.RaftAdvertiseAddress,
			)
		} else {
			cfg.Logger.Info(
				"Node has no existing Raft state. Will attempt to join leader",
				"node_id", cfg.NodeId, "leader", cfg.ClusterConfig.DefaultLeader)
			// Auto-join logic in main() will handle the joining process for non-default-leaders on first launch.
		}
	} else {
		cfg.Logger.Info(
			"Existing Raft state found for node. Skipping bootstrap/join logic in setupRaft.",
			"node_id", cfg.NodeId,
		)
	}

	return r, nil
}
