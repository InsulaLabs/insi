package rft

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/InsulaLabs/insi/internal/config"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// setupRaft is modified to take clusterConfig and isDefaultLeader flag
func setupRaft(nodeDir, nodeId, raftAdvertiseAddress string, kf *kvFsm, clusterConfig *config.Cluster, isDefaultLeaderNode bool) (*raft.Raft, error) {
	raftDataPath := filepath.Join(nodeDir, config.RaftDataDirName)

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

	parsedRaftAddr, err := net.ResolveTCPAddr("tcp", raftAdvertiseAddress)
	if err != nil {
		return nil, fmt.Errorf("could not resolve raft advertise address %s: %w", raftAdvertiseAddress, err)
	}

	transport, err := raft.NewTCPTransport(raftAdvertiseAddress, parsedRaftAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create tcp transport (advertise: %s): %w", raftAdvertiseAddress, err)
	}
	log.Printf("Raft TCP transport created. Listening on: %s, Advertising: %s", transport.LocalAddr(), raftAdvertiseAddress)

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(nodeId)
	// raftCfg.LogLevel = "DEBUG" // Example: enable more detailed Raft logging if needed

	r, err := raft.NewRaft(raftCfg, kf, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("could not create raft instance for node %s: %w", nodeId, err)
	}

	hasState, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing Raft state for node %s: %w", nodeId, err)
	}

	if !hasState {
		if isDefaultLeaderNode {
			log.Printf("No existing Raft state found for node %s (default leader). Bootstrapping new cluster...", nodeId)
			bootstrapCfg := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raft.ServerID(nodeId),
						Address: raft.ServerAddress(raftAdvertiseAddress), // Use the advertised address
					},
				},
			}
			bootstrapFuture := r.BootstrapCluster(bootstrapCfg)
			if err := bootstrapFuture.Error(); err != nil {
				return nil, fmt.Errorf("could not bootstrap cluster for node %s: %w", nodeId, err)
			}
			log.Printf("Raft cluster successfully bootstrapped for default leader node %s with address %s", nodeId, raftAdvertiseAddress)
		} else {
			log.Printf("Node %s: No existing Raft state. Will attempt to join leader %s (not bootstrapping).", nodeId, clusterConfig.DefaultLeader)
			// Auto-join logic in main() will handle the joining process for non-default-leaders on first launch.
		}
	} else {
		log.Printf("Existing Raft state found for node %s. Skipping bootstrap/join logic in setupRaft.", nodeId)
	}

	return r, nil
}
