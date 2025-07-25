/*
	Bosley:	5/22/2025

	This is the "finite state machine" for raft.

	Whenever a "write" goes to an ingestion medium it is applied via the FSM.
	We tell the FSM to do it, it sends the command to the raft instance, and the raft instance
	applies the command to the system. We, then, in "Apply" do the actual work.
*/

package rft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"unsafe"

	"time"

	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/db/tkv"
	"github.com/dgraph-io/badger/v3"

	"github.com/hashicorp/raft"

	"github.com/InsulaLabs/insi/db/models"
)

/*
	See BatchSet for why we perform this check.
*/

func init() {
	var kvPayload models.KVPayload
	var tkvEntry tkv.TKVBatchEntry

	if unsafe.Sizeof(kvPayload) != unsafe.Sizeof(tkvEntry) {
		panic("rft: unsafe.Slice conversion for BatchSet will fail: models.KVPayload and tkv.TKVBatchEntry have different sizes")
	}

	if unsafe.Offsetof(kvPayload.Key) != unsafe.Offsetof(tkvEntry.Key) {
		panic("rft: unsafe.Slice conversion for BatchSet will fail: models.KVPayload.Key and tkv.TKVBatchEntry.Key have different offsets")
	}
	if unsafe.Offsetof(kvPayload.Value) != unsafe.Offsetof(tkvEntry.Value) {
		panic("rft: unsafe.Slice conversion for BatchSet will fail: models.KVPayload.Value and tkv.TKVBatchEntry.Value have different offsets")
	}
}

var EventReplayWindow = 30 * time.Second

type RaftIF interface {
	Apply(l *raft.Log) any
	Snapshot() (raft.FSMSnapshot, error)
	Restore(rc io.ReadCloser) error

	Join(followerId string, followerAddress string) error
	Leader() string
	IsLeader() bool
	LeaderHTTPAddress() (LeaderInfo, error) // Returns full URL: "https://host:http_port"
}

type ValueStoreIF interface {
	Set(kvp models.KVPayload) error
	SetNX(kvp models.KVPayload) error
	CompareAndSwap(p models.CASPayload) error
	Get(key string) (string, error)
	Delete(key string) error
	Iterate(prefix string, offset int, limit int, trimPrefix string) ([]string, error)

	BumpInteger(key string, delta int) error
}

type CacheStoreIF interface {
	SetCache(kvp models.KVPayload) error
	GetCache(key string) (string, error)
	DeleteCache(key string) error
	SetCacheNX(kvp models.KVPayload) error
	CompareAndSwapCache(p models.CASPayload) error
	IterateCache(prefix string, offset int, limit int, trimPrefix string) ([]string, error)
}

type EventIF interface {
	Publish(topic string, data any) error
}

// FSMInstance defines the interface for FSM operations.
type FSMInstance interface {
	RaftIF
	ValueStoreIF
	CacheStoreIF
	EventIF

	Close() error
}

// If given to the FSM, events that come off of the raft network
// will be sent to the event receiver.
type EventReceiverIF interface {
	Receive(topic string, data any) error
}

// Constants for FSM commands (distinct from snapshot db types)
// These are the commands that the leader will send to all the followes
// The followers will apply the command to their own state machine.
// This means we can add events to the FSM and "applying" the event
// will can trigger the remote event listeners attatched to the node that
// received the command
const (
	cmdSetValue       = "set_value"
	cmdDeleteValue    = "delete_value"
	cmdSetValueNX     = "set_value_nx"
	cmdCompareAndSwap = "compare_and_swap"
	cmdSetCache       = "set_cache"
	cmdDeleteCache    = "delete_cache"
	cmdPublishEvent   = "publish_event"

	cmdSetCacheNX          = "set_cache_nx"
	cmdCompareAndSwapCache = "compare_and_swap_cache"

	cmdBumpInteger = "bump_integer"
)

// kvFsm holds references to both BadgerDB instances.
type kvFsm struct {
	tkv        tkv.TKV
	logger     *slog.Logger
	cfg        *config.Cluster
	thisNode   string
	r          *raft.Raft
	eventRecvr EventReceiverIF
}

var _ FSMInstance = &kvFsm{}

type Settings struct {
	Ctx           context.Context
	Logger        *slog.Logger
	Config        *config.Cluster
	NodeCfg       *config.Node
	NodeId        string
	TkvDb         tkv.TKV
	EventReceiver EventReceiverIF
}

func New(settings Settings) (FSMInstance, error) {
	logger := settings.Logger.WithGroup("fsm_init")

	lockFileName := settings.NodeId + ".lock"
	lockFilePath := filepath.Join(settings.Config.InsidHome, lockFileName)
	_, errLockFile := os.Stat(lockFilePath)
	isFirstLaunch := os.IsNotExist(errLockFile)

	if isFirstLaunch {
		logger.Info("First time launch: lock file not found", "path", lockFilePath)
	} else if errLockFile != nil {
		return nil, fmt.Errorf("error checking lock file %s: %v", lockFilePath, errLockFile)
	} else {
		logger.Info("Lock file found: not a first-time launch", "path", lockFilePath)
	}

	nodeDataRootPath := filepath.Join(settings.Config.InsidHome, settings.NodeId)
	if err := os.MkdirAll(nodeDataRootPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("could not create node data root %s: %v", nodeDataRootPath, err)
	}

	kf := &kvFsm{
		logger:     settings.Logger.WithGroup(fmt.Sprintf("fsm_%s", settings.NodeId)),
		tkv:        settings.TkvDb,
		cfg:        settings.Config,
		thisNode:   settings.NodeId,
		eventRecvr: settings.EventReceiver,
	}

	currentRaftAdvertiseAddr := settings.NodeCfg.RaftBinding
	isDefaultLeader := settings.NodeId == settings.Config.DefaultLeader

	raftInstance, err := setupRaft(&SetupConfig{
		Logger:               settings.Logger.WithGroup("raft_setup"),
		NodeDir:              nodeDataRootPath,
		NodeId:               settings.NodeId,
		RaftAdvertiseAddress: currentRaftAdvertiseAddr,
		KvFsm:                kf,
		ClusterConfig:        settings.Config,
		IsDefaultLeader:      isDefaultLeader,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to setup Raft for %s: %v", settings.NodeId, err)
	}
	kf.r = raftInstance

	if isFirstLaunch && !isDefaultLeader {
		logger.Info("First launch, non-leader: attempting auto-join")
		err = attemptAutoJoin(&AutoJoinConfig{
			Logger:     settings.Logger.WithGroup("auto_join"),
			Ctx:        settings.Ctx,
			NodeId:     settings.NodeId,
			ClusterCfg: settings.Config,
			Raft:       raftInstance,
			MyRaftAddr: currentRaftAdvertiseAddr,
		})
		if err != nil {
			return nil, fmt.Errorf("auto-join failed for %s: %v", settings.NodeId, err)
		}
		logger.Info("Auto-join successful")
	}

	if isFirstLaunch {
		file, err := os.Create(lockFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create lock file %s: %v", lockFilePath, err)
		}
		file.Close()
		logger.Debug("Lock file created", "path", lockFilePath)
	}

	logger.Info("kvFsm and Raft initialized successfully")
	return kf, nil
}

func (kf *kvFsm) Close() error {
	kf.logger.Info("Closing FSM resources")
	if err := kf.tkv.Close(); err != nil {
		kf.logger.Error("Failed to close tkv", "error", err)
		return fmt.Errorf("failed to close tkv: %w", err)
	}
	return nil
}

type RaftCommand struct {
	Type    string `json:"type"`
	Payload []byte `json:"payload"`
}

func (kf *kvFsm) Apply(l *raft.Log) any {
	kf.logger.Debug("FSM Apply called", "log_type", l.Type.String(), "index", l.Index, "term", l.Term)
	switch l.Type {
	case raft.LogCommand:
		var cmd RaftCommand
		if err := json.Unmarshal(l.Data, &cmd); err != nil {
			kf.logger.Error("Could not unmarshal raft command", "error", err, "data", string(l.Data))
			return fmt.Errorf("could not unmarshal raft command: %w", err)
		}

		switch cmd.Type {
		case cmdSetValue:
			kf.logger.Debug("Applying CmdSetValue", "payload", string(cmd.Payload))

			var p models.KVPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal set_value payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal set_value payload: %w", err)
			}
			err := kf.tkv.Set(p.Key, p.Value)
			if err != nil {
				kf.logger.Error("TKV Set failed for valuesDb", "key", p.Key, "error", err)
				return fmt.Errorf("tkv Set failed for valuesDb (key %s): %w", p.Key, err)
			}
			kf.logger.Debug("FSM applied set_value", "key", p.Key)
			return nil
		case cmdSetValueNX:
			kf.logger.Debug("Applying CmdSetValueNX", "payload", string(cmd.Payload))
			var p models.KVPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal set_value_nx payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal set_value_nx payload: %w", err)
			}
			err := kf.tkv.SetNX(p.Key, p.Value)
			if err != nil {
				kf.logger.Info("TKV SetNX failed for valuesDb", "key", p.Key, "error", err)
				// Not a system error, but a condition failure, so we return it to the caller.
				return err
			}
			kf.logger.Debug("FSM applied set_value_nx", "key", p.Key)
			return nil
		case cmdCompareAndSwap:
			kf.logger.Debug("Applying CmdCompareAndSwap", "payload", string(cmd.Payload))
			var p models.CASPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal compare_and_swap payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal compare_and_swap payload: %w", err)
			}
			err := kf.tkv.CompareAndSwap(p.Key, p.OldValue, p.NewValue)
			if err != nil {
				kf.logger.Info("TKV CompareAndSwap failed for valuesDb", "key", p.Key, "error", err)
				// Not a system error, but a condition failure, so we return it to the caller.
				return err
			}
			kf.logger.Debug("FSM applied compare_and_swap", "key", p.Key)
			return nil
		case cmdDeleteValue:
			kf.logger.Debug("Applying CmdDeleteValue", "payload", string(cmd.Payload))
			var p models.KeyPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal delete_value payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal delete_value payload: %w", err)
			}
			err := kf.tkv.Delete(p.Key)
			if err != nil {
				kf.logger.Error("TKV Delete failed for valuesDb", "key", p.Key, "error", err)
				return fmt.Errorf("tkv Delete failed for valuesDb (key %s): %w", p.Key, err)
			}
			kf.logger.Debug("FSM applied delete_value", "key", p.Key)
			return nil
		case cmdSetCache:
			var p models.KVPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal set_std_cache payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal set_std_cache payload: %w", err)
			}
			kf.logger.Debug("Applying CmdSetCache", "key", p.Key, "value_len", len(p.Value))
			err := kf.tkv.CacheSet(p.Key, p.Value)
			if err != nil {
				kf.logger.Error("TKV CacheSet failed", "key", p.Key, "error", err)
				return fmt.Errorf("tkv CacheSet failed for stdCache (key %s): %w", p.Key, err)
			}
			kf.logger.Debug("FSM applied set_std_cache", "key", p.Key)
			return nil
		case cmdDeleteCache:
			var p models.KeyPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal delete_cache payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal delete_cache payload: %w", err)
			}
			err := kf.tkv.CacheDelete(p.Key)
			if err != nil {
				kf.logger.Error("TKV CacheDelete failed for stdCache", "key", p.Key, "error", err)
				return fmt.Errorf("tkv CacheDelete failed for stdCache (key %s): %w", p.Key, err)
			}
			kf.logger.Debug("FSM applied delete_cache", "key", p.Key)
			return nil
		case cmdSetCacheNX:
			kf.logger.Debug("Applying CmdSetCacheNX", "payload", string(cmd.Payload))
			var p models.KVPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal set_cache_nx payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal set_cache_nx payload: %w", err)
			}
			err := kf.tkv.CacheSetNX(p.Key, p.Value)
			if err != nil {
				kf.logger.Info("TKV CacheSetNX failed", "key", p.Key, "error", err)
				return err
			}
			kf.logger.Debug("FSM applied set_cache_nx", "key", p.Key)
			return nil
		case cmdCompareAndSwapCache:
			kf.logger.Debug("Applying CmdCompareAndSwapCache", "payload", string(cmd.Payload))
			var p models.CASPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal compare_and_swap_cache payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal compare_and_swap_cache payload: %w", err)
			}
			err := kf.tkv.CacheCompareAndSwap(p.Key, p.OldValue, p.NewValue)
			if err != nil {
				kf.logger.Info("TKV CacheCompareAndSwap failed", "key", p.Key, "error", err)
				return err
			}
			kf.logger.Debug("FSM applied compare_and_swap_cache", "key", p.Key)
			return nil
		case cmdPublishEvent:
			if kf.eventRecvr == nil {
				kf.logger.Warn("No event receiver attached to FSM, skipping publish_event")
				return nil
			}
			var p models.EventPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal publish_event payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal publish_event payload: %w", err)
			}

			// NOTE: We dont persis the events so this MAY be redundant, but its good to be safe.
			if time.Since(p.EmittedAt) > EventReplayWindow {
				kf.logger.Warn(
					"Skipping publish_event because it is outside the replay window",
					"topic", p.Topic,
					"emitted_at", p.EmittedAt,
					"replay_window", EventReplayWindow,
					"current_time", time.Now().Format(time.RFC3339Nano),
				)
				return nil
			}
			/*
				The event is fresh (it was emitted within the replay window) and we have a
				event receiver attached to the FSM so we can emit the event to the event receiver.
			*/
			kf.eventRecvr.Receive(p.Topic, p.Data)
			return nil
		case cmdBumpInteger:
			kf.logger.Debug("Applying CmdBumpInteger", "payload", string(cmd.Payload))
			var p models.KVPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal bump_integer payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal bump_integer payload: %w", err)
			}
			delta, err := strconv.ParseInt(p.Value, 10, 64)
			if err != nil {
				kf.logger.Error("Could not unmarshal bump_integer value", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal bump_integer value: %w", err)
			}

			// Use the tkv's BumpInteger method directly.
			// This is safe because Apply is serialized by Raft.
			if err := kf.tkv.BumpInteger(p.Key, delta); err != nil {
				kf.logger.Error("TKV BumpInteger failed", "key", p.Key, "error", err)
				return fmt.Errorf("tkv BumpInteger failed for key %s: %w", p.Key, err)
			}

			kf.logger.Debug("FSM applied bump_integer", "key", p.Key)
			return nil
		default:
			kf.logger.Error("Unknown raft command type in Apply", "command_type", cmd.Type)
			return fmt.Errorf("unknown raft command type: %s", cmd.Type)
		}
	case raft.LogConfiguration:
		kf.logger.Info("FSM applied raft.LogConfiguration", "index", l.Index, "term", l.Term)
		return nil
	default:
		kf.logger.Warn("FSM encountered unknown raft log type", "type", fmt.Sprintf("%#v", l.Type), "index", l.Index, "term", l.Term)
		return fmt.Errorf("unknown raft log type: %#v", l.Type)
	}
}

func (kf *kvFsm) Snapshot() (raft.FSMSnapshot, error) {
	kf.logger.Info("Creating FSM snapshot")
	return &badgerFSMSnapshot{
		valuesDb: kf.tkv.GetDataDB(),
		stdCache: kf.tkv.GetCache(),
	}, nil
}

func (kf *kvFsm) Restore(rc io.ReadCloser) error {
	kf.logger.Info("Restoring FSM from snapshot")
	defer func() {
		if errClose := rc.Close(); errClose != nil {
			kf.logger.Error("Error closing ReadCloser in Restore", "error", errClose)
		}
	}()

	decoder := json.NewDecoder(rc)
	valuesBatch := kf.tkv.GetDataDB().NewWriteBatch()
	defer valuesBatch.Cancel()

	stdCache := kf.tkv.GetCache()

	valuesCount := 0
	stdCacheCount := 0
	for {
		var entry snapshotEntry // Defined in snapshot.go
		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			kf.logger.Error("Could not decode snapshot entry during restore", "error", err)
			return fmt.Errorf("could not decode snapshot entry: %w", err)
		}

		switch entry.DBType {
		case dbTypeValues: // Defined in snapshot.go
			if err := valuesBatch.Set([]byte(entry.Key), []byte(entry.Value)); err != nil {
				kf.logger.Error("Could not set value from snapshot in valuesDb batch", "key", entry.Key, "error", err)
				return fmt.Errorf("could not set value from snapshot in valuesDb batch (key: %s): %w", entry.Key, err)
			}
			valuesCount++

		case cacheType:
			// With in-memory cache, restoring expired items is not a concern.
			// We just set the key-value pair.
			if err := stdCache.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(entry.Key), []byte(entry.Value))
			}); err != nil {
				kf.logger.Error("Could not set value from snapshot in stdCache", "key", entry.Key, "error", err)
				return fmt.Errorf("could not set value from snapshot in stdCache (key: %s): %w", entry.Key, err)
			}
			stdCacheCount++

		default:
			kf.logger.Warn("Unknown DBType in snapshot entry during restore, skipping", "db_type", entry.DBType, "key", entry.Key)
		}
	}

	if err := valuesBatch.Flush(); err != nil {
		kf.logger.Error("Failed to flush valuesDb batch during restore", "error", err)
		return fmt.Errorf("failed to flush valuesDb batch during restore: %w", err)
	}

	kf.logger.Info(
		"FSM restored successfully from snapshot",
		"values_restored",
		valuesCount,
		"std_cache_restored",
		stdCacheCount,
	)
	return nil
}

func (kf *kvFsm) Set(kvp models.KVPayload) error {
	kf.logger.Debug("SetValue called", "key", kvp.Key)
	cmd := RaftCommand{
		Type: cmdSetValue,
	}
	payloadBytes, err := json.Marshal(kvp)
	if err != nil {
		kf.logger.Error("Could not marshal payload for set_value", "key", kvp.Key, "error", err)
		return fmt.Errorf("could not marshal payload for set_value: %w", err)
	}
	cmd.Payload = payloadBytes

	cmdBytesApply, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for set_value", "key", kvp.Key, "error", err)
		return fmt.Errorf("could not marshal raft command for set_value: %w", err)
	}

	// CHECK FOR ACTUAL CHANGE BEFORE APPLYING TO NETWORK
	//
	exists, err := kf.tkv.Get(kvp.Key)
	if err == nil && exists != "" {
		if exists == kvp.Value {
			kf.logger.Debug("SetValue already exists and is the same, skipping", "key", kvp.Key)
			return nil
		}
	}

	future := kf.r.Apply(cmdBytesApply, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for SetValue", "key", kvp.Key, "error", err)
		return fmt.Errorf("raft Apply for SetValue failed (key %s): %w", kvp.Key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for SetValue", "key", kvp.Key, "error", responseErr)
		return fmt.Errorf("fsm application error for SetValue (key %s): %w", kvp.Key, responseErr)
	}
	kf.logger.Debug("SetValue Raft Apply successful", "key", kvp.Key)
	return nil
}

func (kf *kvFsm) Delete(key string) error {
	kf.logger.Debug("Delete called", "key", key)
	payload := models.KeyPayload{Key: key}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for delete_value", "key", key, "error", err)
		return fmt.Errorf("could not marshal payload for delete_value: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdDeleteValue,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for delete_value", "key", key, "error", err)
		return fmt.Errorf("could not marshal raft command for delete_value: %w", err)
	}

	// CHECK FOR ACTUAL CHANGE BEFORE APPLYING TO NETWORK
	// - If we can't get it then theres nothing to delete
	_, err = kf.tkv.Get(key)
	if err != nil {
		return nil // idempotency
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for Delete", "key", key, "error", err)
		return fmt.Errorf("raft Apply for Delete failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for Delete", "key", key, "error", responseErr)
		return fmt.Errorf("fsm application error for Delete (key %s): %w", key, responseErr)
	}
	kf.logger.Debug("Delete Raft Apply successful", "key", key)
	return nil
}

func (kf *kvFsm) Join(followerId string, followerAddress string) error {
	kf.logger.Info("Attempting to join follower to Raft cluster", "follower_id", followerId, "follower_addr", followerAddress)

	if kf.r.State() != raft.Leader {
		leaderAddr := kf.r.Leader()
		kf.logger.Warn("Join attempt on non-leader node", "current_leader", string(leaderAddr))
		return fmt.Errorf("cannot join: this node is not the leader. Current leader: %s", leaderAddr)
	}

	future := kf.r.AddVoter(raft.ServerID(followerId), raft.ServerAddress(followerAddress), 0, 0)
	if err := future.Error(); err != nil {
		kf.logger.Error("Failed to add follower/voter to Raft cluster", "follower_id", followerId, "follower_addr", followerAddress, "error", err)
		return fmt.Errorf("failed to add voter (id: %s, addr: %s): %w", followerId, followerAddress, err)
	}
	kf.logger.Debug("Successfully added follower/voter to Raft cluster", "follower_id", followerId, "follower_addr", followerAddress)
	return nil
}

// [events]

func (kf *kvFsm) Publish(topic string, data any) error {

	payload := models.EventPayload{
		Topic:     topic,
		Data:      data,
		EmittedAt: time.Now(),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for publish", "topic", topic, "error", err)
		return fmt.Errorf("could not marshal payload for publish: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdPublishEvent,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for publish", "topic", topic, "error", err)
		return fmt.Errorf("could not marshal raft command for publish: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for Publish", "topic", topic, "error", err)
		return fmt.Errorf("raft Apply for Publish failed (topic %s): %w", topic, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for Publish", "topic", topic, "error", responseErr)
		return fmt.Errorf("fsm application error for Publish (topic %s): %w", topic, responseErr)
	}
	kf.logger.Debug("Publish Raft Apply successful", "topic", topic)
	return nil
}

// ------------

// [values]

func (kf *kvFsm) Get(key string) (string, error) {
	kf.logger.Debug("Get called", "key", key)
	var value string
	value, err := kf.tkv.Get(key)
	if err != nil {
		if !tkv.IsErrKeyNotFound(err) {
			kf.logger.Debug("Failed to get value from valuesDb", "key", key, "error", err)
		}
		return "", err
	}
	kf.logger.Debug("GetValue successful", "key", key)
	return value, nil
}

func (kf *kvFsm) Iterate(prefix string, offset int, limit int, trimPrefix string) ([]string, error) {
	kf.logger.Debug("Iterate called", "prefix", prefix, "offset", offset, "limit", limit)
	var value []string
	value, err := kf.tkv.Iterate(prefix, offset, limit, trimPrefix)
	if err != nil {
		kf.logger.Error("Failed to iterate", "prefix", prefix, "offset", offset, "limit", limit, "error", err)
		return nil, err
	}
	kf.logger.Debug("Iterate successful", "prefix", prefix, "offset", offset, "limit", limit)
	return value, nil
}

func (kf *kvFsm) BumpInteger(key string, delta int) error {
	kf.logger.Debug("BumpInteger called", "key", key, "delta", delta)

	payload := models.KVPayload{
		Key:   key,
		Value: strconv.Itoa(delta),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for bump_integer", "key", key, "error", err)
		return fmt.Errorf("could not marshal payload for bump_integer: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdBumpInteger,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for bump_integer", "key", key, "error", err)
		return fmt.Errorf("could not marshal raft command for bump_integer: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for BumpInteger", "key", key, "error", err)
		return fmt.Errorf("raft Apply for BumpInteger failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for BumpInteger", "key", key, "error", responseErr)
		return fmt.Errorf("fsm application error for BumpInteger (key %s): %w", key, responseErr)
	}
	kf.logger.Debug("BumpInteger Raft Apply successful", "key", key)
	return nil
}

// [cache]

func (kf *kvFsm) SetCache(payload models.KVPayload) error {
	kf.logger.Debug("SetCache called", "key", payload.Key)

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for set_cache", "key", payload.Key, "error", err)
		return fmt.Errorf("could not marshal payload for set_cache: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdSetCache,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal payload for set_cache", "key", payload.Key, "error", err)
		return fmt.Errorf("could not marshal payload for set_cache: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for SetCache", "key", payload.Key, "error", err)
		return fmt.Errorf("raft Apply for SetCache failed (key %s): %w", payload.Key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for SetCache", "key", payload.Key, "error", responseErr)
		return fmt.Errorf("fsm application error for SetCache (key %s): %w", payload.Key, responseErr)
	}
	kf.logger.Debug("SetCache Raft Apply successful", "key", payload.Key)
	return nil
}

func (kf *kvFsm) GetCache(key string) (string, error) {
	return kf.tkv.CacheGet(key)
}

func (kf *kvFsm) DeleteCache(key string) error {
	payload := models.KeyPayload{Key: key}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for delete_cache", "key", key, "error", err)
		return fmt.Errorf("could not marshal payload for delete_cache: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdDeleteCache,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal payload for delete_cache", "key", key, "error", err)
		return fmt.Errorf("could not marshal payload for delete_cache: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for DeleteCache", "key", key, "error", err)
		return fmt.Errorf("raft Apply for DeleteCache failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for DeleteCache", "key", key, "error", responseErr)
		return fmt.Errorf("fsm application error for DeleteCache (key %s): %w", key, responseErr)
	}
	kf.logger.Debug("DeleteCache Raft Apply successful", "key", key)
	return nil
}

// ------------

func (kf *kvFsm) IsLeader() bool {
	return kf.r.State() == raft.Leader
}

func (kf *kvFsm) Leader() string {
	return string(kf.r.Leader())
}

type LeaderInfo struct {
	NodeId         string
	PublicBinding  string
	PrivateBinding string
	ClientDomain   string
}

func (fsm *kvFsm) LeaderHTTPAddress() (LeaderInfo, error) {

	// get the raw raft address from the leader
	leaderRaftAddr := fsm.r.Leader()
	if leaderRaftAddr == "" {
		return LeaderInfo{}, fmt.Errorf("no current leader")
	}

	// get the node config for the leader by matching the address

	var leaderNodeID string
	var leaderNodeConfig config.Node
	found := false
	for nodeID, nodeCfg := range fsm.cfg.Nodes {
		if nodeCfg.RaftBinding == string(leaderRaftAddr) {
			leaderNodeID = nodeID // Capture for logging
			leaderNodeConfig = nodeCfg
			found = true
			break
		}
	}

	if !found {
		return LeaderInfo{}, fmt.Errorf("leader Raft address '%s' not found in cluster configuration", leaderRaftAddr)
	}

	return LeaderInfo{
		NodeId:         leaderNodeID,
		PublicBinding:  leaderNodeConfig.PublicBinding,
		PrivateBinding: leaderNodeConfig.PrivateBinding,
		ClientDomain:   leaderNodeConfig.ClientDomain,
	}, nil
}

func (kf *kvFsm) SetNX(kvp models.KVPayload) error {
	kf.logger.Debug("SetNX called", "key", kvp.Key)

	// No pre-check needed here. The atomicity is handled by the FSM Apply method.

	cmd := RaftCommand{
		Type: cmdSetValueNX,
	}
	payloadBytes, err := json.Marshal(kvp)
	if err != nil {
		kf.logger.Error("Could not marshal payload for set_value_nx", "key", kvp.Key, "error", err)
		return fmt.Errorf("could not marshal payload for set_value_nx: %w", err)
	}
	cmd.Payload = payloadBytes

	cmdBytesApply, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for set_value_nx", "key", kvp.Key, "error", err)
		return fmt.Errorf("could not marshal raft command for set_value_nx: %w", err)
	}

	future := kf.r.Apply(cmdBytesApply, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for SetNX", "key", kvp.Key, "error", err)
		return fmt.Errorf("raft Apply for SetNX failed (key %s): %w", kvp.Key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Info("FSM application error for SetNX", "key", kvp.Key, "error", responseErr)
		return responseErr
	}
	kf.logger.Debug("SetNX Raft Apply successful", "key", kvp.Key)
	return nil
}

func (kf *kvFsm) CompareAndSwap(p models.CASPayload) error {
	kf.logger.Debug("CompareAndSwap called", "key", p.Key)

	cmd := RaftCommand{
		Type: cmdCompareAndSwap,
	}
	payloadBytes, err := json.Marshal(p)
	if err != nil {
		kf.logger.Error("Could not marshal payload for compare_and_swap", "key", p.Key, "error", err)
		return fmt.Errorf("could not marshal payload for compare_and_swap: %w", err)
	}
	cmd.Payload = payloadBytes

	cmdBytesApply, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for compare_and_swap", "key", p.Key, "error", err)
		return fmt.Errorf("could not marshal raft command for compare_and_swap: %w", err)
	}

	future := kf.r.Apply(cmdBytesApply, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for CompareAndSwap", "key", p.Key, "error", err)
		return fmt.Errorf("raft Apply for CompareAndSwap failed (key %s): %w", p.Key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Info("FSM application error for CompareAndSwap", "key", p.Key, "error", responseErr)
		return responseErr
	}
	kf.logger.Debug("CompareAndSwap Raft Apply successful", "key", p.Key)
	return nil
}

func (kf *kvFsm) SetCacheNX(kvp models.KVPayload) error {
	kf.logger.Debug("SetCacheNX called", "key", kvp.Key)
	cmd := RaftCommand{
		Type: cmdSetCacheNX,
	}
	payloadBytes, err := json.Marshal(kvp)
	if err != nil {
		kf.logger.Error("Could not marshal payload for set_cache_nx", "key", kvp.Key, "error", err)
		return fmt.Errorf("could not marshal payload for set_cache_nx: %w", err)
	}
	cmd.Payload = payloadBytes

	cmdBytesApply, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for set_cache_nx", "key", kvp.Key, "error", err)
		return fmt.Errorf("could not marshal raft command for set_cache_nx: %w", err)
	}

	future := kf.r.Apply(cmdBytesApply, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for SetCacheNX", "key", kvp.Key, "error", err)
		return fmt.Errorf("raft Apply for SetCacheNX failed (key %s): %w", kvp.Key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Info("FSM application error for SetCacheNX", "key", kvp.Key, "error", responseErr)
		return responseErr
	}
	kf.logger.Debug("SetCacheNX Raft Apply successful", "key", kvp.Key)
	return nil
}

func (kf *kvFsm) CompareAndSwapCache(p models.CASPayload) error {
	kf.logger.Debug("CompareAndSwapCache called", "key", p.Key)
	cmd := RaftCommand{
		Type: cmdCompareAndSwapCache,
	}
	payloadBytes, err := json.Marshal(p)
	if err != nil {
		kf.logger.Error("Could not marshal payload for compare_and_swap_cache", "key", p.Key, "error", err)
		return fmt.Errorf("could not marshal payload for compare_and_swap_cache: %w", err)
	}
	cmd.Payload = payloadBytes

	cmdBytesApply, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for compare_and_swap_cache", "key", p.Key, "error", err)
		return fmt.Errorf("could not marshal raft command for compare_and_swap_cache: %w", err)
	}

	future := kf.r.Apply(cmdBytesApply, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for CompareAndSwapCache", "key", p.Key, "error", err)
		return fmt.Errorf("raft Apply for CompareAndSwapCache failed (key %s): %w", p.Key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Info("FSM application error for CompareAndSwapCache", "key", p.Key, "error", responseErr)
		return responseErr
	}
	kf.logger.Debug("CompareAndSwapCache Raft Apply successful", "key", p.Key)
	return nil
}

func (kf *kvFsm) IterateCache(prefix string, offset int, limit int, trimPrefix string) ([]string, error) {
	kf.logger.Debug("IterateCache called", "prefix", prefix, "offset", offset, "limit", limit)
	keys, err := kf.tkv.CacheIterate(prefix, offset, limit, trimPrefix)
	if err != nil {
		kf.logger.Error("Failed to iterate cache", "prefix", prefix, "offset", offset, "limit", limit, "error", err)
		return nil, err
	}
	kf.logger.Debug("IterateCache successful", "prefix", prefix, "offset", offset, "limit", limit)
	return keys, nil
}
