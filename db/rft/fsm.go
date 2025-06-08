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
	"net"
	"os"
	"path/filepath"
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
	LeaderHTTPAddress() (string, error) // Returns full URL: "https://host:http_port"
}

type ValueStoreIF interface {
	Set(kvp models.KVPayload) error
	Get(key string) (string, error)
	Delete(key string) error
	Iterate(prefix string, offset int, limit int) ([]string, error)
}

type BatchIF interface {
	BatchSet(items []models.KVPayload) error
	BatchDelete(keys []models.KeyPayload) error
}

type CacheStoreIF interface {
	SetCache(kvp models.KVPayload) error
	GetCache(key string) (string, error)
	DeleteCache(key string) error
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

	BatchIF

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
	cmdSetValue          = "set_value"
	cmdDeleteValue       = "delete_value"
	cmdSetCache          = "set_cache"
	cmdDeleteCache       = "delete_cache"
	cmdPublishEvent      = "publish_event"
	cmdBatchSetValues    = "batch_set_values"
	cmdBatchDeleteValues = "batch_delete_values"
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
		case cmdBatchSetValues:
			var batchItems []tkv.TKVBatchEntry
			if err := json.Unmarshal(cmd.Payload, &batchItems); err != nil {
				kf.logger.Error("Could not unmarshal batch_set_values payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal batch_set_values payload: %w", err)
			}
			err := kf.tkv.BatchSet(batchItems)
			if err != nil {
				kf.logger.Error("TKV BatchSet failed", "item_count", len(batchItems), "error", err)
				return fmt.Errorf("tkv BatchSet failed (item_count %d): %w", len(batchItems), err)
			}
			kf.logger.Debug("FSM applied batch_set_values", "item_count", len(batchItems))
			return nil
		case cmdBatchDeleteValues:
			var keys []string
			if err := json.Unmarshal(cmd.Payload, &keys); err != nil {
				kf.logger.Error("Could not unmarshal batch_delete_values payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal batch_delete_values payload: %w", err)
			}
			err := kf.tkv.BatchDelete(keys)
			if err != nil {
				kf.logger.Error("TKV BatchDelete failed", "key_count", len(keys), "error", err)
				return fmt.Errorf("tkv BatchDelete failed (key_count %d): %w", len(keys), err)
			}
			kf.logger.Debug("FSM applied batch_delete_values", "key_count", len(keys))
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
		if err != badger.ErrKeyNotFound {
			kf.logger.Error("Failed to get value from valuesDb", "key", key, "error", err)
		}
		return "", err
	}
	kf.logger.Debug("GetValue successful", "key", key)
	return value, nil
}

func (kf *kvFsm) Iterate(prefix string, offset int, limit int) ([]string, error) {
	kf.logger.Debug("Iterate called", "prefix", prefix, "offset", offset, "limit", limit)
	var value []string
	value, err := kf.tkv.Iterate(prefix, offset, limit)
	if err != nil {
		kf.logger.Error("Failed to iterate", "prefix", prefix, "offset", offset, "limit", limit, "error", err)
		return nil, err
	}
	kf.logger.Debug("Iterate successful", "prefix", prefix, "offset", offset, "limit", limit)
	return value, nil
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

// Batch operations to be called by service layer, these will go through Raft
func (kf *kvFsm) BatchSet(items []models.KVPayload) error {
	kf.logger.Debug("FSM BatchSet called", "item_count", len(items))
	if len(items) == 0 {
		return nil // This ensures items[0] is safe to access below.
	}

	// =======================================================================================

	// Unsafe conversion from []models.KVPayload to []tkv.TKVBatchEntry.
	// WARNING: This is only safe if models.KVPayload and tkv.TKVBatchEntry
	// have an identical memory layout. Changes to either struct could
	// lead to memory corruption or crashes. Key prefixing is assumed to be
	// handled in routes_w.go before calling this FSM method.
	/*
		The alternative is to loop over and copy the values into a new slice.
		That is no bueno and we only seperate the types for the sake of clarity.
	*/
	tkvEntries := unsafe.Slice((*tkv.TKVBatchEntry)(unsafe.Pointer(&items[0])), len(items))

	// =======================================================================================

	cmd := RaftCommand{
		Type: cmdBatchSetValues,
	}
	payloadBytes, err := json.Marshal(tkvEntries)
	if err != nil {
		kf.logger.Error("Could not marshal payload for batch_set_values", "item_count", len(items), "error", err)
		return fmt.Errorf("could not marshal payload for batch_set_values: %w", err)
	}
	cmd.Payload = payloadBytes

	cmdBytesApply, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for batch_set_values", "item_count", len(items), "error", err)
		return fmt.Errorf("could not marshal raft command for batch_set_values: %w", err)
	}

	future := kf.r.Apply(cmdBytesApply, 5*time.Second)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for BatchSet", "item_count", len(items), "error", err)
		return fmt.Errorf("raft Apply for BatchSet failed (item_count %d): %w", len(items), err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for BatchSet", "item_count", len(items), "error", responseErr)
		return fmt.Errorf("fsm application error for BatchSet (item_count %d): %w", len(items), responseErr)
	}
	kf.logger.Debug("FSM BatchSet Raft Apply successful", "item_count", len(items))
	return nil
}

func (kf *kvFsm) BatchDelete(keyPayloads []models.KeyPayload) error {
	kf.logger.Debug("FSM BatchDelete called", "key_count", len(keyPayloads))
	if len(keyPayloads) == 0 {
		return nil
	}

	keys := make([]string, len(keyPayloads))
	for i, p := range keyPayloads {
		// Key prefixing will be handled in routes_w.go before calling this FSM method
		keys[i] = p.Key
	}

	cmd := RaftCommand{
		Type: cmdBatchDeleteValues,
	}
	payloadBytes, err := json.Marshal(keys)
	if err != nil {
		kf.logger.Error("Could not marshal payload for batch_delete_values", "key_count", len(keys), "error", err)
		return fmt.Errorf("could not marshal payload for batch_delete_values: %w", err)
	}
	cmd.Payload = payloadBytes

	cmdBytesApply, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for batch_delete_values", "key_count", len(keys), "error", err)
		return fmt.Errorf("could not marshal raft command for batch_delete_values: %w", err)
	}

	future := kf.r.Apply(cmdBytesApply, 5*time.Second)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for BatchDelete", "key_count", len(keys), "error", err)
		return fmt.Errorf("raft Apply for BatchDelete failed (key_count %d): %w", len(keys), err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for BatchDelete", "key_count", len(keys), "error", responseErr)
		return fmt.Errorf("fsm application error for BatchDelete (key_count %d): %w", len(keys), responseErr)
	}
	kf.logger.Debug("FSM BatchDelete Raft Apply successful", "key_count", len(keys))
	return nil
}

func (kf *kvFsm) IsLeader() bool {
	return kf.r.State() == raft.Leader
}

func (kf *kvFsm) Leader() string {
	return string(kf.r.Leader())
}

func (fsm *kvFsm) LeaderHTTPAddress() (string, error) {
	leaderRaftAddr := fsm.r.Leader() // This is host:raft_port
	if leaderRaftAddr == "" {
		return "", fmt.Errorf("no current leader")
	}

	var leaderNodeID string // For logging
	var leaderNodeConfig config.Node
	found := false
	for nodeID, nodeCfg := range fsm.cfg.Nodes { // Iterate to find the node with this RaftBinding
		if nodeCfg.RaftBinding == string(leaderRaftAddr) {
			leaderNodeID = nodeID // Capture for logging
			leaderNodeConfig = nodeCfg
			found = true
			break
		}
	}

	if !found {
		return "", fmt.Errorf("leader Raft address '%s' not found in cluster configuration", leaderRaftAddr)
	}

	// HttpBinding itself is host:port.
	// ClientDomain is just a host.
	hostPartFromBinding, portPart, err := net.SplitHostPort(leaderNodeConfig.HttpBinding)
	if err != nil {
		fsm.logger.Error("Failed to parse leader HttpBinding in LeaderHTTPAddress",
			"leader_node_id", leaderNodeID,
			"http_binding", leaderNodeConfig.HttpBinding,
			"error", err)
		// Fallback to returning the raw HttpBinding; the caller (redirectToLeader) will attempt to parse it again or use as is.
		return leaderNodeConfig.HttpBinding, nil
	}

	addressToReturn := ""
	if leaderNodeConfig.ClientDomain != "" {
		addressToReturn = net.JoinHostPort(leaderNodeConfig.ClientDomain, portPart)
		fsm.logger.Debug("LeaderHTTPAddress determined address (using ClientDomain)",
			"leader_node_id", leaderNodeID,
			"client_domain", leaderNodeConfig.ClientDomain,
			"port", portPart,
			"returned_address", addressToReturn)
	} else {
		// No ClientDomain, so use the host part from HttpBinding (effectively the original HttpBinding)
		addressToReturn = net.JoinHostPort(hostPartFromBinding, portPart)
		fsm.logger.Debug("LeaderHTTPAddress determined address (using host from HttpBinding)",
			"leader_node_id", leaderNodeID,
			"http_binding_host", hostPartFromBinding,
			"port", portPart,
			"returned_address", addressToReturn)
	}
	return addressToReturn, nil
}
