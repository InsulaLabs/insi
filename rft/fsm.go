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

	"time"

	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insula/tkv"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

// FSMInstance defines the interface for FSM operations.
type FSMInstance interface {
	Apply(l *raft.Log) any
	Snapshot() (raft.FSMSnapshot, error)
	Restore(rc io.ReadCloser) error

	SetValue(kvp KVPayload) error
	GetValue(key string) (string, error)
	DeleteValue(key string) error

	SetTag(kvp KVPayload) error
	GetTag(key string) (string, error)
	DeleteTag(key string) error

	Join(followerId string, followerAddress string) error

	Close() error
}

// Constants for FSM commands (distinct from snapshot db types)
const (
	CmdSetValue   = "set_value"
	CmdSetTag     = "set_tag"
	CmdDeleteTag  = "delete_tag"
	CmdUnsetValue = "unset_value"
)

// kvFsm holds references to both BadgerDB instances.
type kvFsm struct {
	tkv      tkv.TKV
	logger   *slog.Logger
	cfg      *config.Cluster
	thisNode string
	r        *raft.Raft
}

var _ FSMInstance = &kvFsm{}

type Settings struct {
	Ctx     context.Context
	Logger  *slog.Logger
	Config  *config.Cluster
	NodeCfg *config.Node
	NodeId  string
	TkvDb   tkv.TKV
}

func getMapKeys(m map[string]config.Node) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func New(settings Settings) (FSMInstance, error) {
	logger := settings.Logger.WithGroup("fsm_init")

	lockFileName := settings.NodeId + ".lock"
	lockFilePath := filepath.Join(settings.Config.InsudbDir, lockFileName)
	_, errLockFile := os.Stat(lockFilePath)
	isFirstLaunch := os.IsNotExist(errLockFile)

	if isFirstLaunch {
		logger.Info("First time launch: lock file not found", "path", lockFilePath)
	} else if errLockFile != nil {
		return nil, fmt.Errorf("error checking lock file %s: %v", lockFilePath, errLockFile)
	} else {
		logger.Info("Lock file found: not a first-time launch", "path", lockFilePath)
	}

	nodeDataRootPath := filepath.Join(settings.Config.InsudbDir, settings.NodeId)
	if err := os.MkdirAll(nodeDataRootPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("could not create node data root %s: %v", nodeDataRootPath, err)
	}

	kf := &kvFsm{
		logger:   settings.Logger,
		tkv:      settings.TkvDb,
		cfg:      settings.Config,
		thisNode: settings.NodeId,
	}

	currentRaftAdvertiseAddr := net.JoinHostPort(settings.NodeCfg.Host, settings.NodeCfg.RaftPort)
	isDefaultLeader := settings.NodeId == settings.Config.DefaultLeader

	raftInstance, err := setupRaft(
		nodeDataRootPath,
		settings.NodeId,
		currentRaftAdvertiseAddr,
		kf,
		settings.Config,
		isDefaultLeader,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to setup Raft for %s: %v", settings.NodeId, err)
	}
	kf.r = raftInstance

	if isFirstLaunch && !isDefaultLeader {
		logger.Info("First launch, non-leader: attempting auto-join")
		err = attemptAutoJoin(settings.Ctx, settings.NodeId, settings.Config, raftInstance, currentRaftAdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("auto-join failed for %s: %v", settings.NodeId, err)
		}
		logger.Info("Auto-join successful")
	}

	if isFirstLaunch {
		lockFile, errCreate := os.Create(lockFilePath)
		if errCreate != nil {
			return nil, fmt.Errorf("CRITICAL: failed to create lock file %s: %v", lockFilePath, errCreate)
		}
		lockFile.Close()
		logger.Info("Created lock file", "path", lockFilePath)
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

type KVPayload struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type TagPayload struct {
	Key string `json:"key"`
	Tag string `json:"tag"`
}

type KeyPayload struct {
	Key string `json:"key"`
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
		case CmdSetValue:
			var p KVPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal set_value payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal set_value payload: %w", err)
			}
			err := kf.tkv.Set(p.Key, p.Value)
			if err != nil {
				kf.logger.Error("TKV Set failed for valuesDb", "key", p.Key, "error", err)
				return fmt.Errorf("tkv Set failed for valuesDb (key %s): %w", p.Key, err)
			}
			kf.logger.Info("FSM applied set_value", "key", p.Key)
			return nil
		case CmdSetTag:
			var p KVPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal set_tag payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal set_tag payload: %w", err)
			}
			err := kf.tkv.Tag(p.Key, p.Value)
			if err != nil {
				kf.logger.Error("TKV Tag failed for tagsDb", "key", p.Key, "error", err)
				return fmt.Errorf("tkv Tag failed for tagsDb (key %s): %w", p.Key, err)
			}
			kf.logger.Info("FSM applied set_tag", "key", p.Key)
			return nil
		case CmdDeleteTag:
			var p TagPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal delete_tag payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal delete_tag payload: %w", err)
			}
			err := kf.tkv.Untag(p.Key, p.Tag)
			if err != nil {
				kf.logger.Error("TKV Untag failed for tagsDb", "key", p.Key, "error", err)
				return fmt.Errorf("tkv Untag failed for tagsDb (key %s): %w", p.Key, err)
			}
			kf.logger.Info("FSM applied delete_tag", "key", p.Key)
			return nil
		case CmdUnsetValue:
			var p KeyPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal unset_value payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal unset_value payload: %w", err)
			}
			err := kf.tkv.Delete(p.Key)
			if err != nil {
				kf.logger.Error("TKV Delete failed for valuesDb", "key", p.Key, "error", err)
				return fmt.Errorf("tkv Delete failed for valuesDb (key %s): %w", p.Key, err)
			}
			kf.logger.Info("FSM applied unset_value", "key", p.Key)
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
		tagsDb:   kf.tkv.GetTagDB(),
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
	tagsBatch := kf.tkv.GetTagDB().NewWriteBatch()
	defer tagsBatch.Cancel()

	valuesCount := 0
	tagsCount := 0
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
		case dbTypeTags: // Defined in snapshot.go
			if err := tagsBatch.Set([]byte(entry.Key), []byte(entry.Value)); err != nil {
				kf.logger.Error("Could not set value from snapshot in tagsDb batch", "key", entry.Key, "error", err)
				return fmt.Errorf("could not set value from snapshot in tagsDb batch (key: %s): %w", entry.Key, err)
			}
			tagsCount++
		default:
			kf.logger.Warn("Unknown DBType in snapshot entry during restore, skipping", "db_type", entry.DBType, "key", entry.Key)
		}
	}

	if err := valuesBatch.Flush(); err != nil {
		kf.logger.Error("Failed to flush valuesDb batch during restore", "error", err)
		return fmt.Errorf("failed to flush valuesDb batch during restore: %w", err)
	}
	if err := tagsBatch.Flush(); err != nil {
		kf.logger.Error("Failed to flush tagsDb batch during restore", "error", err)
		return fmt.Errorf("failed to flush tagsDb batch during restore: %w", err)
	}
	kf.logger.Info("FSM restored successfully from snapshot", "values_restored", valuesCount, "tags_restored", tagsCount)
	return nil
}

func (kf *kvFsm) SetValue(kvp KVPayload) error {
	kf.logger.Debug("SetValue called", "key", kvp.Key)
	cmd := RaftCommand{
		Type: CmdSetValue,
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
	kf.logger.Info("SetValue Raft Apply successful", "key", kvp.Key)
	return nil
}

func (kf *kvFsm) GetValue(key string) (string, error) {
	kf.logger.Debug("GetValue called", "key", key)
	var value string
	err := kf.tkv.GetDataDB().View(func(txn *badger.Txn) error {
		item, errGet := txn.Get([]byte(key))
		if errGet != nil {
			return errGet
		}
		return item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
	})
	if err != nil {
		if err != badger.ErrKeyNotFound {
			kf.logger.Error("Failed to get value from valuesDb", "key", key, "error", err)
		}
		return "", err
	}
	kf.logger.Debug("GetValue successful", "key", key)
	return value, nil
}

func (kf *kvFsm) DeleteValue(key string) error {
	kf.logger.Debug("DeleteValue called", "key", key)
	payload := KeyPayload{Key: key}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for unset_value", "key", key, "error", err)
		return fmt.Errorf("could not marshal payload for unset_value: %w", err)
	}

	cmd := RaftCommand{
		Type:    CmdUnsetValue,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for unset_value", "key", key, "error", err)
		return fmt.Errorf("could not marshal raft command for unset_value: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for DeleteValue", "key", key, "error", err)
		return fmt.Errorf("raft Apply for DeleteValue failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for DeleteValue", "key", key, "error", responseErr)
		return fmt.Errorf("fsm application error for DeleteValue (key %s): %w", key, responseErr)
	}
	kf.logger.Info("DeleteValue Raft Apply successful", "key", key)
	return nil
}

func (kf *kvFsm) SetTag(kvp KVPayload) error {
	kf.logger.Debug("SetTag called", "key", kvp.Key)
	payloadBytes, err := json.Marshal(kvp)
	if err != nil {
		kf.logger.Error("Could not marshal payload for set_tag", "key", kvp.Key, "error", err)
		return fmt.Errorf("could not marshal payload for set_tag: %w", err)
	}
	cmd := RaftCommand{
		Type:    CmdSetTag,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for set_tag", "key", kvp.Key, "error", err)
		return fmt.Errorf("could not marshal raft command for set_tag: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for SetTag", "key", kvp.Key, "error", err)
		return fmt.Errorf("raft Apply for SetTag failed (key %s): %w", kvp.Key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for SetTag", "key", kvp.Key, "error", responseErr)
		return fmt.Errorf("fsm application error for SetTag (key %s): %w", kvp.Key, responseErr)
	}
	kf.logger.Info("SetTag Raft Apply successful", "key", kvp.Key)
	return nil
}

func (kf *kvFsm) GetTag(key string) (string, error) {
	kf.logger.Debug("GetTag called", "key", key)
	var value string
	err := kf.tkv.GetTagDB().View(func(txn *badger.Txn) error {
		item, errGet := txn.Get([]byte(key))
		if errGet != nil {
			return errGet
		}
		return item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
	})
	if err != nil {
		if err != badger.ErrKeyNotFound {
			kf.logger.Error("Failed to get value from tagsDb", "key", key, "error", err)
		}
		return "", err
	}
	kf.logger.Debug("GetTag successful", "key", key)
	return value, nil
}

func (kf *kvFsm) DeleteTag(key string) error {
	kf.logger.Debug("DeleteTag called", "key", key)
	payload := KeyPayload{Key: key}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for delete_tag", "key", key, "error", err)
		return fmt.Errorf("could not marshal payload for delete_tag: %w", err)
	}

	cmd := RaftCommand{
		Type:    CmdDeleteTag,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for delete_tag", "key", key, "error", err)
		return fmt.Errorf("could not marshal raft command for delete_tag: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for DeleteTag", "key", key, "error", err)
		return fmt.Errorf("raft Apply for DeleteTag failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for DeleteTag", "key", key, "error", responseErr)
		return fmt.Errorf("fsm application error for DeleteTag (key %s): %w", key, responseErr)
	}
	kf.logger.Info("DeleteTag Raft Apply successful", "key", key)
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
	kf.logger.Info("Successfully added follower/voter to Raft cluster", "follower_id", followerId, "follower_addr", followerAddress)
	return nil
}

type BadgerLogger struct {
	slogger *slog.Logger
}

func NewBadgerLogger(logger *slog.Logger) *BadgerLogger {
	return &BadgerLogger{slogger: logger}
}

func (bl *BadgerLogger) Errorf(format string, args ...any) {
	bl.slogger.Error(fmt.Sprintf(format, args...))
}

func (bl *BadgerLogger) Warningf(format string, args ...any) {
	bl.slogger.Warn(fmt.Sprintf(format, args...))
}

func (bl *BadgerLogger) Infof(format string, args ...any) {
	bl.slogger.Info(fmt.Sprintf(format, args...))
}

func (bl *BadgerLogger) Debugf(format string, args ...any) {
	bl.slogger.Debug(fmt.Sprintf(format, args...))
}
