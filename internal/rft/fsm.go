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
	"github.com/InsulaLabs/insi/internal/tkv"
	"github.com/dgraph-io/badger/v3"

	"github.com/hashicorp/raft"

	"github.com/InsulaLabs/insi/models"
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
	SetCache(kvp models.CachePayload) error
	GetCache(key string) (string, error)
	DeleteCache(key string) error
}

type EventIF interface {
	Publish(topic string, data any) error
}

type AtomicIF interface {
	AtomicNew(key string, overwrite bool) error
	AtomicAdd(key string, delta int64) (int64, error) // Returns new value
	AtomicDelete(key string) error
	AtomicGet(key string) (int64, error)
}

type QueueIF interface {
	QueueNew(key string) error
	QueuePush(key string, value string) (int, error) // Returns new length
	QueuePop(key string) (string, error)             // Returns popped value
	QueueDelete(key string) error
}

// FSMInstance defines the interface for FSM operations.
type FSMInstance interface {
	RaftIF
	ValueStoreIF
	CacheStoreIF
	EventIF
	AtomicIF
	QueueIF
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

	// Atomic operation commands
	cmdAtomicNew    = "atomic_new"
	cmdAtomicAdd    = "atomic_add"
	cmdAtomicDelete = "atomic_delete"

	// Queue operation commands
	cmdQueueNew    = "queue_new"
	cmdQueuePush   = "queue_push"
	cmdQueuePop    = "queue_pop"
	cmdQueueDelete = "queue_delete"
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
		logger:     settings.Logger,
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
		err = attemptAutoJoin(settings.Ctx, settings.NodeId, settings.Config, raftInstance, currentRaftAdvertiseAddr)
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
		logger.Info("Lock file created", "path", lockFilePath)
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
			kf.logger.Info("FSM applied set_value", "key", p.Key)
			return nil
		case cmdDeleteValue:
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
			kf.logger.Info("FSM applied delete_value", "key", p.Key)
			return nil
		case cmdSetCache:
			var p models.CachePayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal set_std_cache payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal set_std_cache payload: %w", err)
			}

			kf.logger.Debug("Applying CmdSetCache",
				"key", p.Key,
				"value_len", len(p.Value),
				"ttl_seconds", p.TTL, // Assuming p.TTL is int64 seconds
				"set_at_unmarshaled", p.SetAt.Format(time.RFC3339Nano))

			var err error
			if p.SetAt.IsZero() {
				kf.logger.Warn("CachePayload.SetAt is zero after unmarshaling. Applying with full original TTL from now.",
					"key", p.Key,
					"original_ttl_seconds", p.TTL)

				// Convert p.TTL (seconds) to time.Duration for CacheSet
				actualTTLForCacheSet := time.Duration(p.TTL) * time.Second
				err = kf.tkv.CacheSet(p.Key, p.Value, actualTTLForCacheSet)
				if err != nil {
					kf.logger.Error("TKV CacheSet failed when SetAt was zero", "key", p.Key, "error", err)
					return fmt.Errorf("tkv CacheSet failed for stdCache (key %s, SetAt was zero): %w", p.Key, err)
				}
				kf.logger.Info("FSM applied set_std_cache (SetAt was zero, used full original TTL)", "key", p.Key, "applied_ttl", actualTTLForCacheSet.String())
			} else {
				// SetAt is valid, proceed with precise expiry calculation.
				intendedDuration := time.Duration(p.TTL) * time.Second // Correctly convert int64 seconds to time.Duration
				expiryTime := p.SetAt.Add(intendedDuration)
				currentTime := time.Now()

				if expiryTime.Before(currentTime) {
					kf.logger.Info("Skipping std cache entry because its calculated absolute expiry time is in the past",
						"key", p.Key,
						"original_ttl_seconds", p.TTL, // p.TTL is int64 seconds
						"intended_duration_str", intendedDuration.String(),
						"set_at", p.SetAt.Format(time.RFC3339Nano),
						"calculated_expiry_time", expiryTime.Format(time.RFC3339Nano),
						"current_time", currentTime.Format(time.RFC3339Nano))
					return nil
				}

				remainingTTL := time.Until(expiryTime)
				if remainingTTL <= 0 {
					kf.logger.Info("Skipping std cache entry as calculated remaining TTL is zero or negative",
						"key", p.Key,
						"original_ttl_seconds", p.TTL, // p.TTL is int64 seconds
						"intended_duration_str", intendedDuration.String(),
						"set_at", p.SetAt.Format(time.RFC3339Nano),
						"calculated_expiry_time", expiryTime.Format(time.RFC3339Nano),
						"current_time", currentTime.Format(time.RFC3339Nano),
						"calculated_remaining_ttl", remainingTTL.String())
					return nil
				}

				err = kf.tkv.CacheSet(p.Key, p.Value, remainingTTL) // Use calculated remainingTTL (which is a time.Duration)
				if err != nil {
					kf.logger.Error("TKV CacheSet failed", "key", p.Key, "remaining_ttl", remainingTTL.String(), "error", err)
					return fmt.Errorf("tkv CacheSet failed for stdCache (key %s): %w", p.Key, err)
				}
				kf.logger.Info("FSM applied set_std_cache with remaining TTL", "key", p.Key, "remaining_ttl", remainingTTL.String())
			}
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
			kf.logger.Info("FSM applied delete_cache", "key", p.Key)
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
			kf.logger.Info("FSM applied batch_set_values", "item_count", len(batchItems))
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
			kf.logger.Info("FSM applied batch_delete_values", "key_count", len(keys))
			return nil
		case cmdAtomicNew:
			var p models.AtomicNewRequest
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal atomic_new payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal atomic_new payload: %w", err)
			}
			err := kf.tkv.AtomicNew(p.Key, p.Overwrite)
			if err != nil {
				// Handle specific tkv errors like ErrKeyExists if needed for logging or metrics
				kf.logger.Error("TKV AtomicNew failed", "key", p.Key, "overwrite", p.Overwrite, "error", err)
				return fmt.Errorf("tkv AtomicNew failed (key %s): %w", p.Key, err)
			}
			kf.logger.Info("FSM applied atomic_new", "key", p.Key, "overwrite", p.Overwrite)
			return nil // Return the error itself from tkv.AtomicNew if it occurred
		case cmdAtomicAdd:
			var p models.AtomicAddRequest
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal atomic_add payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal atomic_add payload: %w", err)
			}
			newValue, err := kf.tkv.AtomicAdd(p.Key, p.Delta)
			if err != nil {
				// Handle specific tkv errors like ErrInvalidState
				kf.logger.Error("TKV AtomicAdd failed", "key", p.Key, "delta", p.Delta, "error", err)
				// Return the error to Apply. The caller of Raft Apply will see this error.
				// We also need to return *something* for newValue in the error case for the Apply signature.
				// Returning the error itself is idiomatic for Apply's response when an error occurs.
				return err
			}
			kf.logger.Info("FSM applied atomic_add", "key", p.Key, "delta", p.Delta, "new_value", newValue)
			return newValue // Return the new value on success
		case cmdAtomicDelete:
			var p models.AtomicKeyPayload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal atomic_delete payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal atomic_delete payload: %w", err)
			}
			err := kf.tkv.AtomicDelete(p.Key)
			if err != nil {
				kf.logger.Error("TKV AtomicDelete failed", "key", p.Key, "error", err)
				return fmt.Errorf("tkv AtomicDelete failed (key %s): %w", p.Key, err)
			}
			kf.logger.Info("FSM applied atomic_delete", "key", p.Key)
			return nil
		case cmdQueueNew:
			var p models.QueueNewRequest // Using models.QueueNewRequest
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal queue_new payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal queue_new payload: %w", err)
			}
			err := kf.tkv.QueueNew(p.Key)
			if err != nil {
				kf.logger.Error("TKV QueueNew failed", "key", p.Key, "error", err)
				return fmt.Errorf("tkv QueueNew failed (key %s): %w", p.Key, err)
			}
			kf.logger.Info("FSM applied queue_new", "key", p.Key)
			return nil
		case cmdQueuePush:
			var p models.QueuePushRequest
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal queue_push payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal queue_push payload: %w", err)
			}
			length, err := kf.tkv.QueuePush(p.Key, p.Value)
			if err != nil {
				kf.logger.Error("TKV QueuePush failed", "key", p.Key, "value", p.Value, "error", err)
				return err // Return the error itself (e.g., ErrQueueNotFound)
			}
			kf.logger.Info("FSM applied queue_push", "key", p.Key, "value", p.Value, "new_length", length)
			return length // Return the new length on success
		case cmdQueuePop:
			var p models.QueueKeyPayload // Pop only needs the key in payload
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal queue_pop payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal queue_pop payload: %w", err)
			}
			value, err := kf.tkv.QueuePop(p.Key)
			if err != nil {
				kf.logger.Error("TKV QueuePop failed", "key", p.Key, "error", err)
				return err // Return the error itself (e.g., ErrQueueNotFound, ErrQueueEmpty)
			}
			kf.logger.Info("FSM applied queue_pop", "key", p.Key, "popped_value_len", len(value))
			return value // Return the popped value on success
		case cmdQueueDelete:
			var p models.QueueDeleteRequest // Using models.QueueDeleteRequest
			if err := json.Unmarshal(cmd.Payload, &p); err != nil {
				kf.logger.Error("Could not unmarshal queue_delete payload", "error", err, "payload", string(cmd.Payload))
				return fmt.Errorf("could not unmarshal queue_delete payload: %w", err)
			}
			err := kf.tkv.QueueDelete(p.Key)
			if err != nil {
				kf.logger.Error("TKV QueueDelete failed", "key", p.Key, "error", err)
				return fmt.Errorf("tkv QueueDelete failed (key %s): %w", p.Key, err)
			}
			kf.logger.Info("FSM applied queue_delete", "key", p.Key)
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
			// Get the time of encoding, add the TTL. If we are past that time dont add
			timeOfEncoding := time.Unix(entry.TimeEncoded, 0)
			timeOfEncodingPlusTTL := timeOfEncoding.Add(time.Duration(entry.TTL) * time.Second)
			if time.Now().After(timeOfEncodingPlusTTL) {
				kf.logger.Warn("Skipping std cache entry because it is past its TTL", "key", entry.Key, "ttl", entry.TTL)
				continue
			}
			remainingTTL := time.Until(timeOfEncodingPlusTTL)
			if remainingTTL <= 0 {
				kf.logger.Info("Skipping restore of cache entry as its remaining TTL is zero or negative", "key", entry.Key, "original_ttl_seconds", entry.TTL, "calculated_remaining_ttl", remainingTTL.String())
				continue
			}
			stdCache.Set(entry.Key, entry.Value, remainingTTL)
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
	kf.logger.Info("Delete Raft Apply successful", "key", key)
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
	kf.logger.Info("Publish Raft Apply successful", "topic", topic)
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

func (kf *kvFsm) SetCache(payload models.CachePayload) error {
	kf.logger.Debug("SetCache called", "key", payload.Key)

	// Set SetAt to the current time just before proposing to Raft
	payload.SetAt = time.Now()

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
	kf.logger.Info("SetCache Raft Apply successful", "key", payload.Key)
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
	kf.logger.Info("DeleteCache Raft Apply successful", "key", key)
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
	kf.logger.Info("FSM BatchSet Raft Apply successful", "item_count", len(items))
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
	kf.logger.Info("FSM BatchDelete Raft Apply successful", "key_count", len(keys))
	return nil
}

// ------------

func (kf *kvFsm) UpdateUsage(key string, bytesAdded int) error {

	// TODO: WHOLE KEY given (the exact lookup) no prefixing here

	// TODO: Load and +/- the stored usage -bytesAdded means some
	// were removed.

	// TODO: ensure bytes total not negative

	// TODO: return ErrInsufficientSpace if bytesAdded exceeds
	// the total bytes available and dont save the new usage
	// (this indicates the write should fail)

	// todo: read from the ROOT_PREFIX:usage:ENTITY_NAME:KEY
	// to get usage

	// TODO: write to the ROOT_PREFIX:usage:ENTITY_NAME:KEY:tracker
	// to store the new usage

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

// [atomics]

func (kf *kvFsm) AtomicNew(key string, overwrite bool) error {
	kf.logger.Debug("AtomicNewRaft called", "key", key, "overwrite", overwrite)
	payload := models.AtomicNewRequest{Key: key, Overwrite: overwrite}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for atomic_new", "key", key, "error", err)
		return fmt.Errorf("could not marshal payload for atomic_new: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdAtomicNew,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for atomic_new", "key", key, "error", err)
		return fmt.Errorf("could not marshal raft command for atomic_new: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for AtomicNewRaft", "key", key, "error", err)
		return fmt.Errorf("raft Apply for AtomicNewRaft failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for AtomicNewRaft", "key", key, "error", responseErr)
		// This error comes from the Apply method (e.g., tkv.AtomicNew returning ErrKeyExists)
		return responseErr
	}
	kf.logger.Info("AtomicNewRaft Raft Apply successful", "key", key)
	return nil
}

func (kf *kvFsm) AtomicAdd(key string, delta int64) (int64, error) {
	kf.logger.Debug("AtomicAddRaft called", "key", key, "delta", delta)
	payload := models.AtomicAddRequest{Key: key, Delta: delta}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for atomic_add", "key", key, "error", err)
		return 0, fmt.Errorf("could not marshal payload for atomic_add: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdAtomicAdd,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for atomic_add", "key", key, "error", err)
		return 0, fmt.Errorf("could not marshal raft command for atomic_add: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for AtomicAddRaft", "key", key, "error", err)
		return 0, fmt.Errorf("raft Apply for AtomicAddRaft failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for AtomicAddRaft", "key", key, "error", responseErr)
		return 0, responseErr // Error from Apply (e.g., tkv.AtomicAdd returning ErrInvalidState)
	}

	newValue, ok := response.(int64)
	if !ok {
		// This case should ideally not happen if Apply for cmdAtomicAdd always returns int64 or error
		kf.logger.Error("FSM response for AtomicAddRaft was not an int64", "key", key, "response_type", fmt.Sprintf("%T", response))
		return 0, fmt.Errorf("unexpected response type from FSM for AtomicAddRaft: got %T, expected int64 or error", response)
	}

	kf.logger.Info("AtomicAddRaft Raft Apply successful", "key", key, "new_value", newValue)
	return newValue, nil
}

func (kf *kvFsm) AtomicDelete(key string) error {
	kf.logger.Debug("AtomicDeleteRaft called", "key", key)
	payload := models.AtomicKeyPayload{Key: key}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for atomic_delete", "key", key, "error", err)
		return fmt.Errorf("could not marshal payload for atomic_delete: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdAtomicDelete,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for atomic_delete", "key", key, "error", err)
		return fmt.Errorf("could not marshal raft command for atomic_delete: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for AtomicDeleteRaft", "key", key, "error", err)
		return fmt.Errorf("raft Apply for AtomicDeleteRaft failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for AtomicDeleteRaft", "key", key, "error", responseErr)
		return responseErr // Error from Apply
	}
	kf.logger.Info("AtomicDeleteRaft Raft Apply successful", "key", key)
	return nil
}

// AtomicGet is a direct read from TKV, does not go through Raft Apply
func (kf *kvFsm) AtomicGet(key string) (int64, error) {
	kf.logger.Debug("FSM AtomicGet (direct read) called", "key", key)
	return kf.tkv.AtomicGet(key)
}

// ------------

// [queues]

func (kf *kvFsm) QueueNew(key string) error {
	kf.logger.Debug("QueueNewRaft called", "key", key)
	payload := models.QueueNewRequest{Key: key}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for queue_new", "key", key, "error", err)
		return fmt.Errorf("could not marshal payload for queue_new: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdQueueNew,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for queue_new", "key", key, "error", err)
		return fmt.Errorf("could not marshal raft command for queue_new: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for QueueNewRaft", "key", key, "error", err)
		return fmt.Errorf("raft Apply for QueueNewRaft failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for QueueNewRaft", "key", key, "error", responseErr)
		return responseErr
	}
	kf.logger.Info("QueueNewRaft Raft Apply successful", "key", key)
	return nil
}

func (kf *kvFsm) QueuePush(key string, value string) (int, error) {
	kf.logger.Debug("QueuePushRaft called", "key", key, "value_len", len(value))
	payload := models.QueuePushRequest{Key: key, Value: value}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for queue_push", "key", key, "error", err)
		return 0, fmt.Errorf("could not marshal payload for queue_push: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdQueuePush,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for queue_push", "key", key, "error", err)
		return 0, fmt.Errorf("could not marshal raft command for queue_push: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for QueuePushRaft", "key", key, "error", err)
		return 0, fmt.Errorf("raft Apply for QueuePushRaft failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for QueuePushRaft", "key", key, "error", responseErr)
		return 0, responseErr // Error from Apply (e.g., tkv.ErrQueueNotFound)
	}

	newLength, ok := response.(int)
	if !ok {
		kf.logger.Error("FSM response for QueuePushRaft was not an int", "key", key, "response_type", fmt.Sprintf("%T", response))
		return 0, fmt.Errorf("unexpected response type from FSM for QueuePushRaft: got %T, expected int or error", response)
	}

	kf.logger.Info("QueuePushRaft Raft Apply successful", "key", key, "new_length", newLength)
	return newLength, nil
}

func (kf *kvFsm) QueuePop(key string) (string, error) {
	kf.logger.Debug("QueuePopRaft called", "key", key)
	payload := models.QueueKeyPayload{Key: key} // Pop only needs key
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for queue_pop", "key", key, "error", err)
		return "", fmt.Errorf("could not marshal payload for queue_pop: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdQueuePop,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for queue_pop", "key", key, "error", err)
		return "", fmt.Errorf("could not marshal raft command for queue_pop: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for QueuePopRaft", "key", key, "error", err)
		return "", fmt.Errorf("raft Apply for QueuePopRaft failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for QueuePopRaft", "key", key, "error", responseErr)
		return "", responseErr // Error from Apply (e.g., tkv.ErrQueueNotFound, tkv.ErrQueueEmpty)
	}

	poppedValue, ok := response.(string)
	if !ok {
		kf.logger.Error("FSM response for QueuePopRaft was not a string", "key", key, "response_type", fmt.Sprintf("%T", response))
		return "", fmt.Errorf("unexpected response type from FSM for QueuePopRaft: got %T, expected string or error", response)
	}

	kf.logger.Info("QueuePopRaft Raft Apply successful", "key", key, "popped_value_len", len(poppedValue))
	return poppedValue, nil
}

func (kf *kvFsm) QueueDelete(key string) error {
	kf.logger.Debug("QueueDeleteRaft called", "key", key)
	payload := models.QueueDeleteRequest{Key: key}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		kf.logger.Error("Could not marshal payload for queue_delete", "key", key, "error", err)
		return fmt.Errorf("could not marshal payload for queue_delete: %w", err)
	}

	cmd := RaftCommand{
		Type:    cmdQueueDelete,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		kf.logger.Error("Could not marshal raft command for queue_delete", "key", key, "error", err)
		return fmt.Errorf("could not marshal raft command for queue_delete: %w", err)
	}

	future := kf.r.Apply(cmdBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		kf.logger.Error("Raft Apply failed for QueueDeleteRaft", "key", key, "error", err)
		return fmt.Errorf("raft Apply for QueueDeleteRaft failed (key %s): %w", key, err)
	}

	response := future.Response()
	if responseErr, ok := response.(error); ok && responseErr != nil {
		kf.logger.Error("FSM application error for QueueDeleteRaft", "key", key, "error", responseErr)
		return responseErr
	}
	kf.logger.Info("QueueDeleteRaft Raft Apply successful", "key", key)
	return nil
}

// ------------
