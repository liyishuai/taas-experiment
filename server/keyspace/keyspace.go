// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package keyspace

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/id"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"go.uber.org/zap"
)

const (
	// AllocStep set idAllocator's step when write persistent window boundary.
	// Use a lower value for denser idAllocation in the event of frequent pd leader change.
	AllocStep = uint64(100)
	// AllocLabel is used to label keyspace idAllocator's metrics.
	AllocLabel = "keyspace-idAlloc"
	// DefaultKeyspaceName is the name reserved for default keyspace.
	DefaultKeyspaceName = "DEFAULT"
	// DefaultKeyspaceID is the id of default keyspace.
	DefaultKeyspaceID = uint32(0)
	// regionLabelIDPrefix is used to prefix the keyspace region label.
	regionLabelIDPrefix = "keyspaces/"
	// regionLabelKey is the key for keyspace id in keyspace region label.
	regionLabelKey = "id"
)

// Manager manages keyspace related data.
// It validates requests and provides concurrency control.
type Manager struct {
	// metaLock guards keyspace meta.
	metaLock *syncutil.LockGroup
	// idAllocator allocates keyspace id.
	idAllocator id.Allocator
	// store is the storage for keyspace related information.
	store endpoint.KeyspaceStorage
	// rc is the raft cluster of the server.
	rc *cluster.RaftCluster
	// ctx is the context of the manager, to be used in transaction.
	ctx context.Context
	// config is the configurations of the manager.
	config config.KeyspaceConfig
}

// CreateKeyspaceRequest represents necessary arguments to create a keyspace.
type CreateKeyspaceRequest struct {
	// Name of the keyspace to be created.
	// Using an existing name will result in error.
	Name   string
	Config map[string]string
	// Now is the timestamp used to record creation time.
	Now int64
}

// NewKeyspaceManager creates a Manager of keyspace related data.
func NewKeyspaceManager(store endpoint.KeyspaceStorage,
	rc *cluster.RaftCluster,
	idAllocator id.Allocator,
	config config.KeyspaceConfig,
) *Manager {
	return &Manager{
		metaLock:    syncutil.NewLockGroup(syncutil.WithHash(keyspaceIDHash)),
		idAllocator: idAllocator,
		store:       store,
		rc:          rc,
		ctx:         context.TODO(),
		config:      config,
	}
}

// Bootstrap saves default keyspace info.
func (manager *Manager) Bootstrap() error {
	// Split Keyspace Region for default keyspace.
	if err := manager.splitKeyspaceRegion(DefaultKeyspaceID); err != nil {
		return err
	}
	now := time.Now().Unix()
	defaultKeyspace := &keyspacepb.KeyspaceMeta{
		Id:             DefaultKeyspaceID,
		Name:           DefaultKeyspaceName,
		State:          keyspacepb.KeyspaceState_ENABLED,
		CreatedAt:      now,
		StateChangedAt: now,
	}
	err := manager.saveNewKeyspace(defaultKeyspace)
	// It's possible that default keyspace already exists in the storage (e.g. PD restart/recover),
	// so we ignore the keyspaceExists error.
	if err != nil && err != ErrKeyspaceExists {
		return err
	}

	// Initialize pre-alloc keyspace.
	preAlloc := manager.config.PreAlloc
	for _, keyspaceName := range preAlloc {
		_, err = manager.CreateKeyspace(&CreateKeyspaceRequest{
			Name: keyspaceName,
			Now:  now,
		})
		// Ignore the keyspaceExists error for the same reason as saving default keyspace.
		if err != nil && err != ErrKeyspaceExists {
			return err
		}
	}
	return nil
}

// CreateKeyspace create a keyspace meta with given config and save it to storage.
func (manager *Manager) CreateKeyspace(request *CreateKeyspaceRequest) (*keyspacepb.KeyspaceMeta, error) {
	// Validate purposed name's legality.
	if err := validateName(request.Name); err != nil {
		return nil, err
	}
	// Allocate new keyspaceID.
	newID, err := manager.allocID()
	if err != nil {
		return nil, err
	}
	// Split keyspace region.
	err = manager.splitKeyspaceRegion(newID)
	if err != nil {
		return nil, err
	}
	// Create and save keyspace metadata.
	keyspace := &keyspacepb.KeyspaceMeta{
		Id:             newID,
		Name:           request.Name,
		State:          keyspacepb.KeyspaceState_ENABLED,
		CreatedAt:      request.Now,
		StateChangedAt: request.Now,
		Config:         request.Config,
	}
	err = manager.saveNewKeyspace(keyspace)
	if err != nil {
		log.Warn("[keyspace] failed to create keyspace",
			zap.Uint32("ID", keyspace.GetId()),
			zap.String("name", keyspace.GetName()),
			zap.Error(err),
		)
		return nil, err
	}
	log.Info("[keyspace] keyspace created",
		zap.Uint32("ID", keyspace.GetId()),
		zap.String("name", keyspace.GetName()),
	)
	return keyspace, nil
}

func (manager *Manager) saveNewKeyspace(keyspace *keyspacepb.KeyspaceMeta) error {
	manager.metaLock.Lock(keyspace.Id)
	defer manager.metaLock.Unlock(keyspace.Id)

	return manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		// Save keyspace ID.
		// Check if keyspace with that name already exists.
		nameExists, _, err := manager.store.LoadKeyspaceID(txn, keyspace.Name)
		if err != nil {
			return err
		}
		if nameExists {
			return ErrKeyspaceExists
		}
		err = manager.store.SaveKeyspaceID(txn, keyspace.Id, keyspace.Name)
		if err != nil {
			return err
		}
		// Save keyspace meta.
		// Check if keyspace with that id already exists.
		loadedMeta, err := manager.store.LoadKeyspaceMeta(txn, keyspace.Id)
		if err != nil {
			return err
		}
		if loadedMeta != nil {
			return ErrKeyspaceExists
		}
		return manager.store.SaveKeyspaceMeta(txn, keyspace)
	})
}

// splitKeyspaceRegion add keyspace's boundaries to region label. The corresponding
// region will then be split by Coordinator's patrolRegion.
func (manager *Manager) splitKeyspaceRegion(id uint32) error {
	failpoint.Inject("skipSplitRegion", func() {
		failpoint.Return(nil)
	})

	keyspaceRule := makeLabelRule(id)
	err := manager.rc.GetRegionLabeler().SetLabelRule(keyspaceRule)
	if err != nil {
		log.Warn("[keyspace] failed to add region label for keyspace",
			zap.Uint32("keyspaceID", id),
			zap.Error(err),
		)
	}
	log.Info("[keyspace] added region label for keyspace",
		zap.Uint32("keyspaceID", id),
		zap.Any("LabelRule", keyspaceRule),
	)
	return nil
}

// LoadKeyspace returns the keyspace specified by name.
// It returns error if loading or unmarshalling met error or if keyspace does not exist.
func (manager *Manager) LoadKeyspace(name string) (*keyspacepb.KeyspaceMeta, error) {
	var meta *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		loaded, id, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if !loaded {
			return ErrKeyspaceNotFound
		}
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil {
			return ErrKeyspaceNotFound
		}
		meta.Id = id
		return nil
	})
	return meta, err
}

// LoadKeyspaceByID returns the keyspace specified by id.
// It returns error if loading or unmarshalling met error or if keyspace does not exist.
func (manager *Manager) LoadKeyspaceByID(spaceID uint32) (*keyspacepb.KeyspaceMeta, error) {
	var (
		meta *keyspacepb.KeyspaceMeta
		err  error
	)
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		meta, err = manager.store.LoadKeyspaceMeta(txn, spaceID)
		if err != nil {
			return err
		}
		if meta == nil {
			return ErrKeyspaceNotFound
		}
		return nil
	})
	meta.Id = spaceID
	return meta, err
}

// Mutation represents a single operation to be applied on keyspace config.
type Mutation struct {
	Op    OpType
	Key   string
	Value string
}

// OpType defines the type of keyspace config operation.
type OpType int

const (
	// OpPut denotes a put operation onto the given config.
	// If target key exists, it will put a new value,
	// otherwise, it creates a new config entry.
	OpPut OpType = iota + 1 // Operation type starts at 1.
	// OpDel denotes a deletion operation onto the given config.
	// Note: OpDel is idempotent, deleting a non-existing key
	// will not result in error.
	OpDel
)

// UpdateKeyspaceConfig changes target keyspace's config in the order specified in mutations.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceConfig(name string, mutations []*Mutation) (*keyspacepb.KeyspaceMeta, error) {
	var meta *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		// First get KeyspaceID from Name.
		loaded, id, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if !loaded {
			return ErrKeyspaceNotFound
		}
		manager.metaLock.Lock(id)
		defer manager.metaLock.Unlock(id)
		// Load keyspace by id.
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil {
			return ErrKeyspaceNotFound
		}
		// Only keyspace with state listed in allowChangeConfig are allowed to change their config.
		if !slice.Contains(allowChangeConfig, meta.GetState()) {
			return errors.Errorf("cannot change config for keyspace with state %s", meta.GetState().String())
		}
		// Initialize meta's config map if it's nil.
		if meta.GetConfig() == nil {
			meta.Config = map[string]string{}
		}
		// Update keyspace config according to mutations.
		for _, mutation := range mutations {
			switch mutation.Op {
			case OpPut:
				meta.Config[mutation.Key] = mutation.Value
			case OpDel:
				delete(meta.Config, mutation.Key)
			default:
				return errIllegalOperation
			}
		}
		// Save the updated keyspace meta.
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})

	if err != nil {
		log.Warn("[keyspace] failed to update keyspace config",
			zap.Uint32("ID", meta.GetId()),
			zap.String("name", meta.GetName()),
			zap.Error(err),
		)
		return nil, err
	}
	log.Info("[keyspace] keyspace config updated",
		zap.Uint32("ID", meta.GetId()),
		zap.String("name", meta.GetName()),
		zap.Any("new config", meta.GetConfig()),
	)
	return meta, nil
}

// UpdateKeyspaceState updates target keyspace to the given state if it's not already in that state.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceState(name string, newState keyspacepb.KeyspaceState, now int64) (*keyspacepb.KeyspaceMeta, error) {
	// Changing the state of default keyspace is not allowed.
	if name == DefaultKeyspaceName {
		log.Warn("[keyspace] failed to update keyspace config",
			zap.Error(errModifyDefault),
		)
		return nil, errModifyDefault
	}
	var meta *keyspacepb.KeyspaceMeta
	err := manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		// First get KeyspaceID from Name.
		loaded, id, err := manager.store.LoadKeyspaceID(txn, name)
		if err != nil {
			return err
		}
		if !loaded {
			return ErrKeyspaceNotFound
		}
		manager.metaLock.Lock(id)
		defer manager.metaLock.Unlock(id)
		// Load keyspace by id.
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil {
			return ErrKeyspaceNotFound
		}
		// Update keyspace meta.
		if err = updateKeyspaceState(meta, newState, now); err != nil {
			return err
		}
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err != nil {
		log.Warn("[keyspace] failed to update keyspace config",
			zap.Uint32("ID", meta.GetId()),
			zap.String("name", meta.GetName()),
			zap.Error(err),
		)
		return nil, err
	}
	log.Info("[keyspace] keyspace state updated",
		zap.Uint32("ID", meta.GetId()),
		zap.String("name", meta.GetName()),
		zap.String("new state", newState.String()),
	)
	return meta, nil
}

// UpdateKeyspaceStateByID updates target keyspace to the given state if it's not already in that state.
// It returns error if saving failed, operation not allowed, or if keyspace not exists.
func (manager *Manager) UpdateKeyspaceStateByID(id uint32, newState keyspacepb.KeyspaceState, now int64) (*keyspacepb.KeyspaceMeta, error) {
	// Changing the state of default keyspace is not allowed.
	if id == DefaultKeyspaceID {
		log.Warn("[keyspace] failed to update keyspace config",
			zap.Error(errModifyDefault),
		)
		return nil, errModifyDefault
	}
	var meta *keyspacepb.KeyspaceMeta
	var err error
	err = manager.store.RunInTxn(manager.ctx, func(txn kv.Txn) error {
		manager.metaLock.Lock(id)
		defer manager.metaLock.Unlock(id)
		// Load keyspace by id.
		meta, err = manager.store.LoadKeyspaceMeta(txn, id)
		if err != nil {
			return err
		}
		if meta == nil {
			return ErrKeyspaceNotFound
		}
		// Update keyspace meta.
		if err = updateKeyspaceState(meta, newState, now); err != nil {
			return err
		}
		return manager.store.SaveKeyspaceMeta(txn, meta)
	})
	if err != nil {
		log.Warn("[keyspace] failed to update keyspace config",
			zap.Uint32("ID", meta.GetId()),
			zap.String("name", meta.GetName()),
			zap.Error(err),
		)
		return nil, err
	}
	log.Info("[keyspace] keyspace state updated",
		zap.Uint32("ID", meta.GetId()),
		zap.String("name", meta.GetName()),
		zap.String("new state", newState.String()),
	)
	return meta, nil
}

// updateKeyspaceState updates keyspace meta and record the update time.
func updateKeyspaceState(meta *keyspacepb.KeyspaceMeta, newState keyspacepb.KeyspaceState, now int64) error {
	// If already in the target state, do nothing and return.
	if meta.GetState() == newState {
		return nil
	}
	// Consult state transition table to check if the operation is legal.
	if !slice.Contains(stateTransitionTable[meta.GetState()], newState) {
		return errors.Errorf("cannot change keyspace state from %s to %s", meta.GetState().String(), newState.String())
	}
	// If the operation is legal, update keyspace state and change time.
	meta.State = newState
	meta.StateChangedAt = now
	return nil
}

// LoadRangeKeyspace load up to limit keyspaces starting from keyspace with startID.
func (manager *Manager) LoadRangeKeyspace(startID uint32, limit int) ([]*keyspacepb.KeyspaceMeta, error) {
	// Load Start should fall within acceptable ID range.
	if startID > spaceIDMax {
		return nil, errors.Errorf("startID of the scan %d exceeds spaceID Max %d", startID, spaceIDMax)
	}
	return manager.store.LoadRangeKeyspace(startID, limit)
}

// allocID allocate a new keyspace id.
func (manager *Manager) allocID() (uint32, error) {
	id64, err := manager.idAllocator.Alloc()
	if err != nil {
		return 0, err
	}
	id32 := uint32(id64)
	if err = validateID(id32); err != nil {
		return 0, err
	}
	return id32, nil
}
