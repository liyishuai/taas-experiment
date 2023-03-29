// Copyright 2020 TiKV Project Authors.
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

package replication

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	pb "github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
)

func TestInitial(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeMajority}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)
	re.Equal(&pb.ReplicationStatus{Mode: pb.ReplicationMode_MAJORITY}, rep.GetReplicationStatus())

	conf = config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "dr-label",
		Primary:          "l1",
		DR:               "l2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	rep, err = NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)
	re.Equal(&pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC,
			StateId:             1,
			WaitSyncTimeoutHint: 60,
		},
	}, rep.GetReplicationStatus())
}

func TestStatus(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey: "dr-label",
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)
	re.Equal(&pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC,
			StateId:             1,
			WaitSyncTimeoutHint: 60,
		},
	}, rep.GetReplicationStatus())

	err = rep.drSwitchToAsync(nil)
	re.NoError(err)
	re.Equal(&pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_ASYNC,
			StateId:             2,
			WaitSyncTimeoutHint: 60,
		},
	}, rep.GetReplicationStatus())

	err = rep.drSwitchToSyncRecover()
	re.NoError(err)
	stateID := rep.drAutoSync.StateID
	re.Equal(&pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC_RECOVER,
			StateId:             stateID,
			WaitSyncTimeoutHint: 60,
		},
	}, rep.GetReplicationStatus())

	// test reload
	rep, err = NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)
	re.Equal(drStateSyncRecover, rep.drAutoSync.State)

	err = rep.drSwitchToSync()
	re.NoError(err)
	re.Equal(&pb.ReplicationStatus{
		Mode: pb.ReplicationMode_DR_AUTO_SYNC,
		DrAutoSync: &pb.DRAutoSync{
			LabelKey:            "dr-label",
			State:               pb.DRAutoSyncState_SYNC,
			StateId:             rep.drAutoSync.StateID,
			WaitSyncTimeoutHint: 60,
		},
	}, rep.GetReplicationStatus())
}

type mockFileReplicator struct {
	memberIDs []uint64
	lastData  map[uint64]string
	errors    map[uint64]error
}

func (rep *mockFileReplicator) GetMembers() ([]*pdpb.Member, error) {
	var members []*pdpb.Member
	for _, id := range rep.memberIDs {
		members = append(members, &pdpb.Member{MemberId: id})
	}
	return members, nil
}

func (rep *mockFileReplicator) ReplicateFileToMember(ctx context.Context, member *pdpb.Member, name string, data []byte) error {
	if err := rep.errors[member.GetMemberId()]; err != nil {
		return err
	}
	rep.lastData[member.GetMemberId()] = string(data)
	return nil
}

func newMockReplicator(ids []uint64) *mockFileReplicator {
	return &mockFileReplicator{
		memberIDs: ids,
		lastData:  make(map[uint64]string),
		errors:    make(map[uint64]error),
	}
}

func TestStateSwitch(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  4,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	replicator := newMockReplicator([]uint64{1})
	rep, err := NewReplicationModeManager(conf, store, cluster, replicator)
	re.NoError(err)

	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(3, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(4, 1, map[string]string{"zone": "zone1"})

	// initial state is sync
	re.Equal(drStateSync, rep.drGetState())
	stateID := rep.drAutoSync.StateID
	re.NotEqual(uint64(0), stateID)
	re.Equal(fmt.Sprintf(`{"state":"sync","state_id":%d}`, stateID), replicator.lastData[1])
	assertStateIDUpdate := func() {
		re.NotEqual(stateID, rep.drAutoSync.StateID)
		stateID = rep.drAutoSync.StateID
	}
	syncStoreStatus := func(storeIDs ...uint64) {
		state := rep.GetReplicationStatus()
		for _, s := range storeIDs {
			rep.UpdateStoreDRStatus(s, &pb.StoreDRAutoSyncStatus{State: state.GetDrAutoSync().State, StateId: state.GetDrAutoSync().GetStateId()})
		}
	}

	// only one zone, sync -> async_wait -> async
	rep.tickDR()
	re.Equal(drStateAsyncWait, rep.drGetState())
	assertStateIDUpdate()
	re.Equal(fmt.Sprintf(`{"state":"async_wait","state_id":%d,"available_stores":[1,2,3,4]}`, stateID), replicator.lastData[1])

	re.False(rep.GetReplicationStatus().GetDrAutoSync().GetPauseRegionSplit())
	conf.DRAutoSync.PauseRegionSplit = true
	rep.UpdateConfig(conf)
	re.True(rep.GetReplicationStatus().GetDrAutoSync().GetPauseRegionSplit())

	syncStoreStatus(1, 2, 3, 4)
	rep.tickDR()
	assertStateIDUpdate()
	re.Equal(fmt.Sprintf(`{"state":"async","state_id":%d,"available_stores":[1,2,3,4]}`, stateID), replicator.lastData[1])

	// add new store in dr zone.
	cluster.AddLabelsStore(5, 1, map[string]string{"zone": "zone2"})
	cluster.AddLabelsStore(6, 1, map[string]string{"zone": "zone2"})
	// async -> sync
	rep.tickDR()
	re.Equal(drStateSyncRecover, rep.drGetState())
	rep.drSwitchToSync()
	re.Equal(drStateSync, rep.drGetState())
	assertStateIDUpdate()

	// sync -> async_wait
	rep.tickDR()
	re.Equal(drStateSync, rep.drGetState())
	setStoreState(cluster, "down", "up", "up", "up", "up", "up")
	rep.tickDR()
	re.Equal(drStateSync, rep.drGetState())
	setStoreState(cluster, "down", "down", "up", "up", "up", "up")
	setStoreState(cluster, "down", "down", "down", "up", "up", "up")
	rep.tickDR()
	re.Equal(drStateSync, rep.drGetState()) // cannot guarantee majority, keep sync.

	setStoreState(cluster, "up", "up", "up", "up", "down", "up")
	rep.tickDR()
	re.Equal(drStateAsyncWait, rep.drGetState())
	assertStateIDUpdate()

	rep.drSwitchToSync()
	replicator.errors[2] = errors.New("fail to replicate")
	rep.tickDR()
	re.Equal(drStateAsyncWait, rep.drGetState())
	assertStateIDUpdate()
	delete(replicator.errors, 1)

	// async_wait -> sync
	setStoreState(cluster, "up", "up", "up", "up", "up", "up")
	rep.tickDR()
	re.Equal(drStateSync, rep.drGetState())
	re.False(rep.GetReplicationStatus().GetDrAutoSync().GetPauseRegionSplit())

	// async_wait -> async_wait
	setStoreState(cluster, "up", "up", "up", "up", "down", "up")
	rep.tickDR()
	re.Equal(drStateAsyncWait, rep.drGetState())
	assertStateIDUpdate()
	re.Equal(fmt.Sprintf(`{"state":"async_wait","state_id":%d,"available_stores":[1,2,3,4]}`, stateID), replicator.lastData[1])
	setStoreState(cluster, "down", "up", "up", "up", "down", "up")
	rep.tickDR()
	assertStateIDUpdate()
	re.Equal(fmt.Sprintf(`{"state":"async_wait","state_id":%d,"available_stores":[2,3,4]}`, stateID), replicator.lastData[1])
	setStoreState(cluster, "up", "down", "up", "up", "down", "up")
	rep.tickDR()
	assertStateIDUpdate()
	re.Equal(fmt.Sprintf(`{"state":"async_wait","state_id":%d,"available_stores":[1,3,4]}`, stateID), replicator.lastData[1])

	// async_wait -> async
	rep.tickDR()
	re.Equal(drStateAsyncWait, rep.drGetState())
	syncStoreStatus(1, 3)
	rep.tickDR()
	re.Equal(drStateAsyncWait, rep.drGetState())
	syncStoreStatus(4)
	rep.tickDR()
	assertStateIDUpdate()
	re.Equal(fmt.Sprintf(`{"state":"async","state_id":%d,"available_stores":[1,3,4]}`, stateID), replicator.lastData[1])

	// async -> async
	setStoreState(cluster, "up", "up", "up", "up", "down", "up")
	rep.tickDR()
	// store 2 won't be available before it syncs status.
	re.Equal(fmt.Sprintf(`{"state":"async","state_id":%d,"available_stores":[1,3,4]}`, stateID), replicator.lastData[1])
	syncStoreStatus(1, 2, 3, 4)
	rep.tickDR()
	assertStateIDUpdate()
	re.Equal(fmt.Sprintf(`{"state":"async","state_id":%d,"available_stores":[1,2,3,4]}`, stateID), replicator.lastData[1])

	// async -> sync_recover
	setStoreState(cluster, "up", "up", "up", "up", "up", "up")
	rep.tickDR()
	re.Equal(drStateSyncRecover, rep.drGetState())
	assertStateIDUpdate()
	rep.drSwitchToAsync([]uint64{1, 2, 3, 4, 5})
	setStoreState(cluster, "down", "up", "up", "up", "up", "up")
	rep.tickDR()
	re.Equal(drStateSyncRecover, rep.drGetState())
	assertStateIDUpdate()

	// sync_recover -> async
	rep.tickDR()
	re.Equal(drStateSyncRecover, rep.drGetState())
	setStoreState(cluster, "up", "up", "up", "up", "down", "up")
	rep.tickDR()
	re.Equal(drStateAsync, rep.drGetState())
	assertStateIDUpdate()
	// lost majority, does not switch to async.
	rep.drSwitchToSyncRecover()
	assertStateIDUpdate()
	setStoreState(cluster, "down", "down", "up", "up", "down", "up")
	rep.tickDR()
	re.Equal(drStateSyncRecover, rep.drGetState())

	// sync_recover -> sync
	rep.drSwitchToSyncRecover()
	assertStateIDUpdate()
	setStoreState(cluster, "up", "up", "up", "up", "up", "up")
	cluster.AddLeaderRegion(1, 1, 2, 3, 4, 5)
	region := cluster.GetRegion(1)

	region = region.Clone(core.WithStartKey(nil), core.WithEndKey(nil), core.SetReplicationStatus(&pb.RegionReplicationStatus{
		State: pb.RegionReplicationState_SIMPLE_MAJORITY,
	}))
	cluster.PutRegion(region)
	rep.tickDR()
	re.Equal(drStateSyncRecover, rep.drGetState())

	region = region.Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
		State:   pb.RegionReplicationState_INTEGRITY_OVER_LABEL,
		StateId: rep.drAutoSync.StateID - 1, // mismatch state id
	}))
	cluster.PutRegion(region)
	rep.tickDR()
	re.Equal(drStateSyncRecover, rep.drGetState())
	region = region.Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
		State:   pb.RegionReplicationState_INTEGRITY_OVER_LABEL,
		StateId: rep.drAutoSync.StateID,
	}))
	cluster.PutRegion(region)
	rep.tickDR()
	re.Equal(drStateSync, rep.drGetState())
	assertStateIDUpdate()
}

func TestReplicateState(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	replicator := newMockReplicator([]uint64{1})
	rep, err := NewReplicationModeManager(conf, store, cluster, replicator)
	re.NoError(err)

	stateID := rep.drAutoSync.StateID
	// replicate after initialized
	re.Equal(fmt.Sprintf(`{"state":"sync","state_id":%d}`, stateID), replicator.lastData[1])

	// repliate state to new member
	replicator.memberIDs = append(replicator.memberIDs, 2, 3)
	rep.checkReplicateFile()
	re.Equal(fmt.Sprintf(`{"state":"sync","state_id":%d}`, stateID), replicator.lastData[2])
	re.Equal(fmt.Sprintf(`{"state":"sync","state_id":%d}`, stateID), replicator.lastData[3])

	// inject error
	replicator.errors[2] = errors.New("failed to persist")
	rep.tickDR() // switch async_wait since there is only one zone
	newStateID := rep.drAutoSync.StateID
	re.Equal(fmt.Sprintf(`{"state":"async_wait","state_id":%d}`, newStateID), replicator.lastData[1])
	re.Equal(fmt.Sprintf(`{"state":"sync","state_id":%d}`, stateID), replicator.lastData[2])
	re.Equal(fmt.Sprintf(`{"state":"async_wait","state_id":%d}`, newStateID), replicator.lastData[3])

	// clear error, replicate to node 2 next time
	delete(replicator.errors, 2)
	rep.checkReplicateFile()
	re.Equal(fmt.Sprintf(`{"state":"async_wait","state_id":%d}`, newStateID), replicator.lastData[2])
}

func TestAsynctimeout(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	var replicator mockFileReplicator
	rep, err := NewReplicationModeManager(conf, store, cluster, &replicator)
	re.NoError(err)

	cluster.AddLabelsStore(1, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"zone": "zone1"})
	cluster.AddLabelsStore(3, 1, map[string]string{"zone": "zone2"})

	setStoreState(cluster, "up", "up", "down")
	rep.tickDR()
	re.Equal(drStateAsyncWait, rep.drGetState())
}

func setStoreState(cluster *mockcluster.Cluster, states ...string) {
	for i, state := range states {
		store := cluster.GetStore(uint64(i + 1))
		if state == "down" {
			store.GetMeta().LastHeartbeat = time.Now().Add(-time.Minute * 10).UnixNano()
		} else if state == "up" {
			store.GetMeta().LastHeartbeat = time.Now().UnixNano()
		}
		cluster.PutStore(store)
	}
}

func TestRecoverProgress(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	regionScanBatchSize = 10
	regionMinSampleSize = 5

	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	cluster.AddLabelsStore(1, 1, map[string]string{})
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)

	prepare := func(n int, asyncRegions []int) {
		rep.drSwitchToSyncRecover()
		regions := genRegions(cluster, rep.drAutoSync.StateID, n)
		for _, i := range asyncRegions {
			regions[i] = regions[i].Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
				State:   pb.RegionReplicationState_SIMPLE_MAJORITY,
				StateId: regions[i].GetReplicationStatus().GetStateId(),
			}))
		}
		for _, r := range regions {
			cluster.PutRegion(r)
		}
		rep.updateProgress()
	}

	prepare(20, nil)
	re.Equal(20, rep.drRecoverCount)
	re.Equal(float32(1.0), rep.estimateProgress())

	prepare(10, []int{9})
	re.Equal(9, rep.drRecoverCount)
	re.Equal(10, rep.drTotalRegion)
	re.Equal(1, rep.drSampleTotalRegion)
	re.Equal(0, rep.drSampleRecoverCount)
	re.Equal(float32(9)/float32(10), rep.estimateProgress())

	prepare(30, []int{3, 4, 5, 6, 7, 8, 9})
	re.Equal(3, rep.drRecoverCount)
	re.Equal(30, rep.drTotalRegion)
	re.Equal(7, rep.drSampleTotalRegion)
	re.Equal(0, rep.drSampleRecoverCount)
	re.Equal(float32(3)/float32(30), rep.estimateProgress())

	prepare(30, []int{9, 13, 14})
	re.Equal(9, rep.drRecoverCount)
	re.Equal(30, rep.drTotalRegion)
	re.Equal(6, rep.drSampleTotalRegion) // 9 + 10,11,12,13,14
	re.Equal(3, rep.drSampleRecoverCount)
	re.Equal((float32(9)+float32(30-9)/2)/float32(30), rep.estimateProgress())
}

func TestRecoverProgressWithSplitAndMerge(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	regionScanBatchSize = 10
	regionMinSampleSize = 5

	store := storage.NewStorageWithMemoryBackend()
	conf := config.ReplicationModeConfig{ReplicationMode: modeDRAutoSync, DRAutoSync: config.DRAutoSyncReplicationConfig{
		LabelKey:         "zone",
		Primary:          "zone1",
		DR:               "zone2",
		PrimaryReplicas:  2,
		DRReplicas:       1,
		WaitStoreTimeout: typeutil.Duration{Duration: time.Minute},
	}}
	cluster := mockcluster.NewCluster(ctx, mockconfig.NewTestOptions())
	cluster.AddLabelsStore(1, 1, map[string]string{})
	rep, err := NewReplicationModeManager(conf, store, cluster, newMockReplicator([]uint64{1}))
	re.NoError(err)

	prepare := func(n int, asyncRegions []int) {
		rep.drSwitchToSyncRecover()
		regions := genRegions(cluster, rep.drAutoSync.StateID, n)
		for _, i := range asyncRegions {
			regions[i] = regions[i].Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
				State:   pb.RegionReplicationState_SIMPLE_MAJORITY,
				StateId: regions[i].GetReplicationStatus().GetStateId(),
			}))
		}
		for _, r := range regions {
			cluster.PutRegion(r)
		}
	}

	// merged happened in ahead of the scan
	prepare(20, nil)
	r := cluster.GetRegion(1).Clone(core.WithEndKey(cluster.GetRegion(2).GetEndKey()))
	cluster.PutRegion(r)
	rep.updateProgress()
	re.Equal(19, rep.drRecoverCount)
	re.Equal(float32(1.0), rep.estimateProgress())

	// merged happened during the scan
	prepare(20, nil)
	r1 := cluster.GetRegion(1)
	r2 := cluster.GetRegion(2)
	r = r1.Clone(core.WithEndKey(r2.GetEndKey()))
	cluster.PutRegion(r)
	rep.drRecoverCount = 1
	rep.drRecoverKey = r1.GetEndKey()
	rep.updateProgress()
	re.Equal(20, rep.drRecoverCount)
	re.Equal(float32(1.0), rep.estimateProgress())

	// split, region gap happened during the scan
	rep.drRecoverCount, rep.drRecoverKey = 0, nil
	cluster.PutRegion(r1)
	rep.updateProgress()
	re.Equal(1, rep.drRecoverCount)
	re.NotEqual(float32(1.0), rep.estimateProgress())
	// region gap missing
	cluster.PutRegion(r2)
	rep.updateProgress()
	re.Equal(20, rep.drRecoverCount)
	re.Equal(float32(1.0), rep.estimateProgress())
}

func genRegions(cluster *mockcluster.Cluster, stateID uint64, n int) []*core.RegionInfo {
	var regions []*core.RegionInfo
	for i := 1; i <= n; i++ {
		cluster.AddLeaderRegion(uint64(i), 1)
		region := cluster.GetRegion(uint64(i))
		if i == 1 {
			region = region.Clone(core.WithStartKey(nil))
		}
		if i == n {
			region = region.Clone(core.WithEndKey(nil))
		}
		region = region.Clone(core.SetReplicationStatus(&pb.RegionReplicationStatus{
			State:   pb.RegionReplicationState_INTEGRITY_OVER_LABEL,
			StateId: stateID,
		}))
		regions = append(regions, region)
	}
	return regions
}
