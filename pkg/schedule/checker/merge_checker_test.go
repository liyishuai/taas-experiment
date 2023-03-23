// Copyright 2019 TiKV Project Authors.
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

package checker

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/operatorutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type mergeCheckerTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *mockcluster.Cluster
	mc      *MergeChecker
	regions []*core.RegionInfo
}

func TestMergeCheckerTestSuite(t *testing.T) {
	suite.Run(t, new(mergeCheckerTestSuite))
}

func (suite *mergeCheckerTestSuite) SetupTest() {
	cfg := mockconfig.NewTestOptions()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster = mockcluster.NewCluster(suite.ctx, cfg)
	suite.cluster.SetMaxMergeRegionSize(2)
	suite.cluster.SetMaxMergeRegionKeys(2)
	suite.cluster.SetLabelProperty(config.RejectLeader, "reject", "leader")
	suite.cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	stores := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
		7: {"reject", "leader"},
		8: {"reject", "leader"},
	}
	for storeID, labels := range stores {
		suite.cluster.PutStoreWithLabels(storeID, labels...)
	}
	suite.regions = []*core.RegionInfo{
		newRegionInfo(1, "", "a", 1, 1, []uint64{101, 1}, []uint64{101, 1}, []uint64{102, 2}),
		newRegionInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newRegionInfo(3, "t", "x", 0, 0, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
		newRegionInfo(4, "x", "", 1, 1, []uint64{109, 4}, []uint64{109, 4}),
	}

	for _, region := range suite.regions {
		suite.cluster.PutRegion(region)
	}
	suite.mc = NewMergeChecker(suite.ctx, suite.cluster, suite.cluster.GetOpts())
}

func (suite *mergeCheckerTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *mergeCheckerTestSuite) TestBasic() {
	suite.cluster.SetSplitMergeInterval(0)

	// should with same peer count
	ops := suite.mc.Check(suite.regions[0])
	suite.Nil(ops)
	// The size should be small enough.
	ops = suite.mc.Check(suite.regions[1])
	suite.Nil(ops)
	// target region size is too large
	suite.cluster.PutRegion(suite.regions[1].Clone(core.SetApproximateSize(600)))
	ops = suite.mc.Check(suite.regions[2])
	suite.Nil(ops)

	// it can merge if the max region size of the store is greater than the target region size.
	suite.cluster.SetRegionMaxSize("144MiB")
	suite.cluster.SetRegionSizeMB(1024)

	ops = suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	suite.cluster.SetRegionSizeMB(144)
	ops = suite.mc.Check(suite.regions[2])
	suite.Nil(ops)
	// change the size back
	suite.cluster.PutRegion(suite.regions[1].Clone(core.SetApproximateSize(200)))
	ops = suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	// Check merge with previous region.
	suite.Equal(suite.regions[2].GetID(), ops[0].RegionID())
	suite.Equal(suite.regions[1].GetID(), ops[1].RegionID())

	// Test the peer store check.
	store := suite.cluster.GetStore(1)
	suite.NotNil(store)
	// Test the peer store is deleted.
	suite.cluster.DeleteStore(store)
	ops = suite.mc.Check(suite.regions[2])
	suite.Nil(ops)
	// Test the store is normal.
	suite.cluster.PutStore(store)
	ops = suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	suite.Equal(suite.regions[2].GetID(), ops[0].RegionID())
	suite.Equal(suite.regions[1].GetID(), ops[1].RegionID())
	// Test the store is offline.
	suite.cluster.SetStoreOffline(store.GetID())
	ops = suite.mc.Check(suite.regions[2])
	// Only target region have a peer on the offline store,
	// so it's not ok to merge.
	suite.Nil(ops)
	// Test the store is up.
	suite.cluster.SetStoreUp(store.GetID())
	ops = suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	suite.Equal(suite.regions[2].GetID(), ops[0].RegionID())
	suite.Equal(suite.regions[1].GetID(), ops[1].RegionID())
	store = suite.cluster.GetStore(5)
	suite.NotNil(store)
	// Test the peer store is deleted.
	suite.cluster.DeleteStore(store)
	ops = suite.mc.Check(suite.regions[2])
	suite.Nil(ops)
	// Test the store is normal.
	suite.cluster.PutStore(store)
	ops = suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	suite.Equal(suite.regions[2].GetID(), ops[0].RegionID())
	suite.Equal(suite.regions[1].GetID(), ops[1].RegionID())
	// Test the store is offline.
	suite.cluster.SetStoreOffline(store.GetID())
	ops = suite.mc.Check(suite.regions[2])
	// Both regions have peers on the offline store,
	// so it's ok to merge.
	suite.NotNil(ops)
	suite.Equal(suite.regions[2].GetID(), ops[0].RegionID())
	suite.Equal(suite.regions[1].GetID(), ops[1].RegionID())
	// Test the store is up.
	suite.cluster.SetStoreUp(store.GetID())
	ops = suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	suite.Equal(suite.regions[2].GetID(), ops[0].RegionID())
	suite.Equal(suite.regions[1].GetID(), ops[1].RegionID())

	// Enable one way merge
	suite.cluster.SetEnableOneWayMerge(true)
	ops = suite.mc.Check(suite.regions[2])
	suite.Nil(ops)
	suite.cluster.SetEnableOneWayMerge(false)

	// Make up peers for next region.
	suite.regions[3] = suite.regions[3].Clone(core.WithAddPeer(&metapb.Peer{Id: 110, StoreId: 1}), core.WithAddPeer(&metapb.Peer{Id: 111, StoreId: 2}))
	suite.cluster.PutRegion(suite.regions[3])
	ops = suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	// Now it merges to next region.
	suite.Equal(suite.regions[2].GetID(), ops[0].RegionID())
	suite.Equal(suite.regions[3].GetID(), ops[1].RegionID())

	// merge cannot across rule key.
	suite.cluster.SetEnablePlacementRules(true)
	suite.cluster.RuleManager.SetRule(&placement.Rule{
		GroupID:     "pd",
		ID:          "test",
		Index:       1,
		Override:    true,
		StartKeyHex: hex.EncodeToString([]byte("x")),
		EndKeyHex:   hex.EncodeToString([]byte("z")),
		Role:        placement.Voter,
		Count:       3,
	})
	// region 2 can only merge with previous region now.
	ops = suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	suite.Equal(suite.regions[2].GetID(), ops[0].RegionID())
	suite.Equal(suite.regions[1].GetID(), ops[1].RegionID())
	suite.cluster.RuleManager.DeleteRule("pd", "test")

	//  check 'merge_option' label
	suite.cluster.GetRegionLabeler().SetLabelRule(&labeler.LabelRule{
		ID:       "test",
		Labels:   []labeler.RegionLabel{{Key: mergeOptionLabel, Value: mergeOptionValueDeny}},
		RuleType: labeler.KeyRange,
		Data:     makeKeyRanges("", "74"),
	})
	ops = suite.mc.Check(suite.regions[0])
	suite.Empty(ops)
	ops = suite.mc.Check(suite.regions[1])
	suite.Empty(ops)

	// Skip recently split regions.
	suite.cluster.SetSplitMergeInterval(time.Hour)
	ops = suite.mc.Check(suite.regions[2])
	suite.Nil(ops)

	suite.mc.startTime = time.Now().Add(-2 * time.Hour)
	ops = suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	ops = suite.mc.Check(suite.regions[3])
	suite.NotNil(ops)

	suite.mc.RecordRegionSplit([]uint64{suite.regions[2].GetID()})
	ops = suite.mc.Check(suite.regions[2])
	suite.Nil(ops)
	ops = suite.mc.Check(suite.regions[3])
	suite.Nil(ops)

	suite.cluster.SetSplitMergeInterval(500 * time.Millisecond)
	ops = suite.mc.Check(suite.regions[2])
	suite.Nil(ops)
	ops = suite.mc.Check(suite.regions[3])
	suite.Nil(ops)

	time.Sleep(500 * time.Millisecond)
	ops = suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	ops = suite.mc.Check(suite.regions[3])
	suite.NotNil(ops)
}

func (suite *mergeCheckerTestSuite) TestMatchPeers() {
	suite.cluster.SetSplitMergeInterval(0)
	// partial store overlap not including leader
	ops := suite.mc.Check(suite.regions[2])
	suite.NotNil(ops)
	operatorutil.CheckSteps(suite.Require(), ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 2},
		operator.AddLearner{ToStore: 4},
		operator.PromoteLearner{ToStore: 4},
		operator.TransferLeader{FromStore: 6, ToStore: 5},
		operator.RemovePeer{FromStore: 6},
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(suite.Require(), ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// partial store overlap including leader
	newRegion := suite.regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 106, StoreId: 1},
			{Id: 107, StoreId: 5},
			{Id: 108, StoreId: 6},
		}),
		core.WithLeader(&metapb.Peer{Id: 106, StoreId: 1}),
	)
	suite.regions[2] = newRegion
	suite.cluster.PutRegion(suite.regions[2])
	ops = suite.mc.Check(suite.regions[2])
	operatorutil.CheckSteps(suite.Require(), ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 4},
		operator.PromoteLearner{ToStore: 4},
		operator.RemovePeer{FromStore: 6},
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(suite.Require(), ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// all stores overlap
	suite.regions[2] = suite.regions[2].Clone(core.SetPeers([]*metapb.Peer{
		{Id: 106, StoreId: 1},
		{Id: 107, StoreId: 5},
		{Id: 108, StoreId: 4},
	}))
	suite.cluster.PutRegion(suite.regions[2])
	ops = suite.mc.Check(suite.regions[2])
	operatorutil.CheckSteps(suite.Require(), ops[0], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(suite.Require(), ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// all stores not overlap
	suite.regions[2] = suite.regions[2].Clone(core.SetPeers([]*metapb.Peer{
		{Id: 109, StoreId: 2},
		{Id: 110, StoreId: 3},
		{Id: 111, StoreId: 6},
	}), core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}))
	suite.cluster.PutRegion(suite.regions[2])
	ops = suite.mc.Check(suite.regions[2])
	operatorutil.CheckSteps(suite.Require(), ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 3},
		operator.AddLearner{ToStore: 4},
		operator.PromoteLearner{ToStore: 4},
		operator.RemovePeer{FromStore: 6},
		operator.AddLearner{ToStore: 5},
		operator.PromoteLearner{ToStore: 5},
		operator.TransferLeader{FromStore: 2, ToStore: 1},
		operator.RemovePeer{FromStore: 2},
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(suite.Require(), ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// no overlap with reject leader label
	suite.regions[1] = suite.regions[1].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 112, StoreId: 7},
			{Id: 113, StoreId: 8},
			{Id: 114, StoreId: 1},
		}),
		core.WithLeader(&metapb.Peer{Id: 114, StoreId: 1}),
	)
	suite.cluster.PutRegion(suite.regions[1])
	ops = suite.mc.Check(suite.regions[2])
	operatorutil.CheckSteps(suite.Require(), ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 3},

		operator.AddLearner{ToStore: 7},
		operator.PromoteLearner{ToStore: 7},
		operator.RemovePeer{FromStore: 6},

		operator.AddLearner{ToStore: 8},
		operator.PromoteLearner{ToStore: 8},
		operator.TransferLeader{FromStore: 2, ToStore: 1},
		operator.RemovePeer{FromStore: 2},

		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(suite.Require(), ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})

	// overlap with reject leader label
	suite.regions[1] = suite.regions[1].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 115, StoreId: 7},
			{Id: 116, StoreId: 8},
			{Id: 117, StoreId: 1},
		}),
		core.WithLeader(&metapb.Peer{Id: 117, StoreId: 1}),
	)
	suite.regions[2] = suite.regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 118, StoreId: 7},
			{Id: 119, StoreId: 3},
			{Id: 120, StoreId: 2},
		}),
		core.WithLeader(&metapb.Peer{Id: 120, StoreId: 2}),
	)
	suite.cluster.PutRegion(suite.regions[1])
	ops = suite.mc.Check(suite.regions[2])
	operatorutil.CheckSteps(suite.Require(), ops[0], []operator.OpStep{
		operator.AddLearner{ToStore: 1},
		operator.PromoteLearner{ToStore: 1},
		operator.RemovePeer{FromStore: 3},
		operator.AddLearner{ToStore: 8},
		operator.PromoteLearner{ToStore: 8},
		operator.TransferLeader{FromStore: 2, ToStore: 1},
		operator.RemovePeer{FromStore: 2},
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  false,
		},
	})
	operatorutil.CheckSteps(suite.Require(), ops[1], []operator.OpStep{
		operator.MergeRegion{
			FromRegion: suite.regions[2].GetMeta(),
			ToRegion:   suite.regions[1].GetMeta(),
			IsPassive:  true,
		},
	})
}

func (suite *mergeCheckerTestSuite) TestStoreLimitWithMerge() {
	cfg := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, cfg)
	tc.SetMaxMergeRegionSize(2)
	tc.SetMaxMergeRegionKeys(2)
	tc.SetSplitMergeInterval(0)
	regions := []*core.RegionInfo{
		newRegionInfo(1, "", "a", 1, 1, []uint64{101, 1}, []uint64{101, 1}, []uint64{102, 2}),
		newRegionInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newRegionInfo(3, "t", "x", 1, 1, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
		newRegionInfo(4, "x", "", 10, 10, []uint64{109, 4}, []uint64{109, 4}),
	}

	for i := uint64(1); i <= 6; i++ {
		tc.AddLeaderStore(i, 10)
	}

	for _, region := range regions {
		tc.PutRegion(region)
	}

	mc := NewMergeChecker(suite.ctx, tc, tc.GetOpts())
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc.ID, tc, false /* no need to run */)
	oc := schedule.NewOperatorController(suite.ctx, tc, stream)

	regions[2] = regions[2].Clone(
		core.SetPeers([]*metapb.Peer{
			{Id: 109, StoreId: 2},
			{Id: 110, StoreId: 3},
			{Id: 111, StoreId: 6},
		}),
		core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}),
	)

	// set to a small rate to reduce unstable possibility.
	tc.SetAllStoresLimit(storelimit.AddPeer, 0.0000001)
	tc.SetAllStoresLimit(storelimit.RemovePeer, 0.0000001)
	tc.PutRegion(regions[2])
	// The size of Region is less or equal than 1MB.
	for i := 0; i < 50; i++ {
		ops := mc.Check(regions[2])
		suite.NotNil(ops)
		suite.True(oc.AddOperator(ops...))
		for _, op := range ops {
			oc.RemoveOperator(op)
		}
	}
	regions[2] = regions[2].Clone(
		core.SetApproximateSize(2),
		core.SetApproximateKeys(2),
	)
	tc.PutRegion(regions[2])
	// The size of Region is more than 1MB but no more than 20MB.
	for i := 0; i < 5; i++ {
		ops := mc.Check(regions[2])
		suite.NotNil(ops)
		suite.True(oc.AddOperator(ops...))
		for _, op := range ops {
			oc.RemoveOperator(op)
		}
	}
	{
		ops := mc.Check(regions[2])
		suite.NotNil(ops)
		suite.False(oc.AddOperator(ops...))
	}
}

func (suite *mergeCheckerTestSuite) TestCache() {
	cfg := mockconfig.NewTestOptions()
	suite.cluster = mockcluster.NewCluster(suite.ctx, cfg)
	suite.cluster.SetMaxMergeRegionSize(2)
	suite.cluster.SetMaxMergeRegionKeys(2)
	suite.cluster.SetSplitMergeInterval(time.Hour)
	suite.cluster.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	stores := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
	}
	for storeID, labels := range stores {
		suite.cluster.PutStoreWithLabels(storeID, labels...)
	}
	suite.regions = []*core.RegionInfo{
		newRegionInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newRegionInfo(3, "t", "x", 1, 1, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
	}

	for _, region := range suite.regions {
		suite.cluster.PutRegion(region)
	}

	suite.mc = NewMergeChecker(suite.ctx, suite.cluster, suite.cluster.GetOpts())

	ops := suite.mc.Check(suite.regions[1])
	suite.Nil(ops)
	suite.cluster.SetSplitMergeInterval(0)
	time.Sleep(time.Second)
	ops = suite.mc.Check(suite.regions[1])
	suite.NotNil(ops)
}

func makeKeyRanges(keys ...string) []interface{} {
	var res []interface{}
	for i := 0; i < len(keys); i += 2 {
		res = append(res, map[string]interface{}{"start_key": keys[i], "end_key": keys[i+1]})
	}
	return res
}

func newRegionInfo(id uint64, startKey, endKey string, size, keys int64, leader []uint64, peers ...[]uint64) *core.RegionInfo {
	prs := make([]*metapb.Peer, 0, len(peers))
	for _, peer := range peers {
		prs = append(prs, &metapb.Peer{Id: peer[0], StoreId: peer[1]})
	}
	return core.NewRegionInfo(
		&metapb.Region{
			Id:       id,
			StartKey: []byte(startKey),
			EndKey:   []byte(endKey),
			Peers:    prs,
		},
		&metapb.Peer{Id: leader[0], StoreId: leader[1]},
		core.SetApproximateSize(size),
		core.SetApproximateKeys(keys),
	)
}
