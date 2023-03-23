// Copyright 2018 TiKV Project Authors.
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

package schedule

import (
	"container/heap"
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/mock/mockconfig"
	"github.com/tikv/pd/pkg/schedule/hbstream"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
)

type operatorControllerTestSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc
}

func TestOperatorControllerTestSuite(t *testing.T) {
	suite.Run(t, new(operatorControllerTestSuite))
}

func (suite *operatorControllerTestSuite) SetupSuite() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/unexpectedOperator", "return(true)"))
}

func (suite *operatorControllerTestSuite) TearDownSuite() {
	suite.cancel()
}

// issue #1338
func (suite *operatorControllerTestSuite) TestGetOpInfluence() {
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	oc := NewOperatorController(suite.ctx, tc, nil)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
	}
	op1 := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	op2 := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	suite.True(op1.Start())
	oc.SetOperator(op1)
	suite.True(op2.Start())
	oc.SetOperator(op2)
	re := suite.Require()
	go func(ctx context.Context) {
		suite.checkRemoveOperatorSuccess(oc, op1)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				re.False(oc.RemoveOperator(op1))
			}
		}
	}(suite.ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				oc.GetOpInfluence(tc)
			}
		}
	}(suite.ctx)
	time.Sleep(time.Second)
	suite.NotNil(oc.GetOperator(2))
}

func (suite *operatorControllerTestSuite) TestOperatorStatus() {
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(suite.ctx, tc, stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
		operator.AddPeer{ToStore: 2, PeerID: 4},
	}
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)
	op1 := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	op2 := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	suite.True(op1.Start())
	oc.SetOperator(op1)
	suite.True(op2.Start())
	oc.SetOperator(op2)
	suite.Equal(pdpb.OperatorStatus_RUNNING, oc.GetOperatorStatus(1).Status)
	suite.Equal(pdpb.OperatorStatus_RUNNING, oc.GetOperatorStatus(2).Status)
	op1.SetStatusReachTime(operator.STARTED, time.Now().Add(-operator.SlowStepWaitTime-operator.FastStepWaitTime))
	region2 = ApplyOperatorStep(region2, op2)
	tc.PutRegion(region2)
	oc.Dispatch(region1, "test")
	oc.Dispatch(region2, "test")
	suite.Equal(pdpb.OperatorStatus_TIMEOUT, oc.GetOperatorStatus(1).Status)
	suite.Equal(pdpb.OperatorStatus_RUNNING, oc.GetOperatorStatus(2).Status)
	ApplyOperator(tc, op2)
	oc.Dispatch(region2, "test")
	suite.Equal(pdpb.OperatorStatus_SUCCESS, oc.GetOperatorStatus(2).Status)
}

func (suite *operatorControllerTestSuite) TestFastFailOperator() {
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(suite.ctx, tc, stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	tc.AddLeaderRegion(1, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
		operator.AddPeer{ToStore: 3, PeerID: 4},
	}
	region := tc.GetRegion(1)
	op := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	suite.True(op.Start())
	oc.SetOperator(op)
	oc.Dispatch(region, "test")
	suite.Equal(pdpb.OperatorStatus_RUNNING, oc.GetOperatorStatus(1).Status)
	// change the leader
	region = region.Clone(core.WithLeader(region.GetPeer(2)))
	oc.Dispatch(region, DispatchFromHeartBeat)
	suite.Equal(operator.CANCELED, op.Status())
	suite.Nil(oc.GetOperator(region.GetID()))

	// transfer leader to an illegal store.
	op = operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 5})
	oc.SetOperator(op)
	oc.Dispatch(region, DispatchFromHeartBeat)
	suite.Equal(operator.CANCELED, op.Status())
	suite.Nil(oc.GetOperator(region.GetID()))
}

// Issue 3353
func (suite *operatorControllerTestSuite) TestFastFailWithUnhealthyStore() {
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(suite.ctx, tc, stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	tc.AddLeaderRegion(1, 1, 2)
	region := tc.GetRegion(1)
	steps := []operator.OpStep{operator.TransferLeader{ToStore: 2}}
	op := operator.NewTestOperator(1, region.GetRegionEpoch(), operator.OpLeader, steps...)
	oc.SetOperator(op)
	suite.False(oc.checkStaleOperator(op, steps[0], region))
	tc.SetStoreDown(2)
	suite.True(oc.checkStaleOperator(op, steps[0], region))
}

func (suite *operatorControllerTestSuite) TestCheckAddUnexpectedStatus() {
	suite.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/unexpectedOperator"))

	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(suite.ctx, tc, stream)
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 2, 1)
	tc.AddLeaderRegion(2, 2, 1)
	region1 := tc.GetRegion(1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 1},
		operator.AddPeer{ToStore: 1, PeerID: 4},
	}
	{
		// finished op
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 2})
		suite.True(oc.checkAddOperator(false, op))
		op.Start()
		suite.False(oc.checkAddOperator(false, op)) // started
		suite.Nil(op.Check(region1))

		suite.Equal(operator.SUCCESS, op.Status())
		suite.False(oc.checkAddOperator(false, op)) // success
	}
	{
		// finished op canceled
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 2})
		suite.True(oc.checkAddOperator(false, op))
		suite.True(op.Cancel())
		suite.False(oc.checkAddOperator(false, op))
	}
	{
		// finished op replaced
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 2})
		suite.True(oc.checkAddOperator(false, op))
		suite.True(op.Start())
		suite.True(op.Replace())
		suite.False(oc.checkAddOperator(false, op))
	}
	{
		// finished op expired
		op1 := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 2})
		op2 := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 1})
		suite.True(oc.checkAddOperator(false, op1, op2))
		op1.SetStatusReachTime(operator.CREATED, time.Now().Add(-operator.OperatorExpireTime))
		op2.SetStatusReachTime(operator.CREATED, time.Now().Add(-operator.OperatorExpireTime))
		suite.False(oc.checkAddOperator(false, op1, op2))
		suite.Equal(operator.EXPIRED, op1.Status())
		suite.Equal(operator.EXPIRED, op2.Status())
	}
	// finished op never timeout

	{
		// unfinished op timeout
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
		suite.True(oc.checkAddOperator(false, op))
		op.Start()
		op.SetStatusReachTime(operator.STARTED, time.Now().Add(-operator.SlowStepWaitTime-operator.FastStepWaitTime))
		suite.True(op.CheckTimeout())
		suite.False(oc.checkAddOperator(false, op))
	}
}

// issue #1716
func (suite *operatorControllerTestSuite) TestConcurrentRemoveOperator() {
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(suite.ctx, tc, stream)
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 2, 1)
	region1 := tc.GetRegion(1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 1},
		operator.AddPeer{ToStore: 1, PeerID: 4},
	}
	// finished op with normal priority
	op1 := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 2})
	// unfinished op with high priority
	op2 := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion|operator.OpAdmin, steps...)

	suite.True(op1.Start())
	oc.SetOperator(op1)

	suite.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/concurrentRemoveOperator", "return(true)"))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		oc.Dispatch(region1, "test")
		wg.Done()
	}()
	go func() {
		time.Sleep(50 * time.Millisecond)
		success := oc.AddOperator(op2)
		// If the assert failed before wg.Done, the test will be blocked.
		defer suite.True(success)
		wg.Done()
	}()
	wg.Wait()

	suite.Equal(op2, oc.GetOperator(1))
}

func (suite *operatorControllerTestSuite) TestPollDispatchRegion() {
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(suite.ctx, tc, stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderRegion(1, 1, 2)
	tc.AddLeaderRegion(2, 1, 2)
	tc.AddLeaderRegion(4, 2, 1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
		operator.AddPeer{ToStore: 2, PeerID: 4},
	}
	op1 := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 2})
	op2 := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	op3 := operator.NewTestOperator(3, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	op4 := operator.NewTestOperator(4, &metapb.RegionEpoch{}, operator.OpRegion, operator.TransferLeader{ToStore: 2})
	region1 := tc.GetRegion(1)
	region2 := tc.GetRegion(2)
	region4 := tc.GetRegion(4)
	// Adds operator and pushes to the notifier queue.
	{
		suite.True(op1.Start())
		oc.SetOperator(op1)
		suite.True(op3.Start())
		oc.SetOperator(op3)
		suite.True(op4.Start())
		oc.SetOperator(op4)
		suite.True(op2.Start())
		oc.SetOperator(op2)
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op1, time: time.Now().Add(100 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op3, time: time.Now().Add(300 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op4, time: time.Now().Add(499 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op2, time: time.Now().Add(500 * time.Millisecond)})
	}
	// first poll got nil
	r, next := oc.pollNeedDispatchRegion()
	suite.Nil(r)
	suite.False(next)

	// after wait 100 millisecond, the region1 need to dispatch, but not region2.
	time.Sleep(100 * time.Millisecond)
	r, next = oc.pollNeedDispatchRegion()
	suite.NotNil(r)
	suite.True(next)
	suite.Equal(region1.GetID(), r.GetID())

	// find op3 with nil region, remove it
	suite.NotNil(oc.GetOperator(3))

	r, next = oc.pollNeedDispatchRegion()
	suite.Nil(r)
	suite.True(next)
	suite.Nil(oc.GetOperator(3))

	// find op4 finished
	r, next = oc.pollNeedDispatchRegion()
	suite.NotNil(r)
	suite.True(next)
	suite.Equal(region4.GetID(), r.GetID())

	// after waiting 500 milliseconds, the region2 need to dispatch
	time.Sleep(400 * time.Millisecond)
	r, next = oc.pollNeedDispatchRegion()
	suite.NotNil(r)
	suite.True(next)
	suite.Equal(region2.GetID(), r.GetID())
	r, next = oc.pollNeedDispatchRegion()
	suite.Nil(r)
	suite.False(next)
}

func (suite *operatorControllerTestSuite) TestStoreLimit() {
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(suite.ctx, tc, stream)
	tc.AddLeaderStore(1, 0)
	tc.UpdateLeaderCount(1, 1000)
	tc.AddLeaderStore(2, 0)
	for i := uint64(1); i <= 1000; i++ {
		tc.AddLeaderRegion(i, i)
		// make it small region
		tc.PutRegion(tc.GetRegion(i).Clone(core.SetApproximateSize(10)))
	}

	tc.SetStoreLimit(2, storelimit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: i})
		suite.True(oc.AddOperator(op))
		suite.checkRemoveOperatorSuccess(oc, op)
	}
	op := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 1})
	suite.False(oc.AddOperator(op))
	suite.False(oc.RemoveOperator(op))

	tc.SetStoreLimit(2, storelimit.AddPeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewTestOperator(i, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: i})
		suite.True(oc.AddOperator(op))
		suite.checkRemoveOperatorSuccess(oc, op)
	}
	tc.SetAllStoresLimit(storelimit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = operator.NewTestOperator(i, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: i})
		suite.True(oc.AddOperator(op))
		suite.checkRemoveOperatorSuccess(oc, op)
	}
	op = operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: 2, PeerID: 1})
	suite.False(oc.AddOperator(op))
	suite.False(oc.RemoveOperator(op))

	tc.SetStoreLimit(2, storelimit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		suite.True(oc.AddOperator(op))
		suite.checkRemoveOperatorSuccess(oc, op)
	}
	op = operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
	suite.False(oc.AddOperator(op))
	suite.False(oc.RemoveOperator(op))

	tc.SetStoreLimit(2, storelimit.RemovePeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewTestOperator(i, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		suite.True(oc.AddOperator(op))
		suite.checkRemoveOperatorSuccess(oc, op)
	}
	tc.SetAllStoresLimit(storelimit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = operator.NewTestOperator(i, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		suite.True(oc.AddOperator(op))
		suite.checkRemoveOperatorSuccess(oc, op)
	}
	op = operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
	suite.False(oc.AddOperator(op))
	suite.False(oc.RemoveOperator(op))
}

// #1652
func (suite *operatorControllerTestSuite) TestDispatchOutdatedRegion() {
	cluster := mockcluster.NewCluster(suite.ctx, mockconfig.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, cluster.ID, cluster, false /* no need to run */)
	controller := NewOperatorController(suite.ctx, cluster, stream)

	cluster.AddLeaderStore(1, 2)
	cluster.AddLeaderStore(2, 0)
	cluster.SetAllStoresLimit(storelimit.RemovePeer, 600)
	cluster.AddLeaderRegion(1, 1, 2)
	steps := []operator.OpStep{
		operator.TransferLeader{FromStore: 1, ToStore: 2},
		operator.RemovePeer{FromStore: 1},
	}

	op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 0, Version: 0}, operator.OpRegion, steps...)
	suite.True(controller.AddOperator(op))
	suite.Equal(1, stream.MsgLength())

	// report the result of transferring leader
	region := cluster.MockRegionInfo(1, 2, []uint64{1, 2}, []uint64{},
		&metapb.RegionEpoch{ConfVer: 0, Version: 0})

	controller.Dispatch(region, DispatchFromHeartBeat)
	suite.Equal(uint64(0), op.ConfVerChanged(region))
	suite.Equal(2, stream.MsgLength())

	// report the result of removing peer
	region = cluster.MockRegionInfo(1, 2, []uint64{2}, []uint64{},
		&metapb.RegionEpoch{ConfVer: 0, Version: 0})

	controller.Dispatch(region, DispatchFromHeartBeat)
	suite.Equal(uint64(1), op.ConfVerChanged(region))
	suite.Equal(2, stream.MsgLength())

	// add and dispatch op again, the op should be stale
	op = operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 0, Version: 0},
		operator.OpRegion, steps...)
	suite.True(controller.AddOperator(op))
	suite.Equal(uint64(0), op.ConfVerChanged(region))
	suite.Equal(3, stream.MsgLength())

	// report region with an abnormal confver
	region = cluster.MockRegionInfo(1, 1, []uint64{1, 2}, []uint64{},
		&metapb.RegionEpoch{ConfVer: 1, Version: 0})
	controller.Dispatch(region, DispatchFromHeartBeat)
	suite.Equal(uint64(0), op.ConfVerChanged(region))
	// no new step
	suite.Equal(3, stream.MsgLength())
}

func (suite *operatorControllerTestSuite) TestCalcInfluence() {
	cluster := mockcluster.NewCluster(suite.ctx, mockconfig.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, cluster.ID, cluster, false /* no need to run */)
	controller := NewOperatorController(suite.ctx, cluster, stream)

	epoch := &metapb.RegionEpoch{ConfVer: 0, Version: 0}
	region := cluster.MockRegionInfo(1, 1, []uint64{2}, []uint64{}, epoch)
	region = region.Clone(core.SetApproximateSize(20))
	cluster.PutRegion(region)
	cluster.AddRegionStore(1, 1)
	cluster.AddRegionStore(3, 1)

	steps := []operator.OpStep{
		operator.AddLearner{ToStore: 3, PeerID: 3},
		operator.PromoteLearner{ToStore: 3, PeerID: 3},
		operator.TransferLeader{FromStore: 1, ToStore: 3},
		operator.RemovePeer{FromStore: 1},
	}
	op := operator.NewTestOperator(1, epoch, operator.OpRegion, steps...)
	suite.True(controller.AddOperator(op))

	check := func(influence operator.OpInfluence, id uint64, expect *operator.StoreInfluence) {
		si := influence.GetStoreInfluence(id)
		suite.Equal(si.LeaderCount, expect.LeaderCount)
		suite.Equal(si.LeaderSize, expect.LeaderSize)
		suite.Equal(si.RegionCount, expect.RegionCount)
		suite.Equal(si.RegionSize, expect.RegionSize)
		suite.Equal(si.StepCost[storelimit.AddPeer], expect.StepCost[storelimit.AddPeer])
		suite.Equal(si.StepCost[storelimit.RemovePeer], expect.StepCost[storelimit.RemovePeer])
	}

	influence := controller.GetOpInfluence(cluster)
	check(influence, 1, &operator.StoreInfluence{
		LeaderSize:  -20,
		LeaderCount: -1,
		RegionSize:  -20,
		RegionCount: -1,
		StepCost: map[storelimit.Type]int64{
			storelimit.RemovePeer: 200,
		},
	})
	check(influence, 3, &operator.StoreInfluence{
		LeaderSize:  20,
		LeaderCount: 1,
		RegionSize:  20,
		RegionCount: 1,
		StepCost: map[storelimit.Type]int64{
			storelimit.AddPeer: 200,
		},
	})

	region2 := region.Clone(
		core.WithAddPeer(&metapb.Peer{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}),
		core.WithIncConfVer(),
	)
	suite.True(steps[0].IsFinish(region2))
	op.Check(region2)

	influence = controller.GetOpInfluence(cluster)
	check(influence, 1, &operator.StoreInfluence{
		LeaderSize:  -20,
		LeaderCount: -1,
		RegionSize:  -20,
		RegionCount: -1,
		StepCost: map[storelimit.Type]int64{
			storelimit.RemovePeer: 200,
		},
	})
	check(influence, 3, &operator.StoreInfluence{
		LeaderSize:  20,
		LeaderCount: 1,
		RegionSize:  0,
		RegionCount: 0,
		StepCost:    make(map[storelimit.Type]int64),
	})
}

func (suite *operatorControllerTestSuite) TestDispatchUnfinishedStep() {
	cluster := mockcluster.NewCluster(suite.ctx, mockconfig.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, cluster.ID, cluster, false /* no need to run */)
	controller := NewOperatorController(suite.ctx, cluster, stream)

	// Create a new region with epoch(0, 0)
	// the region has two peers with its peer id allocated incrementally.
	// so the two peers are {peerid: 1, storeid: 1}, {peerid: 2, storeid: 2}
	// The peer on store 1 is the leader
	epoch := &metapb.RegionEpoch{ConfVer: 0, Version: 0}
	region := cluster.MockRegionInfo(1, 1, []uint64{2}, []uint64{}, epoch)
	// Put region into cluster, otherwise, AddOperator will fail because of
	// missing region
	cluster.PutRegion(region)
	cluster.AddRegionStore(1, 1)
	cluster.AddRegionStore(3, 1)
	// The next allocated peer should have peerid 3, so we add this peer
	// to store 3
	testSteps := [][]operator.OpStep{
		{
			operator.AddLearner{ToStore: 3, PeerID: 3},
			operator.PromoteLearner{ToStore: 3, PeerID: 3},
			operator.TransferLeader{FromStore: 1, ToStore: 3},
			operator.RemovePeer{FromStore: 1},
		},
		{
			operator.AddLearner{ToStore: 3, PeerID: 3, IsLightWeight: true},
			operator.PromoteLearner{ToStore: 3, PeerID: 3},
			operator.TransferLeader{FromStore: 1, ToStore: 3},
			operator.RemovePeer{FromStore: 1},
		},
	}

	for _, steps := range testSteps {
		// Create an operator
		op := operator.NewTestOperator(1, epoch, operator.OpRegion, steps...)
		suite.True(controller.AddOperator(op))
		suite.Equal(1, stream.MsgLength())

		// Create region2 which is cloned from the original region.
		// region2 has peer 2 in pending state, so the AddPeer step
		// is left unfinished
		region2 := region.Clone(
			core.WithAddPeer(&metapb.Peer{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}),
			core.WithPendingPeers([]*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
			}),
			core.WithIncConfVer(),
		)
		suite.NotNil(region2.GetPendingPeers())

		suite.False(steps[0].IsFinish(region2))
		controller.Dispatch(region2, DispatchFromHeartBeat)

		// In this case, the conf version has been changed, but the
		// peer added is in pending state, the operator should not be
		// removed by the stale checker
		suite.Equal(uint64(1), op.ConfVerChanged(region2))
		suite.NotNil(controller.GetOperator(1))

		// The operator is valid yet, but the step should not be sent
		// again, because it is in pending state, so the message channel
		// should not be increased
		suite.Equal(1, stream.MsgLength())

		// Finish the step by clearing the pending state
		region3 := region.Clone(
			core.WithAddPeer(&metapb.Peer{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner}),
			core.WithIncConfVer(),
		)
		suite.True(steps[0].IsFinish(region3))
		controller.Dispatch(region3, DispatchFromHeartBeat)
		suite.Equal(uint64(1), op.ConfVerChanged(region3))
		suite.Equal(2, stream.MsgLength())

		region4 := region3.Clone(
			core.WithRole(3, metapb.PeerRole_Voter),
			core.WithIncConfVer(),
		)
		suite.True(steps[1].IsFinish(region4))
		controller.Dispatch(region4, DispatchFromHeartBeat)
		suite.Equal(uint64(2), op.ConfVerChanged(region4))
		suite.Equal(3, stream.MsgLength())

		// Transfer leader
		region5 := region4.Clone(
			core.WithLeader(region4.GetStorePeer(3)),
		)
		suite.True(steps[2].IsFinish(region5))
		controller.Dispatch(region5, DispatchFromHeartBeat)
		suite.Equal(uint64(2), op.ConfVerChanged(region5))
		suite.Equal(4, stream.MsgLength())

		// Remove peer
		region6 := region5.Clone(
			core.WithRemoveStorePeer(1),
			core.WithIncConfVer(),
		)
		suite.True(steps[3].IsFinish(region6))
		controller.Dispatch(region6, DispatchFromHeartBeat)
		suite.Equal(uint64(3), op.ConfVerChanged(region6))

		// The Operator has finished, so no message should be sent
		suite.Equal(4, stream.MsgLength())
		suite.Nil(controller.GetOperator(1))
		e := stream.Drain(4)
		suite.NoError(e)
	}
}

func newRegionInfo(id uint64, startKey, endKey string, size, keys int64, leader []uint64, peers ...[]uint64) *core.RegionInfo {
	prs := make([]*metapb.Peer, 0, len(peers))
	for _, peer := range peers {
		prs = append(prs, &metapb.Peer{Id: peer[0], StoreId: peer[1]})
	}
	start, _ := hex.DecodeString(startKey)
	end, _ := hex.DecodeString(endKey)
	return core.NewRegionInfo(
		&metapb.Region{
			Id:       id,
			StartKey: start,
			EndKey:   end,
			Peers:    prs,
		},
		&metapb.Peer{Id: leader[0], StoreId: leader[1]},
		core.SetApproximateSize(size),
		core.SetApproximateKeys(keys),
	)
}

func (suite *operatorControllerTestSuite) checkRemoveOperatorSuccess(oc *OperatorController, op *operator.Operator) {
	suite.True(oc.RemoveOperator(op))
	suite.True(op.IsEnd())
	suite.Equal(op, oc.GetOperatorStatus(op.RegionID()).Operator)
}

func (suite *operatorControllerTestSuite) TestAddWaitingOperator() {
	opts := mockconfig.NewTestOptions()
	cluster := mockcluster.NewCluster(suite.ctx, opts)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, cluster.ID, cluster, false /* no need to run */)
	controller := NewOperatorController(suite.ctx, cluster, stream)
	cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
	cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})
	addPeerOp := func(i uint64) *operator.Operator {
		start := fmt.Sprintf("%da", i)
		end := fmt.Sprintf("%db", i)
		region := newRegionInfo(i, start, end, 1, 1, []uint64{101, 1}, []uint64{101, 1})
		cluster.PutRegion(region)
		peer := &metapb.Peer{
			StoreId: 2,
		}
		op, err := operator.CreateAddPeerOperator("add-peer", cluster, region, peer, operator.OpKind(0))
		suite.NoError(err)
		suite.NotNil(op)

		return op
	}

	// a batch of operators should be added atomically
	var batch []*operator.Operator
	for i := uint64(0); i < cluster.GetSchedulerMaxWaitingOperator(); i++ {
		batch = append(batch, addPeerOp(i))
	}
	added := controller.AddWaitingOperator(batch...)
	suite.Equal(int(cluster.GetSchedulerMaxWaitingOperator()), added)

	// test adding a batch of operators when some operators will get false in check
	// and remain operators can be added normally
	batch = append(batch, addPeerOp(cluster.GetSchedulerMaxWaitingOperator()))
	added = controller.AddWaitingOperator(batch...)
	suite.Equal(1, added)

	scheduleCfg := opts.GetScheduleConfig().Clone()
	scheduleCfg.SchedulerMaxWaitingOperator = 1
	opts.SetScheduleConfig(scheduleCfg)
	batch = append(batch, addPeerOp(100))
	added = controller.AddWaitingOperator(batch...)
	suite.Equal(1, added)
	suite.NotNil(controller.operators[uint64(100)])

	source := newRegionInfo(101, "1a", "1b", 1, 1, []uint64{101, 1}, []uint64{101, 1})
	cluster.PutRegion(source)
	target := newRegionInfo(102, "0a", "0b", 1, 1, []uint64{101, 1}, []uint64{101, 1})
	cluster.PutRegion(target)

	ops, err := operator.CreateMergeRegionOperator("merge-region", cluster, source, target, operator.OpMerge)
	suite.NoError(err)
	suite.Len(ops, 2)

	// test with label schedule=deny
	labelerManager := cluster.GetRegionLabeler()
	labelerManager.SetLabelRule(&labeler.LabelRule{
		ID:       "schedulelabel",
		Labels:   []labeler.RegionLabel{{Key: "schedule", Value: "deny"}},
		RuleType: labeler.KeyRange,
		Data:     []interface{}{map[string]interface{}{"start_key": "1a", "end_key": "1b"}},
	})

	suite.True(labelerManager.ScheduleDisabled(source))
	// add operator should be failed since it is labeled with `schedule=deny`.
	suite.Equal(0, controller.AddWaitingOperator(ops...))

	// add operator should be success without `schedule=deny`
	labelerManager.DeleteLabelRule("schedulelabel")
	labelerManager.ScheduleDisabled(source)
	suite.False(labelerManager.ScheduleDisabled(source))
	// now there is one operator being allowed to add, if it is a merge operator
	// both of the pair are allowed
	ops, err = operator.CreateMergeRegionOperator("merge-region", cluster, source, target, operator.OpMerge)
	suite.NoError(err)
	suite.Len(ops, 2)
	suite.Equal(2, controller.AddWaitingOperator(ops...))
	suite.Equal(0, controller.AddWaitingOperator(ops...))

	// no space left, new operator can not be added.
	suite.Equal(0, controller.AddWaitingOperator(addPeerOp(0)))
}

// issue #5279
func (suite *operatorControllerTestSuite) TestInvalidStoreId() {
	opt := mockconfig.NewTestOptions()
	tc := mockcluster.NewCluster(suite.ctx, opt)
	stream := hbstream.NewTestHeartbeatStreams(suite.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(suite.ctx, tc, stream)
	// If PD and store 3 are gone, PD will not have info of store 3 after recreating it.
	tc.AddRegionStore(1, 1)
	tc.AddRegionStore(2, 1)
	tc.AddRegionStore(4, 1)
	tc.AddLeaderRegionWithRange(1, "", "", 1, 2, 3, 4)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 3, PeerID: 3, IsDownStore: false},
	}
	op := operator.NewTestOperator(1, &metapb.RegionEpoch{}, operator.OpRegion, steps...)
	suite.True(oc.addOperatorLocked(op))
	// Although store 3 does not exist in PD, PD can also send op to TiKV.
	suite.Equal(pdpb.OperatorStatus_RUNNING, oc.GetOperatorStatus(1).Status)
}
