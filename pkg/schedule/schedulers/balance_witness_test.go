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

package schedulers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/storage"
)

func TestBalanceWitnessSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(balanceWitnessSchedulerTestSuite))
}

type balanceWitnessSchedulerTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	lb     schedule.Scheduler
	oc     *schedule.OperatorController
	conf   config.Config
}

func (suite *balanceWitnessSchedulerTestSuite) SetupTest() {
	suite.cancel, suite.conf, suite.tc, suite.oc = prepareSchedulersTest()
	suite.tc.RuleManager.SetRules([]*placement.Rule{
		{
			GroupID: "pd",
			ID:      "default",
			Role:    placement.Voter,
			Count:   4,
		},
	})
	lb, err := schedule.CreateScheduler(BalanceWitnessType, suite.oc, storage.NewStorageWithMemoryBackend(), schedule.ConfigSliceDecoder(BalanceWitnessType, []string{"", ""}))
	suite.NoError(err)
	suite.lb = lb
}

func (suite *balanceWitnessSchedulerTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *balanceWitnessSchedulerTestSuite) schedule() []*operator.Operator {
	ops, _ := suite.lb.Schedule(suite.tc, false)
	return ops
}

func (suite *balanceWitnessSchedulerTestSuite) TestScheduleWithOpInfluence() {
	suite.tc.SetTolerantSizeRatio(2.5)
	// Stores:     1    2    3    4
	// Witnesses:  7    8    9    14
	// Region1:    F    F    F    L
	suite.tc.AddWitnessStore(1, 7)
	suite.tc.AddWitnessStore(2, 8)
	suite.tc.AddWitnessStore(3, 9)
	suite.tc.AddWitnessStore(4, 14)
	suite.tc.AddLeaderRegionWithWitness(1, 3, []uint64{1, 2, 4}, 4)
	op := suite.schedule()[0]
	suite.NotNil(op)
	suite.oc.SetOperator(op)
	// After considering the scheduled operator, witnesses of store1 and store2 are 8
	// and 13 respectively. As the `TolerantSizeRatio` is 2.5, `shouldBalance`
	// returns false when witness difference is not greater than 5.
	suite.NotEmpty(suite.schedule())

	// Stores:     1    2    3    4
	// Witness:    8    8    9    13
	// Region1:    F    F    F    L
	suite.tc.UpdateWitnessCount(1, 8)
	suite.tc.UpdateWitnessCount(2, 8)
	suite.tc.UpdateWitnessCount(3, 9)
	suite.tc.UpdateWitnessCount(4, 13)
	suite.tc.AddLeaderRegionWithWitness(1, 3, []uint64{1, 2, 4}, 4)
	suite.Empty(suite.schedule())
}

func (suite *balanceWitnessSchedulerTestSuite) TestTransferWitnessOut() {
	// Stores:     1    2    3    4
	// Witnesses:  7    8    9   12
	suite.tc.AddWitnessStore(1, 7)
	suite.tc.AddWitnessStore(2, 8)
	suite.tc.AddWitnessStore(3, 9)
	suite.tc.AddWitnessStore(4, 12)
	suite.tc.SetTolerantSizeRatio(0.1)
	for i := uint64(1); i <= 7; i++ {
		suite.tc.AddLeaderRegionWithWitness(i, 3, []uint64{1, 2, 4}, 4)
	}

	// balance witness: 4->1, 4->1, 4->2
	regions := make(map[uint64]struct{})
	targets := map[uint64]uint64{
		1: 2,
		2: 1,
	}
	for i := 0; i < 20; i++ {
		if len(suite.schedule()) == 0 {
			continue
		}
		if op := suite.schedule()[0]; op != nil {
			if _, ok := regions[op.RegionID()]; !ok {
				suite.oc.SetOperator(op)
				regions[op.RegionID()] = struct{}{}
				from := op.Step(0).(operator.ChangePeerV2Enter).DemoteVoters[0].ToStore
				to := op.Step(1).(operator.BatchSwitchWitness).ToWitnesses[0].StoreID
				suite.Equal(from, uint64(4))
				targets[to]--
			}
		}
	}
	suite.Equal(3, len(regions))
	for _, count := range targets {
		suite.Zero(count)
	}
}
