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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
)

const (
	// TransferWitnessLeaderName is transfer witness leader scheduler name.
	TransferWitnessLeaderName = "transfer-witness-leader-scheduler"
	// TransferWitnessLeaderType is transfer witness leader scheduler type.
	TransferWitnessLeaderType = "transfer-witness-leader"
	// TransferWitnessLeaderBatchSize is the number of operators to to transfer
	// leaders by one scheduling
	transferWitnessLeaderBatchSize = 3
	// TransferWitnessLeaderRecvMaxRegionSize is the max number of region can receive
	// TODO: make it a reasonable value
	transferWitnessLeaderRecvMaxRegionSize = 10000
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	transferWitnessLeaderCounter              = schedulerCounter.WithLabelValues(TransferWitnessLeaderName, "schedule")
	transferWitnessLeaderNewOperatorCounter   = schedulerCounter.WithLabelValues(TransferWitnessLeaderName, "new-operator")
	transferWitnessLeaderNoTargetStoreCounter = schedulerCounter.WithLabelValues(TransferWitnessLeaderName, "no-target-store")
)

type trasferWitnessLeaderScheduler struct {
	*BaseScheduler
	regions chan *core.RegionInfo
}

// newTransferWitnessLeaderScheduler creates an admin scheduler that transfers witness leader of a region.
func newTransferWitnessLeaderScheduler(opController *schedule.OperatorController) schedule.Scheduler {
	return &trasferWitnessLeaderScheduler{
		BaseScheduler: NewBaseScheduler(opController),
		regions:       make(chan *core.RegionInfo, transferWitnessLeaderRecvMaxRegionSize),
	}
}

func (s *trasferWitnessLeaderScheduler) GetName() string {
	return TransferWitnessLeaderName
}

func (s *trasferWitnessLeaderScheduler) GetType() string {
	return TransferWitnessLeaderType
}

func (s *trasferWitnessLeaderScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return true
}

func (s *trasferWitnessLeaderScheduler) Schedule(cluster schedule.Cluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	transferWitnessLeaderCounter.Inc()
	return s.scheduleTransferWitnessLeaderBatch(s.GetName(), s.GetType(), cluster, transferWitnessLeaderBatchSize), nil
}

func (s *trasferWitnessLeaderScheduler) scheduleTransferWitnessLeaderBatch(name, typ string, cluster schedule.Cluster, batchSize int) []*operator.Operator {
	var ops []*operator.Operator
	for i := 0; i < batchSize; i++ {
		select {
		case region := <-s.regions:
			op, err := s.scheduleTransferWitnessLeader(name, typ, cluster, region)
			if err != nil {
				log.Debug("fail to create transfer leader operator", errs.ZapError(err))
				continue
			}
			if op != nil {
				op.SetPriorityLevel(constant.Urgent)
				op.Counters = append(op.Counters, transferWitnessLeaderNewOperatorCounter)
				ops = append(ops, op)
			}
		default:
			break
		}
	}
	return ops
}

func (s *trasferWitnessLeaderScheduler) scheduleTransferWitnessLeader(name, typ string, cluster schedule.Cluster, region *core.RegionInfo) (*operator.Operator, error) {
	var filters []filter.Filter
	unhealthyPeerStores := make(map[uint64]struct{})
	for _, peer := range region.GetDownPeers() {
		unhealthyPeerStores[peer.GetPeer().GetStoreId()] = struct{}{}
	}
	for _, peer := range region.GetPendingPeers() {
		unhealthyPeerStores[peer.GetStoreId()] = struct{}{}
	}
	filters = append(filters, filter.NewExcludedFilter(name, nil, unhealthyPeerStores), &filter.StoreStateFilter{ActionScope: name, TransferLeader: true})
	candidates := filter.NewCandidates(cluster.GetFollowerStores(region)).FilterTarget(cluster.GetOpts(), nil, nil, filters...)
	// Compatible with old TiKV transfer leader logic.
	target := candidates.RandomPick()
	targets := candidates.PickAll()
	// `targets` MUST contains `target`, so only needs to check if `target` is nil here.
	if target == nil {
		transferWitnessLeaderNoTargetStoreCounter.Inc()
		return nil, errors.New("no target store to schedule")
	}
	targetIDs := make([]uint64, 0, len(targets))
	for _, t := range targets {
		targetIDs = append(targetIDs, t.GetID())
	}
	return operator.CreateTransferLeaderOperator(typ, cluster, region, region.GetLeader().GetStoreId(), target.GetID(), targetIDs, operator.OpWitnessLeader)
}

// RecvRegionInfo receives a checked region from coordinator
func RecvRegionInfo(s schedule.Scheduler) chan<- *core.RegionInfo {
	return s.(*trasferWitnessLeaderScheduler).regions
}
