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

package cases

import (
	"math/rand"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newAddNodesDynamic() *Case {
	var simCase Case

	storeNum, regionNum := getStoreNum(), getRegionNum()
	noEmptyRatio := rand.Float64() // the ratio of noEmpty store to total store
	noEmptyStoreNum := getNoEmptyStoreNum(storeNum, noEmptyRatio)

	for i := 1; i <= int(noEmptyStoreNum); i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     IDAllocator.nextID(),
			Status: metapb.StoreState_Up,
		})
	}

	var ids []uint64
	for i := 1; i <= storeNum-int(noEmptyStoreNum); i++ {
		ids = append(ids, IDAllocator.nextID())
	}

	for i := 0; i < regionNum*storeNum/3; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i)%noEmptyStoreNum + 1},
			{Id: IDAllocator.nextID(), StoreId: uint64(i+1)%noEmptyStoreNum + 1},
			{Id: IDAllocator.nextID(), StoreId: uint64(i+2)%noEmptyStoreNum + 1},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * units.MiB,
			Keys:   960000,
		})
	}

	numNodes := int(noEmptyStoreNum)
	e := &AddNodesDescriptor{}
	e.Step = func(tick int64) uint64 {
		if tick%100 == 0 && numNodes < storeNum {
			numNodes++
			nodeID := ids[0]
			ids = append(ids[:0], ids[1:]...)
			return nodeID
		}
		return 0
	}
	simCase.Events = []EventDescriptor{e}

	threshold := 0.05
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		res := numNodes == storeNum
		leaderCounts := make([]int, 0, numNodes)
		regionCounts := make([]int, 0, numNodes)
		for i := 1; i <= numNodes; i++ {
			leaderCount := regions.GetStoreLeaderCount(uint64(i))
			regionCount := regions.GetStoreRegionCount(uint64(i))
			leaderCounts = append(leaderCounts, leaderCount)
			regionCounts = append(regionCounts, regionCount)
			res = res && leaderAndRegionIsUniform(leaderCount, regionCount, regionNum, threshold)
		}

		simutil.Logger.Info("current counts", zap.Ints("leader", leaderCounts), zap.Ints("region", regionCounts))
		return res
	}
	return &simCase
}
