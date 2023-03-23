// Copyright 2017 TiKV Project Authors.
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

func newAddNodes() *Case {
	var simCase Case

	storeNum, regionNum := getStoreNum(), getRegionNum()
	noEmptyRatio := rand.Float64() // the ratio of noEmpty store to total store
	noEmptyStoreNum := getNoEmptyStoreNum(storeNum, noEmptyRatio)

	for i := 1; i <= storeNum; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     IDAllocator.nextID(),
			Status: metapb.StoreState_Up,
		})
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

	threshold := 0.05
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		res := true
		leaderCounts := make([]int, 0, storeNum)
		regionCounts := make([]int, 0, storeNum)
		for i := 1; i <= storeNum; i++ {
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
