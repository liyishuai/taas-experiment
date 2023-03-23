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

package cases

import (
	"fmt"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
)

func newLabelNotMatch1() *Case {
	var simCase Case
	simCase.Labels = []string{"host"}

	num1, num2 := 3, 1
	storeNum, regionNum := num1+num2, 200
	for i := 0; i < num1; i++ {
		id := IDAllocator.nextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{{Key: "host", Value: fmt.Sprintf("host%d", id)}},
		})
	}
	simCase.Stores = append(simCase.Stores, &Store{
		ID:     IDAllocator.nextID(),
		Status: metapb.StoreState_Up,
	})

	for i := 0; i < regionNum; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%num1 + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%num1 + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%num1 + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * units.MiB,
			Keys:   960000,
		})
	}

	storesLastUpdateTime := make([]int64, storeNum+1)
	storeLastAvailable := make([]uint64, storeNum+1)
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		res := true
		curTime := time.Now().Unix()
		storesAvailable := make([]uint64, 0, storeNum+1)
		for i := 1; i <= storeNum; i++ {
			available := stats[i].GetAvailable()
			storesAvailable = append(storesAvailable, available)
			if curTime-storesLastUpdateTime[i] > 360 {
				if storeLastAvailable[i] != available {
					res = false
				}
				if stats[i].ToCompactionSize != 0 {
					res = false
				}
				storesLastUpdateTime[i] = curTime
				storeLastAvailable[i] = available
			} else {
				res = false
			}
		}
		simutil.Logger.Info("current counts", zap.Uint64s("storesAvailable", storesAvailable))
		return res
	}
	return &simCase
}

func newLabelIsolation1() *Case {
	var simCase Case
	simCase.Labels = []string{"host"}

	num1, num2 := 2, 2
	storeNum, regionNum := num1+num2, 300
	for i := 0; i < num1; i++ {
		id := IDAllocator.nextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{{Key: "host", Value: fmt.Sprintf("host%d", id)}},
		})
	}
	id := IDAllocator.GetID() + 1
	for i := 0; i < num2; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     IDAllocator.nextID(),
			Status: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{{Key: "host", Value: fmt.Sprintf("host%d", id)}},
		})
	}

	for i := 0; i < regionNum; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%num1 + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%num1 + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64(i%num2 + num1 + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * units.MiB,
			Keys:   960000,
		})
	}

	storesLastUpdateTime := make([]int64, storeNum+1)
	storeLastAvailable := make([]uint64, storeNum+1)
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		res := true
		curTime := time.Now().Unix()
		storesAvailable := make([]uint64, 0, storeNum+1)
		for i := 1; i <= storeNum; i++ {
			available := stats[i].GetAvailable()
			storesAvailable = append(storesAvailable, available)
			if curTime-storesLastUpdateTime[i] > 360 {
				if storeLastAvailable[i] != available {
					res = false
				}
				if stats[i].ToCompactionSize != 0 {
					res = false
				}
				storesLastUpdateTime[i] = curTime
				storeLastAvailable[i] = available
			} else {
				res = false
			}
		}
		simutil.Logger.Info("current counts", zap.Uint64s("storesAvailable", storesAvailable))
		return res
	}
	return &simCase
}

func newLabelIsolation2() *Case {
	var simCase Case
	simCase.Labels = []string{"dc", "zone", "host"}

	storeNum, regionNum := 5, 200
	for i := 0; i < storeNum; i++ {
		id := IDAllocator.nextID()
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     id,
			Status: metapb.StoreState_Up,
		})
	}
	simCase.Stores[0].Labels = []*metapb.StoreLabel{{Key: "dc", Value: "dc1"}, {Key: "zone", Value: "zone1"}, {Key: "host", Value: "host1"}}
	simCase.Stores[1].Labels = []*metapb.StoreLabel{{Key: "dc", Value: "dc1"}, {Key: "zone", Value: "zone1"}, {Key: "host", Value: "host2"}}
	simCase.Stores[2].Labels = []*metapb.StoreLabel{{Key: "dc", Value: "dc1"}, {Key: "zone", Value: "zone2"}, {Key: "host", Value: "host3"}}
	simCase.Stores[3].Labels = []*metapb.StoreLabel{{Key: "dc", Value: "dc1"}, {Key: "zone", Value: "zone2"}, {Key: "host", Value: "host4"}}
	simCase.Stores[4].Labels = []*metapb.StoreLabel{{Key: "dc", Value: "dc1"}, {Key: "zone", Value: "zone3"}, {Key: "host", Value: "host5"}}

	for i := 0; i < regionNum; i++ {
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(i%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+1)%storeNum + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64((i+2)%storeNum + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   96 * units.MiB,
			Keys:   960000,
		})
	}

	storesLastUpdateTime := make([]int64, storeNum+1)
	storeLastAvailable := make([]uint64, storeNum+1)
	simCase.Checker = func(regions *core.RegionsInfo, stats []info.StoreStats) bool {
		res := true
		curTime := time.Now().Unix()
		storesAvailable := make([]uint64, 0, storeNum+1)
		for i := 1; i <= storeNum; i++ {
			available := stats[i].GetAvailable()
			storesAvailable = append(storesAvailable, available)
			if curTime-storesLastUpdateTime[i] > 360 {
				if storeLastAvailable[i] != available {
					res = false
				}
				if stats[i].ToCompactionSize != 0 {
					res = false
				}
				storesLastUpdateTime[i] = curTime
				storeLastAvailable[i] = available
			} else {
				res = false
			}
		}
		simutil.Logger.Info("current counts", zap.Uint64s("storesAvailable", storesAvailable))
		return res
	}
	return &simCase
}
