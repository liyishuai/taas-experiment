// Copyright 2021 TiKV Project Authors.
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

package statistics

import (
	"math"

	"github.com/tikv/pd/pkg/core"
)

// StoreLoadDetail records store load information.
type StoreLoadDetail struct {
	*StoreSummaryInfo
	LoadPred *StoreLoadPred
	HotPeers []*HotPeerStat
}

// ToHotPeersStat abstracts load information to HotPeersStat.
func (li *StoreLoadDetail) ToHotPeersStat() *HotPeersStat {
	storeByteRate, storeKeyRate, storeQueryRate := li.LoadPred.Current.Loads[ByteDim],
		li.LoadPred.Current.Loads[KeyDim], li.LoadPred.Current.Loads[QueryDim]
	if len(li.HotPeers) == 0 {
		return &HotPeersStat{
			StoreByteRate:  storeByteRate,
			StoreKeyRate:   storeKeyRate,
			StoreQueryRate: storeQueryRate,
			TotalBytesRate: 0.0,
			TotalKeysRate:  0.0,
			TotalQueryRate: 0.0,
			Count:          0,
			Stats:          make([]HotPeerStatShow, 0),
		}
	}
	var byteRate, keyRate, queryRate float64
	peers := make([]HotPeerStatShow, 0, len(li.HotPeers))
	for _, peer := range li.HotPeers {
		if peer.HotDegree > 0 {
			peers = append(peers, toHotPeerStatShow(peer))
			byteRate += peer.GetLoad(ByteDim)
			keyRate += peer.GetLoad(KeyDim)
			queryRate += peer.GetLoad(QueryDim)
		}
	}

	return &HotPeersStat{
		TotalBytesRate: byteRate,
		TotalKeysRate:  keyRate,
		TotalQueryRate: queryRate,
		StoreByteRate:  storeByteRate,
		StoreKeyRate:   storeKeyRate,
		StoreQueryRate: storeQueryRate,
		Count:          len(peers),
		Stats:          peers,
	}
}

// IsUniform returns true if the stores are uniform.
func (li *StoreLoadDetail) IsUniform(dim int, threshold float64) bool {
	return li.LoadPred.Stddev.Loads[dim] < threshold
}

func toHotPeerStatShow(p *HotPeerStat) HotPeerStatShow {
	byteRate := p.GetLoad(ByteDim)
	keyRate := p.GetLoad(KeyDim)
	queryRate := p.GetLoad(QueryDim)
	return HotPeerStatShow{
		StoreID:   p.StoreID,
		Stores:    p.GetStores(),
		IsLeader:  p.IsLeader(),
		RegionID:  p.RegionID,
		HotDegree: p.HotDegree,
		ByteRate:  byteRate,
		KeyRate:   keyRate,
		QueryRate: queryRate,
		AntiCount: p.AntiCount,
	}
}

// StoreSummaryInfo records the summary information of store.
type StoreSummaryInfo struct {
	*core.StoreInfo
	isTiFlash  bool
	PendingSum *Influence
}

// Influence records operator influence.
type Influence struct {
	Loads []float64
	Count float64
}

// SummaryStoreInfos return a mapping from store to summary information.
func SummaryStoreInfos(stores []*core.StoreInfo) map[uint64]*StoreSummaryInfo {
	infos := make(map[uint64]*StoreSummaryInfo, len(stores))
	for _, store := range stores {
		info := &StoreSummaryInfo{
			StoreInfo:  store,
			isTiFlash:  store.IsTiFlash(),
			PendingSum: nil,
		}
		infos[store.GetID()] = info
	}
	return infos
}

// AddInfluence adds influence to pending sum.
func (s *StoreSummaryInfo) AddInfluence(infl *Influence, w float64) {
	if infl == nil || w == 0 {
		return
	}
	if s.PendingSum == nil {
		s.PendingSum = &Influence{
			Loads: make([]float64, len(infl.Loads)),
			Count: 0,
		}
	}
	for i, load := range infl.Loads {
		s.PendingSum.Loads[i] += load * w
	}
	s.PendingSum.Count += infl.Count * w
}

// IsTiFlash returns true if the store is TiFlash.
func (s *StoreSummaryInfo) IsTiFlash() bool {
	return s.isTiFlash
}

// SetEngineAsTiFlash set whether store is TiFlash, it is only used in tests.
func (s *StoreSummaryInfo) SetEngineAsTiFlash() {
	s.isTiFlash = true
}

// StoreLoad records the current load.
type StoreLoad struct {
	Loads []float64
	Count float64
}

// ToLoadPred returns the current load and future predictive load.
func (load StoreLoad) ToLoadPred(rwTy RWType, infl *Influence) *StoreLoadPred {
	future := StoreLoad{
		Loads: append(load.Loads[:0:0], load.Loads...),
		Count: load.Count,
	}
	if infl != nil {
		switch rwTy {
		case Read:
			future.Loads[ByteDim] += infl.Loads[RegionReadBytes]
			future.Loads[KeyDim] += infl.Loads[RegionReadKeys]
			future.Loads[QueryDim] += infl.Loads[RegionReadQueryNum]
		case Write:
			future.Loads[ByteDim] += infl.Loads[RegionWriteBytes]
			future.Loads[KeyDim] += infl.Loads[RegionWriteKeys]
			future.Loads[QueryDim] += infl.Loads[RegionWriteQueryNum]
		}
		future.Count += infl.Count
	}
	return &StoreLoadPred{
		Current: load,
		Future:  future,
	}
}

// StoreLoadPred is a prediction of a store.
type StoreLoadPred struct {
	Current StoreLoad
	Future  StoreLoad
	Expect  StoreLoad
	Stddev  StoreLoad
}

// Min returns the min load between current and future.
func (lp *StoreLoadPred) Min() *StoreLoad {
	return MinLoad(&lp.Current, &lp.Future)
}

// Max returns the max load between current and future.
func (lp *StoreLoadPred) Max() *StoreLoad {
	return MaxLoad(&lp.Current, &lp.Future)
}

// Pending returns the pending load.
func (lp *StoreLoadPred) Pending() *StoreLoad {
	mx, mn := lp.Max(), lp.Min()
	loads := make([]float64, len(mx.Loads))
	for i := range loads {
		loads[i] = mx.Loads[i] - mn.Loads[i]
	}
	return &StoreLoad{
		Loads: loads,
		Count: 0,
	}
}

// Diff return the difference between min and max.
func (lp *StoreLoadPred) Diff() *StoreLoad {
	mx, mn := lp.Max(), lp.Min()
	loads := make([]float64, len(mx.Loads))
	for i := range loads {
		loads[i] = mx.Loads[i] - mn.Loads[i]
	}
	return &StoreLoad{
		Loads: loads,
		Count: mx.Count - mn.Count,
	}
}

// MinLoad return the min store load.
func MinLoad(a, b *StoreLoad) *StoreLoad {
	loads := make([]float64, len(a.Loads))
	for i := range loads {
		loads[i] = math.Min(a.Loads[i], b.Loads[i])
	}
	return &StoreLoad{
		Loads: loads,
		Count: math.Min(a.Count, b.Count),
	}
}

// MaxLoad return the max store load.
func MaxLoad(a, b *StoreLoad) *StoreLoad {
	loads := make([]float64, len(a.Loads))
	for i := range loads {
		loads[i] = math.Max(a.Loads[i], b.Loads[i])
	}
	return &StoreLoad{
		Loads: loads,
		Count: math.Max(a.Count, b.Count),
	}
}
