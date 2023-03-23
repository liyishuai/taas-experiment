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

package statistics

import (
	"context"
	"math"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/smallnest/chanx"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/slice"
)

const (
	// TopNN is the threshold which means we can get hot threshold from store.
	TopNN = 60
	// HotThresholdRatio is used to calculate hot thresholds
	HotThresholdRatio = 0.8
	// WriteReportInterval indicates the interval between write interval
	WriteReportInterval = RegionHeartBeatReportInterval
	// ReadReportInterval indicates the interval between read stats report
	ReadReportInterval = StoreHeartBeatReportInterval

	rollingWindowsSize = 5

	// HotRegionReportMinInterval is used for the simulator and test
	HotRegionReportMinInterval = 3

	// HotRegionAntiCount is default value for antiCount
	HotRegionAntiCount = 2

	queueCap = 20000
)

// ThresholdsUpdateInterval is the default interval to update thresholds.
// the refresh interval should be less than store heartbeat interval to keep the next calculate must use the latest threshold.
var ThresholdsUpdateInterval = 8 * time.Second

// Denoising is an option to calculate flow base on the real heartbeats. Should
// only turn off by the simulator and the test.
var Denoising = true

// MinHotThresholds is the threshold at which this dimension is recorded as a hot spot.
var MinHotThresholds = [RegionStatCount]float64{
	RegionReadBytes:     8 * units.KiB,
	RegionReadKeys:      128,
	RegionReadQueryNum:  128,
	RegionWriteBytes:    1 * units.KiB,
	RegionWriteKeys:     32,
	RegionWriteQueryNum: 32,
}

type thresholds struct {
	updatedTime time.Time
	rates       []float64
	topNLen     int
	metrics     [DimLen + 1]prometheus.Gauge // 0 is for byte, 1 is for key, 2 is for query, 3 is for total length.
}

// hotPeerCache saves the hot peer's statistics.
type hotPeerCache struct {
	kind              RWType
	peersOfStore      map[uint64]*TopN               // storeID -> hot peers
	storesOfRegion    map[uint64]map[uint64]struct{} // regionID -> storeIDs
	regionsOfStore    map[uint64]map[uint64]struct{} // storeID -> regionIDs
	topNTTL           time.Duration
	taskQueue         *chanx.UnboundedChan[FlowItemTask]
	thresholdsOfStore map[uint64]*thresholds                     // storeID -> thresholds
	metrics           map[uint64][ActionTypeLen]prometheus.Gauge // storeID -> metrics
	// TODO: consider to remove store info when store is offline.
}

// NewHotPeerCache creates a hotPeerCache
func NewHotPeerCache(ctx context.Context, kind RWType) *hotPeerCache {
	return &hotPeerCache{
		kind:              kind,
		peersOfStore:      make(map[uint64]*TopN),
		storesOfRegion:    make(map[uint64]map[uint64]struct{}),
		regionsOfStore:    make(map[uint64]map[uint64]struct{}),
		taskQueue:         chanx.NewUnboundedChan[FlowItemTask](ctx, queueCap),
		thresholdsOfStore: make(map[uint64]*thresholds),
		topNTTL:           time.Duration(3*kind.ReportInterval()) * time.Second,
		metrics:           make(map[uint64][ActionTypeLen]prometheus.Gauge),
	}
}

// TODO: rename RegionStats as PeerStats
// RegionStats returns hot items
func (f *hotPeerCache) RegionStats(minHotDegree int) map[uint64][]*HotPeerStat {
	res := make(map[uint64][]*HotPeerStat)
	defaultAntiCount := f.kind.DefaultAntiCount()
	for storeID, peers := range f.peersOfStore {
		values := peers.GetAll()
		stat := make([]*HotPeerStat, 0, len(values))
		for _, v := range values {
			if peer := v.(*HotPeerStat); peer.HotDegree >= minHotDegree && !peer.inCold && peer.AntiCount == defaultAntiCount {
				stat = append(stat, peer)
			}
		}
		res[storeID] = stat
	}
	return res
}

func (f *hotPeerCache) updateStat(item *HotPeerStat) {
	switch item.actionType {
	case Remove:
		f.removeItem(item)
		item.Log("region heartbeat remove from cache")
	case Add, Update:
		f.putItem(item)
		item.Log("region heartbeat update")
	default:
		return
	}
	f.incMetrics(item.actionType, item.StoreID)
}

func (f *hotPeerCache) incMetrics(action ActionType, storeID uint64) {
	if _, ok := f.metrics[storeID]; !ok {
		store := storeTag(storeID)
		kind := f.kind.String()
		f.metrics[storeID] = [ActionTypeLen]prometheus.Gauge{
			Add:    hotCacheStatusGauge.WithLabelValues("add_item", store, kind),
			Remove: hotCacheStatusGauge.WithLabelValues("remove_item", store, kind),
			Update: hotCacheStatusGauge.WithLabelValues("update_item", store, kind),
		}
	}
	f.metrics[storeID][action].Inc()
}

func (f *hotPeerCache) collectPeerMetrics(loads []float64, interval uint64) {
	regionHeartbeatIntervalHist.Observe(float64(interval))
	if interval == 0 {
		return
	}
	// TODO: use unified metrics. (keep backward compatibility at the same time)
	for _, k := range f.kind.RegionStats() {
		switch k {
		case RegionReadBytes:
			readByteHist.Observe(loads[int(k)])
		case RegionReadKeys:
			readKeyHist.Observe(loads[int(k)])
		case RegionWriteBytes:
			writeByteHist.Observe(loads[int(k)])
		case RegionWriteKeys:
			writeKeyHist.Observe(loads[int(k)])
		case RegionWriteQueryNum:
			writeQueryHist.Observe(loads[int(k)])
		case RegionReadQueryNum:
			readQueryHist.Observe(loads[int(k)])
		}
	}
}

// collectExpiredItems collects expired items, mark them as needDelete and puts them into inherit items
func (f *hotPeerCache) collectExpiredItems(region *core.RegionInfo) []*HotPeerStat {
	regionID := region.GetID()
	items := make([]*HotPeerStat, 0)
	if ids, ok := f.storesOfRegion[regionID]; ok {
		for storeID := range ids {
			if region.GetStorePeer(storeID) == nil {
				item := f.getOldHotPeerStat(regionID, storeID)
				if item != nil {
					item.actionType = Remove
					items = append(items, item)
				}
			}
		}
	}
	return items
}

// checkPeerFlow checks the flow information of a peer.
// Notice: checkPeerFlow couldn't be used concurrently.
// checkPeerFlow will update oldItem's rollingLoads into newItem, thus we should use write lock here.
func (f *hotPeerCache) checkPeerFlow(peer *core.PeerInfo, region *core.RegionInfo) *HotPeerStat {
	interval := peer.GetInterval()
	if Denoising && interval < HotRegionReportMinInterval { // for test or simulator purpose
		return nil
	}
	storeID := peer.GetStoreId()
	deltaLoads := peer.GetLoads()
	f.collectPeerMetrics(deltaLoads, interval) // update metrics
	regionID := region.GetID()
	oldItem := f.getOldHotPeerStat(regionID, storeID)

	// check whether the peer is allowed to be inherited
	source := direct
	if oldItem == nil {
		for _, storeID := range f.getAllStoreIDs(region) {
			oldItem = f.getOldHotPeerStat(regionID, storeID)
			if oldItem != nil && oldItem.allowInherited {
				source = inherit
				break
			}
		}
	}

	// check new item whether is hot
	if oldItem == nil {
		regionStats := f.kind.RegionStats()
		thresholds := f.calcHotThresholds(storeID)
		isHot := slice.AnyOf(regionStats, func(i int) bool {
			return deltaLoads[regionStats[i]]/float64(interval) >= thresholds[i]
		})
		if !isHot {
			return nil
		}
	}

	peers := region.GetPeers()
	newItem := &HotPeerStat{
		StoreID:    storeID,
		RegionID:   regionID,
		Loads:      f.kind.GetLoadRatesFromPeer(peer),
		isLeader:   region.GetLeader().GetStoreId() == storeID,
		actionType: Update,
		stores:     make([]uint64, len(peers)),
	}
	for _, peer := range peers {
		newItem.stores = append(newItem.stores, peer.GetStoreId())
	}

	if oldItem == nil {
		return f.updateNewHotPeerStat(newItem, deltaLoads, time.Duration(interval)*time.Second)
	}
	return f.updateHotPeerStat(region, newItem, oldItem, deltaLoads, time.Duration(interval)*time.Second, source)
}

// checkColdPeer checks the collect the un-heartbeat peer and maintain it.
func (f *hotPeerCache) checkColdPeer(storeID uint64, reportRegions map[uint64]*core.RegionInfo, interval uint64) (ret []*HotPeerStat) {
	// for test or simulator purpose
	if Denoising && interval < HotRegionReportMinInterval {
		return
	}
	previousHotStat, ok := f.regionsOfStore[storeID]
	// There is no need to continue since the store doesn't have any hot regions.
	if !ok {
		return
	}
	// Check if the original hot regions are still reported by the store heartbeat.
	for regionID := range previousHotStat {
		// If it's not reported, we need to update the original information.
		if region, ok := reportRegions[regionID]; !ok {
			oldItem := f.getOldHotPeerStat(regionID, storeID)
			// The region is not hot in the store, do nothing.
			if oldItem == nil {
				continue
			}

			// update the original hot peer, and mark it as cold.
			newItem := &HotPeerStat{
				StoreID:  storeID,
				RegionID: regionID,
				// use 0 to make the cold newItem won't affect the loads.
				Loads:      make([]float64, len(oldItem.Loads)),
				isLeader:   oldItem.isLeader,
				actionType: Update,
				inCold:     true,
				stores:     oldItem.stores,
			}
			deltaLoads := make([]float64, RegionStatCount)
			thresholds := f.calcHotThresholds(storeID)
			source := direct
			for i, loads := range thresholds {
				deltaLoads[i] = loads * float64(interval)
			}
			stat := f.updateHotPeerStat(region, newItem, oldItem, deltaLoads, time.Duration(interval)*time.Second, source)
			if stat != nil {
				ret = append(ret, stat)
			}
		}
	}
	return
}

func (f *hotPeerCache) collectMetrics() {
	for _, thresholds := range f.thresholdsOfStore {
		thresholds.metrics[ByteDim].Set(thresholds.rates[ByteDim])
		thresholds.metrics[KeyDim].Set(thresholds.rates[KeyDim])
		thresholds.metrics[QueryDim].Set(thresholds.rates[QueryDim])
		thresholds.metrics[DimLen].Set(float64(thresholds.topNLen))
	}
}

func (f *hotPeerCache) getOldHotPeerStat(regionID, storeID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfStore[storeID]; ok {
		if v := hotPeers.Get(regionID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) calcHotThresholds(storeID uint64) []float64 {
	// check whether the thresholds is updated recently
	t, ok := f.thresholdsOfStore[storeID]
	if ok && time.Since(t.updatedTime) <= ThresholdsUpdateInterval {
		return t.rates
	}
	// if no exist, or the thresholds is outdated, we need to update it.
	if !ok {
		store := storeTag(storeID)
		kind := f.kind.String()
		t = &thresholds{
			rates: make([]float64, DimLen),
			metrics: [DimLen + 1]prometheus.Gauge{
				ByteDim:  hotCacheStatusGauge.WithLabelValues("byte-rate-threshold", store, kind),
				KeyDim:   hotCacheStatusGauge.WithLabelValues("key-rate-threshold", store, kind),
				QueryDim: hotCacheStatusGauge.WithLabelValues("query-rate-threshold", store, kind),
				DimLen:   hotCacheStatusGauge.WithLabelValues("total_length", store, kind),
			},
		}
	}
	// update the thresholds
	f.thresholdsOfStore[storeID] = t
	t.updatedTime = time.Now()
	statKinds := f.kind.RegionStats()
	for dim, kind := range statKinds {
		t.rates[dim] = MinHotThresholds[kind]
	}
	if tn, ok := f.peersOfStore[storeID]; ok {
		t.topNLen = tn.Len()
		if t.topNLen < TopNN {
			return t.rates
		}
		for i := range t.rates {
			t.rates[i] = math.Max(tn.GetTopNMin(i).(*HotPeerStat).GetLoad(i)*HotThresholdRatio, t.rates[i])
		}
	}
	return t.rates
}

// gets the storeIDs, including old region and new region
func (f *hotPeerCache) getAllStoreIDs(region *core.RegionInfo) []uint64 {
	regionPeers := region.GetPeers()
	ret := make([]uint64, 0, len(regionPeers))
	isInSlice := func(id uint64) bool {
		for _, storeID := range ret {
			if storeID == id {
				return true
			}
		}
		return false
	}
	// old stores
	if ids, ok := f.storesOfRegion[region.GetID()]; ok {
		for storeID := range ids {
			ret = append(ret, storeID)
		}
	}
	// new stores
	for _, peer := range regionPeers {
		storeID := peer.GetStoreId()
		if isInSlice(storeID) {
			continue
		}
		ret = append(ret, storeID)
	}
	return ret
}

func (f *hotPeerCache) isOldColdPeer(oldItem *HotPeerStat, storeID uint64) bool {
	isOldPeer := func() bool {
		for _, id := range oldItem.stores {
			if id == storeID {
				return true
			}
		}
		return false
	}
	isInHotCache := func() bool {
		if ids, ok := f.storesOfRegion[oldItem.RegionID]; ok {
			if _, ok := ids[storeID]; ok {
				return true
			}
		}
		return false
	}
	return isOldPeer() && !isInHotCache()
}

func (f *hotPeerCache) justTransferLeader(region *core.RegionInfo, oldItem *HotPeerStat) bool {
	if region == nil {
		return false
	}
	if oldItem.isLeader { // old item is not nil according to the function
		return oldItem.StoreID != region.GetLeader().GetStoreId()
	}
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
			if oldItem == nil {
				continue
			}
			if oldItem.isLeader {
				return oldItem.StoreID != region.GetLeader().GetStoreId()
			}
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithAnyPeers(region *core.RegionInfo, hotDegree int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeer(region, peer, hotDegree) {
			return true
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithPeer(region *core.RegionInfo, peer *metapb.Peer, hotDegree int) bool {
	if peer == nil {
		return false
	}
	if stat := f.getHotPeerStat(region.GetID(), peer.GetStoreId()); stat != nil {
		return stat.HotDegree >= hotDegree
	}
	return false
}

func (f *hotPeerCache) getHotPeerStat(regionID, storeID uint64) *HotPeerStat {
	if peers, ok := f.peersOfStore[storeID]; ok {
		if stat := peers.Get(regionID); stat != nil {
			return stat.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) updateHotPeerStat(region *core.RegionInfo, newItem, oldItem *HotPeerStat, deltaLoads []float64, interval time.Duration, source sourceKind) *HotPeerStat {
	regionStats := f.kind.RegionStats()

	if source == inherit {
		for _, dim := range oldItem.rollingLoads {
			newItem.rollingLoads = append(newItem.rollingLoads, dim.Clone())
		}
		newItem.allowInherited = false
	} else {
		newItem.rollingLoads = oldItem.rollingLoads
		newItem.allowInherited = oldItem.allowInherited
	}

	if f.justTransferLeader(region, oldItem) {
		newItem.lastTransferLeaderTime = time.Now()
		// skip the first heartbeat flow statistic after transfer leader, because its statistics are calculated by the last leader in this store and are inaccurate
		// maintain anticount and hotdegree to avoid store threshold and hot peer are unstable.
		// For write stat, as the stat is send by region heartbeat, the first heartbeat will be skipped.
		// For read stat, as the stat is send by store heartbeat, the first heartbeat won't be skipped.
		if f.kind == Write {
			f.inheritItem(newItem, oldItem)
			return newItem
		}
	} else {
		newItem.lastTransferLeaderTime = oldItem.lastTransferLeaderTime
	}

	for i, k := range regionStats {
		newItem.rollingLoads[i].Add(deltaLoads[k], interval)
	}

	isFull := newItem.rollingLoads[0].isFull(f.interval()) // The intervals of dims are the same, so it is only necessary to determine whether any of them
	if !isFull {
		// not update hot degree and anti count
		f.inheritItem(newItem, oldItem)
	} else {
		// If item is inCold, it means the pd didn't recv this item in the store heartbeat,
		// thus we make it colder
		if newItem.inCold {
			f.coldItem(newItem, oldItem)
		} else {
			thresholds := f.calcHotThresholds(newItem.StoreID)
			if f.isOldColdPeer(oldItem, newItem.StoreID) {
				if newItem.isHot(thresholds) {
					f.initItem(newItem)
				} else {
					newItem.actionType = Remove
				}
			} else {
				if newItem.isHot(thresholds) {
					f.hotItem(newItem, oldItem)
				} else {
					f.coldItem(newItem, oldItem)
				}
			}
		}
		newItem.clearLastAverage()
	}
	return newItem
}

func (f *hotPeerCache) updateNewHotPeerStat(newItem *HotPeerStat, deltaLoads []float64, interval time.Duration) *HotPeerStat {
	regionStats := f.kind.RegionStats()
	// interval is not 0 which is guaranteed by the caller.
	if interval.Seconds() >= float64(f.kind.ReportInterval()) {
		f.initItem(newItem)
	}
	newItem.actionType = Add
	newItem.rollingLoads = make([]*dimStat, len(regionStats))
	for i, k := range regionStats {
		ds := newDimStat(f.interval())
		ds.Add(deltaLoads[k], interval)
		if ds.isFull(f.interval()) {
			ds.clearLastAverage()
		}
		newItem.rollingLoads[i] = ds
	}
	return newItem
}

func (f *hotPeerCache) putItem(item *HotPeerStat) {
	peers, ok := f.peersOfStore[item.StoreID]
	if !ok {
		peers = NewTopN(DimLen, TopNN, f.topNTTL)
		f.peersOfStore[item.StoreID] = peers
	}
	peers.Put(item)
	stores, ok := f.storesOfRegion[item.RegionID]
	if !ok {
		stores = make(map[uint64]struct{})
		f.storesOfRegion[item.RegionID] = stores
	}
	stores[item.StoreID] = struct{}{}
	regions, ok := f.regionsOfStore[item.StoreID]
	if !ok {
		regions = make(map[uint64]struct{})
		f.regionsOfStore[item.StoreID] = regions
	}
	regions[item.RegionID] = struct{}{}
}

func (f *hotPeerCache) removeItem(item *HotPeerStat) {
	if peers, ok := f.peersOfStore[item.StoreID]; ok {
		peers.Remove(item.RegionID)
	}
	if stores, ok := f.storesOfRegion[item.RegionID]; ok {
		delete(stores, item.StoreID)
	}
	if regions, ok := f.regionsOfStore[item.StoreID]; ok {
		delete(regions, item.RegionID)
	}
}

func (f *hotPeerCache) coldItem(newItem, oldItem *HotPeerStat) {
	newItem.HotDegree = oldItem.HotDegree - 1
	newItem.AntiCount = oldItem.AntiCount - 1
	if newItem.AntiCount <= 0 {
		newItem.actionType = Remove
	} else {
		newItem.allowInherited = true
	}
}

func (f *hotPeerCache) hotItem(newItem, oldItem *HotPeerStat) {
	newItem.HotDegree = oldItem.HotDegree + 1
	if oldItem.AntiCount < f.kind.DefaultAntiCount() {
		newItem.AntiCount = oldItem.AntiCount + 1
	} else {
		newItem.AntiCount = oldItem.AntiCount
	}
	newItem.allowInherited = true
}

func (f *hotPeerCache) initItem(item *HotPeerStat) {
	item.HotDegree = 1
	item.AntiCount = f.kind.DefaultAntiCount()
	item.allowInherited = true
}

func (f *hotPeerCache) inheritItem(newItem, oldItem *HotPeerStat) {
	newItem.HotDegree = oldItem.HotDegree
	newItem.AntiCount = oldItem.AntiCount
}

func (f *hotPeerCache) interval() time.Duration {
	return time.Duration(f.kind.ReportInterval()) * time.Second
}
