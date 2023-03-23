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

package statistics

import (
	"fmt"
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/server/config"
)

const (
	unknown   = "unknown"
	labelType = "label"
)

type storeStatistics struct {
	opt             *config.PersistOptions
	storeConfig     *config.StoreConfig
	Up              int
	Disconnect      int
	Unhealthy       int
	Down            int
	Offline         int
	Tombstone       int
	LowSpace        int
	Slow            int
	StorageSize     uint64
	StorageCapacity uint64
	RegionCount     int
	LeaderCount     int
	WitnessCount    int
	LabelCounter    map[string]int
	Preparing       int
	Serving         int
	Removing        int
	Removed         int
}

func newStoreStatistics(opt *config.PersistOptions, storeConfig *config.StoreConfig) *storeStatistics {
	return &storeStatistics{
		opt:          opt,
		storeConfig:  storeConfig,
		LabelCounter: make(map[string]int),
	}
}

func (s *storeStatistics) Observe(store *core.StoreInfo, stats *StoresStats) {
	for _, k := range s.opt.GetLocationLabels() {
		v := store.GetLabelValue(k)
		if v == "" {
			v = unknown
		}
		key := fmt.Sprintf("%s:%s", k, v)
		// exclude tombstone
		if !store.IsRemoved() {
			s.LabelCounter[key]++
		}
	}
	storeAddress := store.GetAddress()
	id := strconv.FormatUint(store.GetID(), 10)
	// Store state.
	switch store.GetNodeState() {
	case metapb.NodeState_Preparing, metapb.NodeState_Serving:
		if store.DownTime() >= s.opt.GetMaxStoreDownTime() {
			s.Down++
		} else if store.IsUnhealthy() {
			s.Unhealthy++
		} else if store.IsDisconnected() {
			s.Disconnect++
		} else if store.IsSlow() {
			s.Slow++
		} else {
			s.Up++
		}
		if store.IsPreparing() {
			s.Preparing++
		} else {
			s.Serving++
		}
	case metapb.NodeState_Removing:
		s.Offline++
		s.Removing++
	case metapb.NodeState_Removed:
		s.Tombstone++
		s.Removed++
		s.resetStoreStatistics(storeAddress, id)
		return
	}
	if store.IsLowSpace(s.opt.GetLowSpaceRatio()) {
		s.LowSpace++
	}

	// Store stats.
	s.StorageSize += store.StorageSize()
	s.StorageCapacity += store.GetCapacity()
	s.RegionCount += store.GetRegionCount()
	s.LeaderCount += store.GetLeaderCount()
	s.WitnessCount += store.GetWitnessCount()
	// TODO: pre-allocate gauge metrics
	storeStatusGauge.WithLabelValues(storeAddress, id, "region_score").Set(store.RegionScore(s.opt.GetRegionScoreFormulaVersion(), s.opt.GetHighSpaceRatio(), s.opt.GetLowSpaceRatio(), 0))
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_score").Set(store.LeaderScore(s.opt.GetLeaderSchedulePolicy(), 0))
	storeStatusGauge.WithLabelValues(storeAddress, id, "region_size").Set(float64(store.GetRegionSize()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "region_count").Set(float64(store.GetRegionCount()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_size").Set(float64(store.GetLeaderSize()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "leader_count").Set(float64(store.GetLeaderCount()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "witness_count").Set(float64(store.GetWitnessCount()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_available").Set(float64(store.GetAvailable()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_used").Set(float64(store.GetUsedSize()))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_capacity").Set(float64(store.GetCapacity()))
	slowTrend := store.GetSlowTrend()
	if slowTrend != nil {
		storeStatusGauge.WithLabelValues(storeAddress, id, "store_slow_trend_cause_value").Set(slowTrend.CauseValue)
		storeStatusGauge.WithLabelValues(storeAddress, id, "store_slow_trend_cause_rate").Set(slowTrend.CauseRate)
		storeStatusGauge.WithLabelValues(storeAddress, id, "store_slow_trend_result_value").Set(slowTrend.ResultValue)
		storeStatusGauge.WithLabelValues(storeAddress, id, "store_slow_trend_result_rate").Set(slowTrend.ResultRate)
	}

	// Store flows.
	storeFlowStats := stats.GetRollingStoreStats(store.GetID())
	if storeFlowStats == nil {
		return
	}

	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_rate_bytes").Set(storeFlowStats.GetLoad(StoreWriteBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_rate_bytes").Set(storeFlowStats.GetLoad(StoreReadBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_rate_keys").Set(storeFlowStats.GetLoad(StoreWriteKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_rate_keys").Set(storeFlowStats.GetLoad(StoreReadKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_query_rate").Set(storeFlowStats.GetLoad(StoreWriteQuery))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_query_rate").Set(storeFlowStats.GetLoad(StoreReadQuery))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_cpu_usage").Set(storeFlowStats.GetLoad(StoreCPUUsage))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_disk_read_rate").Set(storeFlowStats.GetLoad(StoreDiskReadRate))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_disk_write_rate").Set(storeFlowStats.GetLoad(StoreDiskWriteRate))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_regions_write_rate_bytes").Set(storeFlowStats.GetLoad(StoreRegionsWriteBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_regions_write_rate_keys").Set(storeFlowStats.GetLoad(StoreRegionsWriteKeys))

	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_rate_bytes_instant").Set(storeFlowStats.GetInstantLoad(StoreWriteBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_rate_bytes_instant").Set(storeFlowStats.GetInstantLoad(StoreReadBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_rate_keys_instant").Set(storeFlowStats.GetInstantLoad(StoreWriteKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_rate_keys_instant").Set(storeFlowStats.GetInstantLoad(StoreReadKeys))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_write_query_rate_instant").Set(storeFlowStats.GetInstantLoad(StoreWriteQuery))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_read_query_rate_instant").Set(storeFlowStats.GetInstantLoad(StoreReadQuery))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_regions_write_rate_bytes_instant").Set(storeFlowStats.GetInstantLoad(StoreRegionsWriteBytes))
	storeStatusGauge.WithLabelValues(storeAddress, id, "store_regions_write_rate_keys_instant").Set(storeFlowStats.GetInstantLoad(StoreRegionsWriteKeys))
}

func (s *storeStatistics) Collect() {
	placementStatusGauge.Reset()

	metrics := make(map[string]float64)
	metrics["store_up_count"] = float64(s.Up)
	metrics["store_disconnected_count"] = float64(s.Disconnect)
	metrics["store_down_count"] = float64(s.Down)
	metrics["store_unhealth_count"] = float64(s.Unhealthy)
	metrics["store_offline_count"] = float64(s.Offline)
	metrics["store_tombstone_count"] = float64(s.Tombstone)
	metrics["store_low_space_count"] = float64(s.LowSpace)
	metrics["store_slow_count"] = float64(s.Slow)
	metrics["store_preparing_count"] = float64(s.Preparing)
	metrics["store_serving_count"] = float64(s.Serving)
	metrics["store_removing_count"] = float64(s.Removing)
	metrics["store_removed_count"] = float64(s.Removed)
	metrics["region_count"] = float64(s.RegionCount)
	metrics["leader_count"] = float64(s.LeaderCount)
	metrics["witness_count"] = float64(s.WitnessCount)
	metrics["storage_size"] = float64(s.StorageSize)
	metrics["storage_capacity"] = float64(s.StorageCapacity)

	for typ, value := range metrics {
		clusterStatusGauge.WithLabelValues(typ).Set(value)
	}

	// Current scheduling configurations of the cluster
	configs := make(map[string]float64)
	configs["leader-schedule-limit"] = float64(s.opt.GetLeaderScheduleLimit())
	configs["region-schedule-limit"] = float64(s.opt.GetRegionScheduleLimit())
	configs["merge-schedule-limit"] = float64(s.opt.GetMergeScheduleLimit())
	configs["replica-schedule-limit"] = float64(s.opt.GetReplicaScheduleLimit())
	configs["max-replicas"] = float64(s.opt.GetMaxReplicas())
	configs["high-space-ratio"] = s.opt.GetHighSpaceRatio()
	configs["low-space-ratio"] = s.opt.GetLowSpaceRatio()
	configs["tolerant-size-ratio"] = s.opt.GetTolerantSizeRatio()
	configs["hot-region-schedule-limit"] = float64(s.opt.GetHotRegionScheduleLimit())
	configs["hot-region-cache-hits-threshold"] = float64(s.opt.GetHotRegionCacheHitsThreshold())
	configs["max-pending-peer-count"] = float64(s.opt.GetMaxPendingPeerCount())
	configs["max-snapshot-count"] = float64(s.opt.GetMaxSnapshotCount())
	configs["max-merge-region-size"] = float64(s.opt.GetMaxMergeRegionSize())
	configs["max-merge-region-keys"] = float64(s.opt.GetMaxMergeRegionKeys())
	configs["region-max-size"] = float64(s.storeConfig.GetRegionMaxSize())
	configs["region-split-size"] = float64(s.storeConfig.GetRegionSplitSize())
	configs["region-split-keys"] = float64(s.storeConfig.GetRegionSplitKeys())
	configs["region-max-keys"] = float64(s.storeConfig.GetRegionMaxKeys())

	var enableMakeUpReplica, enableRemoveDownReplica, enableRemoveExtraReplica, enableReplaceOfflineReplica float64
	if s.opt.IsMakeUpReplicaEnabled() {
		enableMakeUpReplica = 1
	}
	if s.opt.IsRemoveDownReplicaEnabled() {
		enableRemoveDownReplica = 1
	}
	if s.opt.IsRemoveExtraReplicaEnabled() {
		enableRemoveExtraReplica = 1
	}
	if s.opt.IsReplaceOfflineReplicaEnabled() {
		enableReplaceOfflineReplica = 1
	}

	configs["enable-makeup-replica"] = enableMakeUpReplica
	configs["enable-remove-down-replica"] = enableRemoveDownReplica
	configs["enable-remove-extra-replica"] = enableRemoveExtraReplica
	configs["enable-replace-offline-replica"] = enableReplaceOfflineReplica

	for typ, value := range configs {
		configStatusGauge.WithLabelValues(typ).Set(value)
	}

	for name, value := range s.LabelCounter {
		placementStatusGauge.WithLabelValues(labelType, name).Set(float64(value))
	}

	for storeID, limit := range s.opt.GetStoresLimit() {
		id := strconv.FormatUint(storeID, 10)
		StoreLimitGauge.WithLabelValues(id, "add-peer").Set(limit.AddPeer)
		StoreLimitGauge.WithLabelValues(id, "remove-peer").Set(limit.RemovePeer)
	}
}

func (s *storeStatistics) resetStoreStatistics(storeAddress string, id string) {
	metrics := []string{
		"region_score",
		"leader_score",
		"region_size",
		"region_count",
		"leader_size",
		"leader_count",
		"witness_count",
		"store_available",
		"store_used",
		"store_capacity",
		"store_write_rate_bytes",
		"store_read_rate_bytes",
		"store_write_rate_keys",
		"store_read_rate_keys",
		"store_write_query_rate",
		"store_read_query_rate",
		"store_regions_write_rate_bytes",
		"store_regions_write_rate_keys",
	}
	for _, m := range metrics {
		storeStatusGauge.DeleteLabelValues(storeAddress, id, m)
	}
}

type storeStatisticsMap struct {
	opt   *config.PersistOptions
	stats *storeStatistics
}

// NewStoreStatisticsMap creates a new storeStatisticsMap.
func NewStoreStatisticsMap(opt *config.PersistOptions, storeConfig *config.StoreConfig) *storeStatisticsMap {
	return &storeStatisticsMap{
		opt:   opt,
		stats: newStoreStatistics(opt, storeConfig),
	}
}

func (m *storeStatisticsMap) Observe(store *core.StoreInfo, stats *StoresStats) {
	m.stats.Observe(store, stats)
}

func (m *storeStatisticsMap) Collect() {
	m.stats.Collect()
}

func (m *storeStatisticsMap) Reset() {
	storeStatusGauge.Reset()
	clusterStatusGauge.Reset()
	placementStatusGauge.Reset()
}
