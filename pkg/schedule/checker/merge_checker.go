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

package checker

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	maxTargetRegionSize   = 500
	maxTargetRegionFactor = 4
)

// When a region has label `merge_option=deny`, skip merging the region.
// If label value is `allow` or other value, it will be treated as `allow`.
const (
	mergeCheckerName     = "merge_checker"
	mergeOptionLabel     = "merge_option"
	mergeOptionValueDeny = "deny"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	mergeCheckerCounter                     = checkerCounter.WithLabelValues(mergeCheckerName, "check")
	mergeCheckerPausedCounter               = checkerCounter.WithLabelValues(mergeCheckerName, "paused")
	mergeCheckerRecentlySplitCounter        = checkerCounter.WithLabelValues(mergeCheckerName, "recently-split")
	mergeCheckerRecentlyStartCounter        = checkerCounter.WithLabelValues(mergeCheckerName, "recently-start")
	mergeCheckerSkipUninitRegionCounter     = checkerCounter.WithLabelValues(mergeCheckerName, "skip-uninit-region")
	mergeCheckerNoNeedCounter               = checkerCounter.WithLabelValues(mergeCheckerName, "no-need")
	mergeCheckerSpecialPeerCounter          = checkerCounter.WithLabelValues(mergeCheckerName, "special-peer")
	mergeCheckerAbnormalReplicaCounter      = checkerCounter.WithLabelValues(mergeCheckerName, "abnormal-replica")
	mergeCheckerHotRegionCounter            = checkerCounter.WithLabelValues(mergeCheckerName, "hot-region")
	mergeCheckerNoTargetCounter             = checkerCounter.WithLabelValues(mergeCheckerName, "no-target")
	mergeCheckerTargetTooLargeCounter       = checkerCounter.WithLabelValues(mergeCheckerName, "target-too-large")
	mergeCheckerSplitSizeAfterMergeCounter  = checkerCounter.WithLabelValues(mergeCheckerName, "split-size-after-merge")
	mergeCheckerSplitKeysAfterMergeCounter  = checkerCounter.WithLabelValues(mergeCheckerName, "split-keys-after-merge")
	mergeCheckerNewOpCounter                = checkerCounter.WithLabelValues(mergeCheckerName, "new-operator")
	mergeCheckerLargerSourceCounter         = checkerCounter.WithLabelValues(mergeCheckerName, "larger-source")
	mergeCheckerAdjNotExistCounter          = checkerCounter.WithLabelValues(mergeCheckerName, "adj-not-exist")
	mergeCheckerAdjRecentlySplitCounter     = checkerCounter.WithLabelValues(mergeCheckerName, "adj-recently-split")
	mergeCheckerAdjRegionHotCounter         = checkerCounter.WithLabelValues(mergeCheckerName, "adj-region-hot")
	mergeCheckerAdjDisallowMergeCounter     = checkerCounter.WithLabelValues(mergeCheckerName, "adj-disallow-merge")
	mergeCheckerAdjAbnormalPeerStoreCounter = checkerCounter.WithLabelValues(mergeCheckerName, "adj-abnormal-peerstore")
	mergeCheckerAdjSpecialPeerCounter       = checkerCounter.WithLabelValues(mergeCheckerName, "adj-special-peer")
	mergeCheckerAdjAbnormalReplicaCounter   = checkerCounter.WithLabelValues(mergeCheckerName, "adj-abnormal-replica")
)

// MergeChecker ensures region to merge with adjacent region when size is small
type MergeChecker struct {
	PauseController
	cluster    schedule.Cluster
	conf       config.Config
	splitCache *cache.TTLUint64
	startTime  time.Time // it's used to judge whether server recently start.
}

// NewMergeChecker creates a merge checker.
func NewMergeChecker(ctx context.Context, cluster schedule.Cluster, conf config.Config) *MergeChecker {
	splitCache := cache.NewIDTTL(ctx, time.Minute, conf.GetSplitMergeInterval())
	return &MergeChecker{
		cluster:    cluster,
		conf:       conf,
		splitCache: splitCache,
		startTime:  time.Now(),
	}
}

// GetType return MergeChecker's type
func (m *MergeChecker) GetType() string {
	return "merge-checker"
}

// RecordRegionSplit put the recently split region into cache. MergeChecker
// will skip check it for a while.
func (m *MergeChecker) RecordRegionSplit(regionIDs []uint64) {
	for _, regionID := range regionIDs {
		m.splitCache.PutWithTTL(regionID, nil, m.conf.GetSplitMergeInterval())
	}
}

// Check verifies a region's replicas, creating an Operator if need.
func (m *MergeChecker) Check(region *core.RegionInfo) []*operator.Operator {
	mergeCheckerCounter.Inc()

	if m.IsPaused() {
		mergeCheckerPausedCounter.Inc()
		return nil
	}

	expireTime := m.startTime.Add(m.conf.GetSplitMergeInterval())
	if time.Now().Before(expireTime) {
		mergeCheckerRecentlyStartCounter.Inc()
		return nil
	}

	m.splitCache.UpdateTTL(m.conf.GetSplitMergeInterval())
	if m.splitCache.Exists(region.GetID()) {
		mergeCheckerRecentlySplitCounter.Inc()
		return nil
	}

	// when pd just started, it will load region meta from region storage,
	if region.GetLeader() == nil {
		mergeCheckerSkipUninitRegionCounter.Inc()
		return nil
	}

	// region is not small enough
	if !region.NeedMerge(int64(m.conf.GetMaxMergeRegionSize()), int64(m.conf.GetMaxMergeRegionKeys())) {
		mergeCheckerNoNeedCounter.Inc()
		return nil
	}

	// skip region has down peers or pending peers
	if !filter.IsRegionHealthy(region) {
		mergeCheckerSpecialPeerCounter.Inc()
		return nil
	}

	if !filter.IsRegionReplicated(m.cluster, region) {
		mergeCheckerAbnormalReplicaCounter.Inc()
		return nil
	}

	// skip hot region
	if m.cluster.IsRegionHot(region) {
		mergeCheckerHotRegionCounter.Inc()
		return nil
	}

	prev, next := m.cluster.GetAdjacentRegions(region)

	var target *core.RegionInfo
	if m.checkTarget(region, next) {
		target = next
	}
	if !m.conf.IsOneWayMergeEnabled() && m.checkTarget(region, prev) { // allow a region can be merged by two ways.
		if target == nil || prev.GetApproximateSize() < next.GetApproximateSize() { // pick smaller
			target = prev
		}
	}

	if target == nil {
		mergeCheckerNoTargetCounter.Inc()
		return nil
	}

	regionMaxSize := m.cluster.GetStoreConfig().GetRegionMaxSize()
	maxTargetRegionSizeThreshold := int64(float64(regionMaxSize) * float64(maxTargetRegionFactor))
	if maxTargetRegionSizeThreshold < maxTargetRegionSize {
		maxTargetRegionSizeThreshold = maxTargetRegionSize
	}
	if target.GetApproximateSize() > maxTargetRegionSizeThreshold {
		mergeCheckerTargetTooLargeCounter.Inc()
		return nil
	}
	if err := m.cluster.GetStoreConfig().CheckRegionSize(uint64(target.GetApproximateSize()+region.GetApproximateSize()),
		m.conf.GetMaxMergeRegionSize()); err != nil {
		mergeCheckerSplitSizeAfterMergeCounter.Inc()
		return nil
	}

	if err := m.cluster.GetStoreConfig().CheckRegionKeys(uint64(target.GetApproximateKeys()+region.GetApproximateKeys()),
		m.conf.GetMaxMergeRegionKeys()); err != nil {
		mergeCheckerSplitKeysAfterMergeCounter.Inc()
		return nil
	}

	log.Debug("try to merge region",
		logutil.ZapRedactStringer("from", core.RegionToHexMeta(region.GetMeta())),
		logutil.ZapRedactStringer("to", core.RegionToHexMeta(target.GetMeta())))
	ops, err := operator.CreateMergeRegionOperator("merge-region", m.cluster, region, target, operator.OpMerge)
	if err != nil {
		log.Warn("create merge region operator failed", errs.ZapError(err))
		return nil
	}
	mergeCheckerNewOpCounter.Inc()
	if region.GetApproximateSize() > target.GetApproximateSize() ||
		region.GetApproximateKeys() > target.GetApproximateKeys() {
		mergeCheckerLargerSourceCounter.Inc()
	}
	return ops
}

func (m *MergeChecker) checkTarget(region, adjacent *core.RegionInfo) bool {
	if adjacent == nil {
		mergeCheckerAdjNotExistCounter.Inc()
		return false
	}

	if m.splitCache.Exists(adjacent.GetID()) {
		mergeCheckerAdjRecentlySplitCounter.Inc()
		return false
	}

	if m.cluster.IsRegionHot(adjacent) {
		mergeCheckerAdjRegionHotCounter.Inc()
		return false
	}

	if !AllowMerge(m.cluster, region, adjacent) {
		mergeCheckerAdjDisallowMergeCounter.Inc()
		return false
	}

	if !checkPeerStore(m.cluster, region, adjacent) {
		mergeCheckerAdjAbnormalPeerStoreCounter.Inc()
		return false
	}

	if !filter.IsRegionHealthy(adjacent) {
		mergeCheckerAdjSpecialPeerCounter.Inc()
		return false
	}

	if !filter.IsRegionReplicated(m.cluster, adjacent) {
		mergeCheckerAdjAbnormalReplicaCounter.Inc()
		return false
	}

	return true
}

// AllowMerge returns true if two regions can be merged according to the key type.
func AllowMerge(cluster schedule.Cluster, region, adjacent *core.RegionInfo) bool {
	var start, end []byte
	if bytes.Equal(region.GetEndKey(), adjacent.GetStartKey()) && len(region.GetEndKey()) != 0 {
		start, end = region.GetStartKey(), adjacent.GetEndKey()
	} else if bytes.Equal(adjacent.GetEndKey(), region.GetStartKey()) && len(adjacent.GetEndKey()) != 0 {
		start, end = adjacent.GetStartKey(), region.GetEndKey()
	} else {
		return false
	}

	// The interface probe is used here to get the rule manager and region
	// labeler because AllowMerge is also used by the random merge scheduler,
	// where it is not easy to get references to concrete objects.
	// We can consider using dependency injection techniques to optimize in
	// the future.

	if cluster.GetOpts().IsPlacementRulesEnabled() {
		cl, ok := cluster.(interface{ GetRuleManager() *placement.RuleManager })
		if !ok || len(cl.GetRuleManager().GetSplitKeys(start, end)) > 0 {
			return false
		}
	}

	if cl, ok := cluster.(interface{ GetRegionLabeler() *labeler.RegionLabeler }); ok {
		l := cl.GetRegionLabeler()
		if len(l.GetSplitKeys(start, end)) > 0 {
			return false
		}
		if l.GetRegionLabel(region, mergeOptionLabel) == mergeOptionValueDeny || l.GetRegionLabel(adjacent, mergeOptionLabel) == mergeOptionValueDeny {
			return false
		}
	}

	policy := cluster.GetOpts().GetKeyType()
	switch policy {
	case constant.Table:
		if cluster.GetOpts().IsCrossTableMergeEnabled() {
			return true
		}
		return isTableIDSame(region, adjacent)
	case constant.Raw:
		return true
	case constant.Txn:
		return true
	default:
		return isTableIDSame(region, adjacent)
	}
}

func isTableIDSame(region, adjacent *core.RegionInfo) bool {
	return codec.Key(region.GetStartKey()).TableID() == codec.Key(adjacent.GetStartKey()).TableID()
}

// Check whether there is a peer of the adjacent region on an offline store,
// while the source region has no peer on it. This is to prevent from bringing
// any other peer into an offline store to slow down the offline process.
func checkPeerStore(cluster schedule.Cluster, region, adjacent *core.RegionInfo) bool {
	regionStoreIDs := region.GetStoreIDs()
	for _, peer := range adjacent.GetPeers() {
		storeID := peer.GetStoreId()
		store := cluster.GetStore(storeID)
		if store == nil || store.IsRemoving() {
			if _, ok := regionStoreIDs[storeID]; !ok {
				return false
			}
		}
	}
	return true
}
