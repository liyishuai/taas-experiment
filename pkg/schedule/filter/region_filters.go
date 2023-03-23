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

package filter

import (
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/slice"
)

// SelectRegions selects regions that be selected from the list.
func SelectRegions(regions []*core.RegionInfo, filters ...RegionFilter) []*core.RegionInfo {
	return filterRegionsBy(regions, func(r *core.RegionInfo) bool {
		return slice.AllOf(filters, func(i int) bool {
			return filters[i].Select(r).IsOK()
		})
	})
}

func filterRegionsBy(regions []*core.RegionInfo, keepPred func(*core.RegionInfo) bool) (selected []*core.RegionInfo) {
	for _, s := range regions {
		if keepPred(s) {
			selected = append(selected, s)
		}
	}
	return
}

// SelectOneRegion selects one region that be selected from the list.
func SelectOneRegion(regions []*core.RegionInfo, collector *plan.Collector, filters ...RegionFilter) *core.RegionInfo {
	for _, r := range regions {
		if len(filters) == 0 || slice.AllOf(filters,
			func(i int) bool {
				status := filters[i].Select(r)
				if !status.IsOK() {
					if collector != nil {
						collector.Collect(plan.SetResource(r), plan.SetStatus(status))
					}
					return false
				}
				return true
			}) {
			return r
		}
	}
	return nil
}

// RegionFilter is an interface to filter region.
type RegionFilter interface {
	// Return plan.Status show whether be filtered
	Select(region *core.RegionInfo) *plan.Status
}

type regionPendingFilter struct {
}

// NewRegionPendingFilter creates a RegionFilter that filters all regions with pending peers.
func NewRegionPendingFilter() RegionFilter {
	return &regionPendingFilter{}
}

func (f *regionPendingFilter) Select(region *core.RegionInfo) *plan.Status {
	if hasPendingPeers(region) {
		return statusRegionPendingPeer
	}
	return statusOK
}

type regionDownFilter struct {
}

// NewRegionDownFilter creates a RegionFilter that filters all regions with down peers.
func NewRegionDownFilter() RegionFilter {
	return &regionDownFilter{}
}

func (f *regionDownFilter) Select(region *core.RegionInfo) *plan.Status {
	if hasDownPeers(region) {
		return statusRegionDownPeer
	}
	return statusOK
}

// RegionReplicatedFilter filters all unreplicated regions.
type RegionReplicatedFilter struct {
	cluster regionHealthCluster
	fit     *placement.RegionFit
}

// NewRegionReplicatedFilter creates a RegionFilter that filters all unreplicated regions.
func NewRegionReplicatedFilter(cluster regionHealthCluster) RegionFilter {
	return &RegionReplicatedFilter{cluster: cluster}
}

// GetFit returns the region fit.
func (f *RegionReplicatedFilter) GetFit() *placement.RegionFit {
	return f.fit
}

// Select returns Ok if the given region satisfy the replication.
// it will cache the lasted region fit if the region satisfy the replication.
func (f *RegionReplicatedFilter) Select(region *core.RegionInfo) *plan.Status {
	if f.cluster.GetOpts().IsPlacementRulesEnabled() {
		fit := f.cluster.GetRuleManager().FitRegion(f.cluster, region)
		if !fit.IsSatisfied() {
			return statusRegionNotMatchRule
		}
		f.fit = fit
		return statusOK
	}
	if !isRegionReplicasSatisfied(f.cluster, region) {
		return statusRegionNotReplicated
	}
	return statusOK
}

type regionEmptyFilter struct {
	cluster regionHealthCluster
}

// NewRegionEmptyFilter returns creates a RegionFilter that filters all empty regions.
func NewRegionEmptyFilter(cluster regionHealthCluster) RegionFilter {
	return &regionEmptyFilter{cluster: cluster}
}

func (f *regionEmptyFilter) Select(region *core.RegionInfo) *plan.Status {
	if !isEmptyRegionAllowBalance(f.cluster, region) {
		return statusRegionEmpty
	}
	return statusOK
}

// isEmptyRegionAllowBalance returns true if the region is not empty or the number of regions is too small.
func isEmptyRegionAllowBalance(cluster regionHealthCluster, region *core.RegionInfo) bool {
	return region.GetApproximateSize() > core.EmptyRegionApproximateSize || cluster.GetRegionCount() < core.InitClusterRegionThreshold
}

type regionWitnessFilter struct {
	storeID uint64
}

// NewRegionWitnessFilter returns creates a RegionFilter that filters regions with witness peer on the specific store.
func NewRegionWitnessFilter(storeID uint64) RegionFilter {
	return &regionWitnessFilter{storeID: storeID}
}

func (f *regionWitnessFilter) Select(region *core.RegionInfo) *plan.Status {
	if region.GetStoreWitness(f.storeID) != nil {
		return statusRegionWitnessPeer
	}
	return statusOK
}
