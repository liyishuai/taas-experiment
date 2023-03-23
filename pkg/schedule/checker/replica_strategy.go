// Copyright 2020 TiKV Project Authors.
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
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/filter"
	"go.uber.org/zap"
)

// ReplicaStrategy collects some utilities to manipulate region peers. It
// exists to allow replica_checker and rule_checker to reuse common logics.
type ReplicaStrategy struct {
	checkerName    string // replica-checker / rule-checker
	cluster        schedule.Cluster
	locationLabels []string
	isolationLevel string
	region         *core.RegionInfo
	extraFilters   []filter.Filter
	fastFailover   bool
}

// SelectStoreToAdd returns the store to add a replica to a region.
// `coLocationStores` are the stores used to compare location with target
// store.
// `extraFilters` is used to set up more filters based on the context that
// calling this method.
//
// For example, to select a target store to replace a region's peer, we can use
// the peer list with the peer removed as `coLocationStores`.
// Meanwhile, we need to provide more constraints to ensure that the isolation
// level cannot be reduced after replacement.
func (s *ReplicaStrategy) SelectStoreToAdd(coLocationStores []*core.StoreInfo, extraFilters ...filter.Filter) (uint64, bool) {
	// The selection process uses a two-stage fashion. The first stage
	// ignores the temporary state of the stores and selects the stores
	// with the highest score according to the location label. The second
	// stage considers all temporary states and capacity factors to select
	// the most suitable target.
	//
	// The reason for it is to prevent the non-optimal replica placement due
	// to the short-term state, resulting in redundant scheduling.
	filters := []filter.Filter{
		filter.NewExcludedFilter(s.checkerName, nil, s.region.GetStoreIDs()),
		filter.NewStorageThresholdFilter(s.checkerName),
		filter.NewSpecialUseFilter(s.checkerName),
		&filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, AllowTemporaryStates: true},
	}
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		filters = append(filters, filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationStores))
	}
	if len(extraFilters) > 0 {
		filters = append(filters, extraFilters...)
	}
	if len(s.extraFilters) > 0 {
		filters = append(filters, s.extraFilters...)
	}

	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationStores)
	strictStateFilter := &filter.StoreStateFilter{ActionScope: s.checkerName, MoveRegion: true, AllowFastFailover: s.fastFailover}
	targetCandidate := filter.NewCandidates(s.cluster.GetStores()).
		FilterTarget(s.cluster.GetOpts(), nil, nil, filters...).
		KeepTheTopStores(isolationComparer, false) // greater isolation score is better
	if targetCandidate.Len() == 0 {
		return 0, false
	}
	target := targetCandidate.FilterTarget(s.cluster.GetOpts(), nil, nil, strictStateFilter).
		PickTheTopStore(filter.RegionScoreComparer(s.cluster.GetOpts()), true) // less region score is better
	if target == nil {
		return 0, true // filter by temporary states
	}
	return target.GetID(), false
}

// SelectStoreToFix returns a store to replace down/offline old peer. The location
// placement after scheduling is allowed to be worse than original.
func (s *ReplicaStrategy) SelectStoreToFix(coLocationStores []*core.StoreInfo, old uint64) (uint64, bool) {
	// trick to avoid creating a slice with `old` removed.
	s.swapStoreToFirst(coLocationStores, old)
	return s.SelectStoreToAdd(coLocationStores[1:])
}

// SelectStoreToImprove returns a store to replace oldStore. The location
// placement after scheduling should be better than original.
func (s *ReplicaStrategy) SelectStoreToImprove(coLocationStores []*core.StoreInfo, old uint64) (uint64, bool) {
	// trick to avoid creating a slice with `old` removed.
	s.swapStoreToFirst(coLocationStores, old)
	oldStore := s.cluster.GetStore(old)
	if oldStore == nil {
		return 0, false
	}
	filters := []filter.Filter{
		filter.NewLocationImprover(s.checkerName, s.locationLabels, coLocationStores, oldStore),
	}
	if len(s.locationLabels) > 0 && s.isolationLevel != "" {
		filters = append(filters, filter.NewIsolationFilter(s.checkerName, s.isolationLevel, s.locationLabels, coLocationStores[1:]))
	}
	return s.SelectStoreToAdd(coLocationStores[1:], filters...)
}

func (s *ReplicaStrategy) swapStoreToFirst(stores []*core.StoreInfo, id uint64) {
	for i, s := range stores {
		if s.GetID() == id {
			stores[0], stores[i] = stores[i], stores[0]
			return
		}
	}
}

// SelectStoreToRemove returns the best option to remove from the region.
func (s *ReplicaStrategy) SelectStoreToRemove(coLocationStores []*core.StoreInfo) uint64 {
	isolationComparer := filter.IsolationComparer(s.locationLabels, coLocationStores)
	source := filter.NewCandidates(coLocationStores).
		FilterSource(s.cluster.GetOpts(), nil, nil, &filter.StoreStateFilter{ActionScope: replicaCheckerName, MoveRegion: true}).
		KeepTheTopStores(isolationComparer, true).
		PickTheTopStore(filter.RegionScoreComparer(s.cluster.GetOpts()), false)
	if source == nil {
		log.Debug("no removable store", zap.Uint64("region-id", s.region.GetID()))
		return 0
	}
	return source.GetID()
}
