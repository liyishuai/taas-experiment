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

package schedule

import (
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/buckets"
)

// Cluster provides an overview of a cluster's regions distribution.
type Cluster interface {
	core.RegionSetInformer
	core.StoreSetInformer
	core.StoreSetController

	statistics.RegionStatInformer
	statistics.StoreStatInformer
	buckets.BucketStatInformer

	operator.ClusterInformer

	RemoveScheduler(name string) error
	AddSuspectRegions(ids ...uint64)
	SetHotPendingInfluenceMetrics(storeLabel, rwTy, dim string, load float64)
	RecordOpStepWithTTL(regionID uint64)
}
