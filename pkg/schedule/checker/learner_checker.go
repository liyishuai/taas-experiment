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

package checker

import (
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/operator"
)

// LearnerChecker ensures region has a learner will be promoted.
type LearnerChecker struct {
	PauseController
	cluster schedule.Cluster
}

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	learnerCheckerPausedCounter = checkerCounter.WithLabelValues("learner_checker", "paused")
)

// NewLearnerChecker creates a learner checker.
func NewLearnerChecker(cluster schedule.Cluster) *LearnerChecker {
	return &LearnerChecker{
		cluster: cluster,
	}
}

// Check verifies a region's role, creating an Operator if need.
func (l *LearnerChecker) Check(region *core.RegionInfo) *operator.Operator {
	if l.IsPaused() {
		learnerCheckerPausedCounter.Inc()
		return nil
	}
	for _, p := range region.GetLearners() {
		op, err := operator.CreatePromoteLearnerOperator("promote-learner", l.cluster, region, p)
		if err != nil {
			log.Debug("fail to create promote learner operator", errs.ZapError(err))
			continue
		}
		return op
	}
	return nil
}
