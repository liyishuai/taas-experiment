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

package checker

import (
	"gitlab.alibaba-inc.com/zelu.wjz/taasplugin/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
)

// SplitChecker splits regions when the key range spans across rule/label boundary.
type SplitChecker struct {
	PauseController
	cluster     schedule.Cluster
	ruleManager *placement.RuleManager
	labeler     *labeler.RegionLabeler
}

const splitCheckerName = "split_checker"

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	splitCheckerCounter       = checkerCounter.WithLabelValues(splitCheckerName, "check")
	splitCheckerPausedCounter = checkerCounter.WithLabelValues(splitCheckerName, "paused")
)

// NewSplitChecker creates a new SplitChecker.
func NewSplitChecker(cluster schedule.Cluster, ruleManager *placement.RuleManager, labeler *labeler.RegionLabeler) *SplitChecker {
	return &SplitChecker{
		cluster:     cluster,
		ruleManager: ruleManager,
		labeler:     labeler,
	}
}

// GetType returns the checker type.
func (c *SplitChecker) GetType() string {
	return "split-checker"
}

// Check checks whether the region need to split and returns Operator to fix.
func (c *SplitChecker) Check(region *core.RegionInfo) *operator.Operator {
	splitCheckerCounter.Inc()

	if c.IsPaused() {
		splitCheckerPausedCounter.Inc()
		return nil
	}

	start, end := region.GetStartKey(), region.GetEndKey()
	// We may consider to merge labeler split keys and rule split keys together
	// before creating operator. It can help to reduce operator count. However,
	// handle them separately helps to understand the reason for the split.
	desc := "labeler-split-region"
	keys := c.labeler.GetSplitKeys(start, end)

	if len(keys) == 0 && c.cluster.GetOpts().IsPlacementRulesEnabled() {
		desc = "rule-split-region"
		keys = c.ruleManager.GetSplitKeys(start, end)
	}

	if len(keys) == 0 {
		return nil
	}

	op, err := operator.CreateSplitRegionOperator(desc, region, 0, pdpb.CheckPolicy_USEKEY, keys)
	if err != nil {
		log.Debug("create split region operator failed", errs.ZapError(err))
		return nil
	}
	return op
}
