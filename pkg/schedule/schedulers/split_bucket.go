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

package schedulers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"gitlab.alibaba-inc.com/zelu.wjz/taasplugin/pkg/pdpb"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/statistics/buckets"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/unrolled/render"
)

const (
	// SplitBucketName is the split bucket name.
	SplitBucketName = "split-bucket-scheduler"
	// SplitBucketType is the spilt bucket type.
	SplitBucketType = "split-bucket"
	// defaultHotDegree is the default hot region threshold.
	defaultHotDegree  = 3
	defaultSplitLimit = 10
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	splitBucketDisableCounter            = schedulerCounter.WithLabelValues(SplitBucketName, "bucket-disable")
	splitBuckerSplitLimitCounter         = schedulerCounter.WithLabelValues(SplitBucketName, "split-limit")
	splitBucketScheduleCounter           = schedulerCounter.WithLabelValues(SplitBucketName, "schedule")
	splitBucketNoRegionCounter           = schedulerCounter.WithLabelValues(SplitBucketName, "no-region")
	splitBucketRegionTooSmallCounter     = schedulerCounter.WithLabelValues(SplitBucketName, "region-too-small")
	splitBucketOperatorExistCounter      = schedulerCounter.WithLabelValues(SplitBucketName, "operator-exist")
	splitBucketKeyRangeNotMatchCounter   = schedulerCounter.WithLabelValues(SplitBucketName, "key-range-not-match")
	splitBucketNoSplitKeysCounter        = schedulerCounter.WithLabelValues(SplitBucketName, "no-split-keys")
	splitBucketCreateOpeartorFailCounter = schedulerCounter.WithLabelValues(SplitBucketName, "create-operator-fail")
	splitBucketNewOperatorCounter        = schedulerCounter.WithLabelValues(SplitBucketName, "new-operator")
)

func initSplitBucketConfig() *splitBucketSchedulerConfig {
	return &splitBucketSchedulerConfig{
		Degree:     defaultHotDegree,
		SplitLimit: defaultSplitLimit,
	}
}

type splitBucketSchedulerConfig struct {
	mu         syncutil.RWMutex
	storage    endpoint.ConfigStorage
	Degree     int    `json:"degree"`
	SplitLimit uint64 `json:"split-limit"`
}

func (conf *splitBucketSchedulerConfig) Clone() *splitBucketSchedulerConfig {
	conf.mu.RLock()
	defer conf.mu.RUnlock()
	return &splitBucketSchedulerConfig{
		Degree: conf.Degree,
	}
}

func (conf *splitBucketSchedulerConfig) persistLocked() error {
	data, err := schedule.EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(SplitBucketName, data)
}

type splitBucketScheduler struct {
	*BaseScheduler
	conf    *splitBucketSchedulerConfig
	handler http.Handler
}

type splitBucketHandler struct {
	conf *splitBucketSchedulerConfig
	rd   *render.Render
}

func (h *splitBucketHandler) ListConfig(w http.ResponseWriter, _ *http.Request) {
	conf := h.conf.Clone()
	_ = h.rd.JSON(w, http.StatusOK, conf)
}

func (h *splitBucketHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	h.conf.mu.Lock()
	defer h.conf.mu.Unlock()
	rd := render.New(render.Options{IndentJSON: true})
	oldc, _ := json.Marshal(h.conf)
	data, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := json.Unmarshal(data, h.conf); err != nil {
		rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	newc, _ := json.Marshal(h.conf)
	if !bytes.Equal(oldc, newc) {
		h.conf.persistLocked()
		rd.Text(w, http.StatusOK, "success")
	}

	rd.Text(w, http.StatusBadRequest, "config item not found")
}

func newSplitBucketHandler(conf *splitBucketSchedulerConfig) http.Handler {
	h := &splitBucketHandler{
		conf: conf,
		rd:   render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/list", h.ListConfig).Methods(http.MethodGet)
	router.HandleFunc("/config", h.UpdateConfig).Methods(http.MethodPost)
	return router
}

func newSplitBucketScheduler(opController *schedule.OperatorController, conf *splitBucketSchedulerConfig) *splitBucketScheduler {
	base := NewBaseScheduler(opController)
	handler := newSplitBucketHandler(conf)
	ret := &splitBucketScheduler{
		BaseScheduler: base,
		conf:          conf,
		handler:       handler,
	}
	return ret
}

// GetName returns the name of the split bucket scheduler.
func (s *splitBucketScheduler) GetName() string {
	return SplitBucketName
}

// GetType returns the type of the split bucket scheduler.
func (s *splitBucketScheduler) GetType() string {
	return SplitBucketType
}

// ServerHTTP implement Http server.
func (s *splitBucketScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// IsScheduleAllowed return true if the sum of executing opSplit operator is less  .
func (s *splitBucketScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	if !cluster.GetStoreConfig().IsEnableRegionBucket() {
		splitBucketDisableCounter.Inc()
		return false
	}
	allowed := s.BaseScheduler.OpController.OperatorCount(operator.OpSplit) < s.conf.SplitLimit
	if !allowed {
		splitBuckerSplitLimitCounter.Inc()
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpSplit.String()).Inc()
	}
	return allowed
}

type splitBucketPlan struct {
	hotBuckets         map[uint64][]*buckets.BucketStat
	cluster            schedule.Cluster
	conf               *splitBucketSchedulerConfig
	hotRegionSplitSize int64
}

// Schedule return operators if some bucket is too hot.
func (s *splitBucketScheduler) Schedule(cluster schedule.Cluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	splitBucketScheduleCounter.Inc()
	conf := s.conf.Clone()
	plan := &splitBucketPlan{
		conf:               conf,
		cluster:            cluster,
		hotBuckets:         cluster.BucketsStats(conf.Degree),
		hotRegionSplitSize: cluster.GetOpts().GetMaxMovableHotPeerSize(),
	}
	return s.splitBucket(plan), nil
}

func (s *splitBucketScheduler) splitBucket(plan *splitBucketPlan) []*operator.Operator {
	var splitBucket *buckets.BucketStat
	for regionID, buckets := range plan.hotBuckets {
		region := plan.cluster.GetRegion(regionID)
		// skip if the region doesn't exist
		if region == nil {
			splitBucketNoRegionCounter.Inc()
			continue
		}
		// region size is less than split region size
		if region.GetApproximateSize() <= plan.hotRegionSplitSize {
			splitBucketRegionTooSmallCounter.Inc()
			continue
		}
		if op := s.OpController.GetOperator(regionID); op != nil {
			splitBucketOperatorExistCounter.Inc()
			continue
		}
		for _, bucket := range buckets {
			// the key range of the bucket must less than the region.
			// like bucket: [001 100] and region: [001 100] will not pass.
			// like bucket: [003 100] and region: [002 100] will pass.
			if bytes.Compare(bucket.StartKey, region.GetStartKey()) < 0 || bytes.Compare(bucket.EndKey, region.GetEndKey()) > 0 {
				splitBucketKeyRangeNotMatchCounter.Inc()
				continue
			}
			if bytes.Equal(bucket.StartKey, region.GetStartKey()) && bytes.Equal(bucket.EndKey, region.GetEndKey()) {
				splitBucketNoSplitKeysCounter.Inc()
				continue
			}

			if splitBucket == nil || bucket.HotDegree > splitBucket.HotDegree {
				splitBucket = bucket
			}
		}
	}
	if splitBucket != nil {
		region := plan.cluster.GetRegion(splitBucket.RegionID)
		splitKey := make([][]byte, 0)
		if bytes.Compare(region.GetStartKey(), splitBucket.StartKey) < 0 {
			splitKey = append(splitKey, splitBucket.StartKey)
		}
		if bytes.Compare(region.GetEndKey(), splitBucket.EndKey) > 0 {
			splitKey = append(splitKey, splitBucket.EndKey)
		}
		op, err := operator.CreateSplitRegionOperator(SplitBucketType, plan.cluster.GetRegion(splitBucket.RegionID), operator.OpSplit,
			pdpb.CheckPolicy_USEKEY, splitKey)
		if err != nil {
			splitBucketCreateOpeartorFailCounter.Inc()
			return nil
		}
		splitBucketNewOperatorCounter.Inc()
		op.AdditionalInfos["region-start-key"] = core.HexRegionKeyStr(region.GetStartKey())
		op.AdditionalInfos["region-end-key"] = core.HexRegionKeyStr(region.GetEndKey())
		op.AdditionalInfos["hot-degree"] = strconv.FormatInt(int64(splitBucket.HotDegree), 10)
		return []*operator.Operator{op}
	}
	return nil
}
