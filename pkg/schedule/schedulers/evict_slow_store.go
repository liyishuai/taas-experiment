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

package schedulers

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"go.uber.org/zap"
)

const (
	// EvictSlowStoreName is evict leader scheduler name.
	EvictSlowStoreName = "evict-slow-store-scheduler"
	// EvictSlowStoreType is evict leader scheduler type.
	EvictSlowStoreType = "evict-slow-store"

	slowStoreEvictThreshold   = 100
	slowStoreRecoverThreshold = 1
)

// WithLabelValues is a heavy operation, define variable to avoid call it every time.
var evictSlowStoreCounter = schedulerCounter.WithLabelValues(EvictSlowStoreName, "schedule")

type evictSlowStoreSchedulerConfig struct {
	storage       endpoint.ConfigStorage
	EvictedStores []uint64 `json:"evict-stores"`
}

func (conf *evictSlowStoreSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	data, err := schedule.EncodeConfig(conf)
	failpoint.Inject("persistFail", func() {
		err = errors.New("fail to persist")
	})
	if err != nil {
		return err
	}
	return conf.storage.SaveScheduleConfig(name, data)
}

func (conf *evictSlowStoreSchedulerConfig) getSchedulerName() string {
	return EvictSlowStoreName
}

func (conf *evictSlowStoreSchedulerConfig) getStores() []uint64 {
	return conf.EvictedStores
}

func (conf *evictSlowStoreSchedulerConfig) getKeyRangesByID(id uint64) []core.KeyRange {
	if conf.evictStore() != id {
		return nil
	}
	return []core.KeyRange{core.NewKeyRange("", "")}
}

func (conf *evictSlowStoreSchedulerConfig) evictStore() uint64 {
	if len(conf.EvictedStores) == 0 {
		return 0
	}
	return conf.EvictedStores[0]
}

func (conf *evictSlowStoreSchedulerConfig) setStoreAndPersist(id uint64) error {
	conf.EvictedStores = []uint64{id}
	return conf.Persist()
}

func (conf *evictSlowStoreSchedulerConfig) clearAndPersist() (oldID uint64, err error) {
	oldID = conf.evictStore()
	if oldID > 0 {
		conf.EvictedStores = []uint64{}
		err = conf.Persist()
	}
	return
}

type evictSlowStoreScheduler struct {
	*BaseScheduler
	conf *evictSlowStoreSchedulerConfig
}

func (s *evictSlowStoreScheduler) GetName() string {
	return EvictSlowStoreName
}

func (s *evictSlowStoreScheduler) GetType() string {
	return EvictSlowStoreType
}

func (s *evictSlowStoreScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *evictSlowStoreScheduler) Prepare(cluster schedule.Cluster) error {
	evictStore := s.conf.evictStore()
	if evictStore != 0 {
		return cluster.SlowStoreEvicted(evictStore)
	}
	return nil
}

func (s *evictSlowStoreScheduler) Cleanup(cluster schedule.Cluster) {
	s.cleanupEvictLeader(cluster)
}

func (s *evictSlowStoreScheduler) prepareEvictLeader(cluster schedule.Cluster, storeID uint64) error {
	err := s.conf.setStoreAndPersist(storeID)
	if err != nil {
		log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", storeID))
		return err
	}

	return cluster.SlowStoreEvicted(storeID)
}

func (s *evictSlowStoreScheduler) cleanupEvictLeader(cluster schedule.Cluster) {
	evictSlowStore, err := s.conf.clearAndPersist()
	if err != nil {
		log.Info("evict-slow-store-scheduler persist config failed", zap.Uint64("store-id", evictSlowStore))
	}
	if evictSlowStore == 0 {
		return
	}
	cluster.SlowStoreRecovered(evictSlowStore)
}

func (s *evictSlowStoreScheduler) schedulerEvictLeader(cluster schedule.Cluster) []*operator.Operator {
	return scheduleEvictLeaderBatch(s.GetName(), s.GetType(), cluster, s.conf, EvictLeaderBatchSize)
}

func (s *evictSlowStoreScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	if s.conf.evictStore() != 0 {
		allowed := s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
		if !allowed {
			operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
		}
		return allowed
	}
	return true
}

func (s *evictSlowStoreScheduler) Schedule(cluster schedule.Cluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	evictSlowStoreCounter.Inc()
	var ops []*operator.Operator

	if s.conf.evictStore() != 0 {
		store := cluster.GetStore(s.conf.evictStore())
		if store == nil || store.IsRemoved() {
			// Previous slow store had been removed, remove the scheduler and check
			// slow node next time.
			log.Info("slow store has been removed",
				zap.Uint64("store-id", store.GetID()))
		} else if store.GetSlowScore() <= slowStoreRecoverThreshold {
			log.Info("slow store has been recovered",
				zap.Uint64("store-id", store.GetID()))
		} else {
			return s.schedulerEvictLeader(cluster), nil
		}
		s.cleanupEvictLeader(cluster)
		return ops, nil
	}

	var slowStore *core.StoreInfo

	for _, store := range cluster.GetStores() {
		if store.IsRemoved() {
			continue
		}

		if (store.IsPreparing() || store.IsServing()) && store.IsSlow() {
			// Do nothing if there is more than one slow store.
			if slowStore != nil {
				return ops, nil
			}
			slowStore = store
		}
	}

	if slowStore == nil || slowStore.GetSlowScore() < slowStoreEvictThreshold {
		return ops, nil
	}

	// If there is only one slow store, evict leaders from that store.
	log.Info("detected slow store, start to evict leaders",
		zap.Uint64("store-id", slowStore.GetID()))
	err := s.prepareEvictLeader(cluster, slowStore.GetID())
	if err != nil {
		log.Info("prepare for evicting leader failed", zap.Error(err), zap.Uint64("store-id", slowStore.GetID()))
		return ops, nil
	}
	return s.schedulerEvictLeader(cluster), nil
}

// newEvictSlowStoreScheduler creates a scheduler that detects and evicts slow stores.
func newEvictSlowStoreScheduler(opController *schedule.OperatorController, conf *evictSlowStoreSchedulerConfig) schedule.Scheduler {
	base := NewBaseScheduler(opController)

	s := &evictSlowStoreScheduler{
		BaseScheduler: base,
		conf:          conf,
	}
	return s
}
