// Copyright 2023 TiKV Project Authors.
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
	"strconv"
	"strings"
	"sync"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

var registerOnce sync.Once

// Register registers schedulers.
func Register() {
	registerOnce.Do(func() {
		schedulersRegister()
	})
}

func schedulersRegister() {
	// balance leader
	schedule.RegisterSliceDecoderBuilder(BalanceLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Batch = BalanceLeaderBatchSize
			return nil
		}
	})

	schedule.RegisterScheduler(BalanceLeaderType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceLeaderSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		if conf.Batch == 0 {
			conf.Batch = BalanceLeaderBatchSize
		}
		return newBalanceLeaderScheduler(opController, conf), nil
	})

	// balance region
	schedule.RegisterSliceDecoderBuilder(BalanceRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = BalanceRegionName
			return nil
		}
	})

	schedule.RegisterScheduler(BalanceRegionType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceRegionSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newBalanceRegionScheduler(opController, conf), nil
	})

	// balance witness
	schedule.RegisterSliceDecoderBuilder(BalanceWitnessType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*balanceWitnessSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Batch = balanceWitnessBatchSize
			return nil
		}
	})

	schedule.RegisterScheduler(BalanceWitnessType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &balanceWitnessSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		if conf.Batch == 0 {
			conf.Batch = balanceWitnessBatchSize
		}
		return newBalanceWitnessScheduler(opController, conf), nil
	})

	// evict leader
	schedule.RegisterSliceDecoderBuilder(EvictLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errs.ErrSchedulerConfig.FastGenByArgs("id")
			}
			conf, ok := v.(*evictLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
			}

			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return err
			}
			conf.StoreIDWithRanges[id] = ranges
			return nil
		}
	})

	schedule.RegisterScheduler(EvictLeaderType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.cluster = opController.GetCluster()
		return newEvictLeaderScheduler(opController, conf), nil
	})

	// evict slow store
	schedule.RegisterSliceDecoderBuilder(EvictSlowStoreType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	schedule.RegisterScheduler(EvictSlowStoreType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictSlowStoreSchedulerConfig{storage: storage, EvictedStores: make([]uint64, 0)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newEvictSlowStoreScheduler(opController, conf), nil
	})

	// grant hot region
	schedule.RegisterSliceDecoderBuilder(GrantHotRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 2 {
				return errs.ErrSchedulerConfig.FastGenByArgs("id")
			}

			conf, ok := v.(*grantHotRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			leaderID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
			}

			storeIDs := make([]uint64, 0)
			for _, id := range strings.Split(args[1], ",") {
				storeID, err := strconv.ParseUint(id, 10, 64)
				if err != nil {
					return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
				}
				storeIDs = append(storeIDs, storeID)
			}
			if !conf.setStore(leaderID, storeIDs) {
				return errs.ErrSchedulerConfig
			}
			return nil
		}
	})

	schedule.RegisterScheduler(GrantHotRegionType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &grantHotRegionSchedulerConfig{StoreIDs: make([]uint64, 0), storage: storage}
		conf.cluster = opController.GetCluster()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newGrantHotRegionScheduler(opController, conf), nil
	})

	// hot region
	schedule.RegisterSliceDecoderBuilder(HotRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	schedule.RegisterScheduler(HotRegionType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := initHotRegionScheduleConfig()
		var data map[string]interface{}
		if err := decoder(&data); err != nil {
			return nil, err
		}
		if len(data) != 0 {
			// After upgrading, use compatible config.
			// For clusters with the initial version >= v5.2, it will be overwritten by the default config.
			conf.applyPrioritiesConfig(compatiblePrioritiesConfig)
			// For clusters with the initial version >= v6.4, it will be overwritten by the default config.
			conf.SetRankFormulaVersion("")
			if err := decoder(conf); err != nil {
				return nil, err
			}
		}
		conf.storage = storage
		return newHotScheduler(opController, conf), nil
	})

	// grant leader
	schedule.RegisterSliceDecoderBuilder(GrantLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 1 {
				return errs.ErrSchedulerConfig.FastGenByArgs("id")
			}

			conf, ok := v.(*grantLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}

			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
			}
			ranges, err := getKeyRanges(args[1:])
			if err != nil {
				return err
			}
			conf.StoreIDWithRanges[id] = ranges
			return nil
		}
	})

	schedule.RegisterScheduler(GrantLeaderType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &grantLeaderSchedulerConfig{StoreIDWithRanges: make(map[uint64][]core.KeyRange), storage: storage}
		conf.cluster = opController.GetCluster()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newGrantLeaderScheduler(opController, conf), nil
	})

	// label
	schedule.RegisterSliceDecoderBuilder(LabelType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*labelSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = LabelName
			return nil
		}
	})

	schedule.RegisterScheduler(LabelType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &labelSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newLabelScheduler(opController, conf), nil
	})

	// random merge
	schedule.RegisterSliceDecoderBuilder(RandomMergeType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*randomMergeSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = RandomMergeName
			return nil
		}
	})

	schedule.RegisterScheduler(RandomMergeType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &randomMergeSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newRandomMergeScheduler(opController, conf), nil
	})

	// scatter range
	// args: [start-key, end-key, range-name].
	schedule.RegisterSliceDecoderBuilder(ScatterRangeType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			if len(args) != 3 {
				return errs.ErrSchedulerConfig.FastGenByArgs("ranges and name")
			}
			if len(args[2]) == 0 {
				return errs.ErrSchedulerConfig.FastGenByArgs("range name")
			}
			conf, ok := v.(*scatterRangeSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			conf.StartKey = args[0]
			conf.EndKey = args[1]
			conf.RangeName = args[2]
			return nil
		}
	})

	schedule.RegisterScheduler(ScatterRangeType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &scatterRangeSchedulerConfig{
			storage: storage,
		}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		rangeName := conf.RangeName
		if len(rangeName) == 0 {
			return nil, errs.ErrSchedulerConfig.FastGenByArgs("range name")
		}
		return newScatterRangeScheduler(opController, conf), nil
	})

	// shuffle hot region
	schedule.RegisterSliceDecoderBuilder(ShuffleHotRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleHotRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			conf.Limit = uint64(1)
			if len(args) == 1 {
				limit, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return errs.ErrStrconvParseUint.Wrap(err).FastGenWithCause()
				}
				conf.Limit = limit
			}
			conf.Name = ShuffleHotRegionName
			return nil
		}
	})

	schedule.RegisterScheduler(ShuffleHotRegionType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleHotRegionSchedulerConfig{Limit: uint64(1)}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleHotRegionScheduler(opController, conf), nil
	})

	// shuffle leader
	schedule.RegisterSliceDecoderBuilder(ShuffleLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleLeaderSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Name = ShuffleLeaderName
			return nil
		}
	})

	schedule.RegisterScheduler(ShuffleLeaderType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleLeaderSchedulerConfig{}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleLeaderScheduler(opController, conf), nil
	})

	// shuffle region
	schedule.RegisterSliceDecoderBuilder(ShuffleRegionType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*shuffleRegionSchedulerConfig)
			if !ok {
				return errs.ErrScheduleConfigNotExist.FastGenByArgs()
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return err
			}
			conf.Ranges = ranges
			conf.Roles = allRoles
			return nil
		}
	})

	schedule.RegisterScheduler(ShuffleRegionType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &shuffleRegionSchedulerConfig{storage: storage}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newShuffleRegionScheduler(opController, conf), nil
	})

	// split bucket
	schedule.RegisterSliceDecoderBuilder(SplitBucketType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	schedule.RegisterScheduler(SplitBucketType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := initSplitBucketConfig()
		if err := decoder(conf); err != nil {
			return nil, err
		}
		conf.storage = storage
		return newSplitBucketScheduler(opController, conf), nil
	})

	// transfer witness leader
	schedule.RegisterSliceDecoderBuilder(TransferWitnessLeaderType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	schedule.RegisterScheduler(TransferWitnessLeaderType, func(opController *schedule.OperatorController, _ endpoint.ConfigStorage, _ schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newTransferWitnessLeaderScheduler(opController), nil
	})

	// evict slow store by trend
	schedule.RegisterSliceDecoderBuilder(EvictSlowTrendType, func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})

	schedule.RegisterScheduler(EvictSlowTrendType, func(opController *schedule.OperatorController, storage endpoint.ConfigStorage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &evictSlowTrendSchedulerConfig{storage: storage, EvictedStores: make([]uint64, 0), evictCandidate: 0}
		if err := decoder(conf); err != nil {
			return nil, err
		}
		return newEvictSlowTrendScheduler(opController, conf), nil
	})
}
