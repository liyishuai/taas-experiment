// Copyright 2016 TiKV Project Authors.
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

package tso

import (
	"fmt"
	"path"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// using physical and logical to represent taas logical timestamp <physical, logical>
type taasObject struct {
	syncutil.RWMutex
	tsHigh int64
	tsLow  int64
}

// taasNode is used to maintain the logic timestamp in memory and limit in etcd
type taasNode struct {
	client   *clientv3.Client
	rootPath string
	// When ltsPath is empty, it means that it is a global timestampOracle.
	ttsPath string
	storage endpoint.TaasStorage
	// TODO: remove saveInterval
	saveInterval           time.Duration
	updatePhysicalInterval time.Duration
	maxResetTSGap          func() time.Duration
	// tso info stored in the memory
	taasMux *taasObject
	// last timestamp window stored in etcd
	lastSavedTime atomic.Value // stored as time.Time
	dcLocation    string
}

func (t *taasNode) setTaasHigh(syncTs int64) {
	t.taasMux.Lock()
	defer t.taasMux.Unlock()
	t.taasMux.tsHigh = syncTs
}

func (t *taasNode) setTaasLow(syncTs int64) {
	t.taasMux.Lock()
	defer t.taasMux.Unlock()
	t.taasMux.tsLow = syncTs
}

func (t *taasNode) getTSO() pdpb.Timestamp {
	t.taasMux.Lock()
	defer t.taasMux.RUnlock()
	timestamp := &pdpb.Timestamp{
		Physical:   t.taasMux.tsHigh,
		Logical:    t.taasMux.tsLow,
		SuffixBits: 0,
	}
	t.taasMux.tsHigh += 1
	return *timestamp
}

// generateTSO will add the TSO's logical part with the given count and returns the new TSO result.
func (t *taasNode) generateTSO(count int64, suffixBits int) (physical int64, logical int64, lastUpdateTime time.Time) {
	t.taasMux.Lock()
	defer t.taasMux.Unlock()

	t.taasMux.tsHigh += count
	logical = t.taasMux.logical

	return physical, logical, lastUpdateTime
}

func (t *taasNode) getTimestampPath() string {
	return path.Join(t.ttsPath, timestampKey)
}

// SyncTimestamp is used to synchronize the timestamp.
func (t *taasNode) SyncTimestamp(leadership *election.Leadership) error {
	last, err := t.storage.LoadTaasTimestamp(t.ttsPath)
	if err != nil {
		return err
	}

	log.Info("sync and save timestamp", zap.Int64("last", last))
	// save into memory
	t.setTaasHigh(last)
	return nil
}

// isInitialized is used to check whether the timestampOracle is initialized.
// There are two situations we have an uninitialized timestampOracle:
// 1. When the SyncTimestamp has not been called yet.
// 2. When the ResetUserTimestamp has been called already.
func (t *taasNode) isInitialized() bool {
	t.taasMux.RLock()
	defer t.taasMux.RUnlock()
	return t.taasMux.tsHigh > 0
}

// resetUserTimestamp update the TSO in memory with specified TSO by an atomically way.
// When ignoreSmaller is true, resetUserTimestamp will ignore the smaller tso resetting error and do nothing.
// It's used to write MaxTS during the Global TSO synchronization without failing the writing as much as possible.
// cannot set timestamp to one which >= current + maxResetTSGap
func (t *taasNode) resetUserTimestamp(leadership *election.Leadership, tso uint64, ignoreSmaller bool) error {
	return t.resetUserTimestampInner(leadership, tso, ignoreSmaller, false)
}

func (t *taasNode) resetUserTimestampInner(leadership *election.Leadership, tso uint64, ignoreSmaller, skipUpperBoundCheck bool) error {
	t.taasMux.Lock()
	defer t.taasMux.Unlock()
	if !leadership.Check() {
		tsoCounter.WithLabelValues("err_lease_reset_ts", t.dcLocation).Inc()
		return errs.ErrResetUserTimestamp.FastGenByArgs("lease expired")
	}
	var (
		nextPhysical, nextLogical = tsoutil.ParseTS(tso)
		logicalDifference         = int64(nextLogical) - t.taasMux.logical
		physicalDifference        = typeutil.SubTSOPhysicalByWallClock(nextPhysical, t.taasMux.physical)
	)
	// do not update if next physical time is less/before than prev
	if physicalDifference < 0 {
		tsoCounter.WithLabelValues("err_reset_small_ts", t.dcLocation).Inc()
		if ignoreSmaller {
			return nil
		}
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified ts is smaller than now")
	}
	// do not update if next logical time is less/before/equal than prev
	if physicalDifference == 0 && logicalDifference <= 0 {
		tsoCounter.WithLabelValues("err_reset_small_counter", t.dcLocation).Inc()
		if ignoreSmaller {
			return nil
		}
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified counter is smaller than now")
	}
	// do not update if physical time is too greater than prev
	if !skipUpperBoundCheck && physicalDifference >= t.maxResetTSGap().Milliseconds() {
		tsoCounter.WithLabelValues("err_reset_large_ts", t.dcLocation).Inc()
		return errs.ErrResetUserTimestamp.FastGenByArgs("the specified ts is too larger than now")
	}
	// save into etcd only if nextPhysical is close to lastSavedTime
	if typeutil.SubRealTimeByWallClock(t.lastSavedTime.Load().(time.Time), nextPhysical) <= UpdateTimestampGuard {
		save := nextPhysical.Add(t.saveInterval)
		if err := t.storage.SaveTimestamp(t.getTimestampPath(), save); err != nil {
			tsoCounter.WithLabelValues("err_save_reset_ts", t.dcLocation).Inc()
			return err
		}
		t.lastSavedTime.Store(save)
	}
	// save into memory only if nextPhysical or nextLogical is greater.
	t.taasMux.physical = nextPhysical
	t.taasMux.logical = int64(nextLogical)
	t.setTSOUpdateTimeLocked(time.Now())
	tsoCounter.WithLabelValues("reset_tso_ok", t.dcLocation).Inc()
	return nil
}

// UpdateTimestamp is used to update the timestamp.
// This function will do two things:
//  1. When the logical time is going to be used up, increase the current physical time.
//  2. When the time window is not big enough, which means the saved etcd time minus the next physical time
//     will be less than or equal to `UpdateTimestampGuard`, then the time window needs to be updated and
//     we also need to save the next physical time plus `TSOSaveInterval` into etcd.
//
// Here is some constraints that this function must satisfy:
// 1. The saved time is monotonically increasing.
// 2. The physical time is monotonically increasing.
// 3. The physical time is always less than the saved timestamp.
//
// NOTICE: this function should be called after the TSO in memory has been initialized
// and should not be called when the TSO in memory has been reset anymore.
func (t *taasNode) UpdateTimestamp(leadership *election.Leadership) error {
	prevPhysical, prevLogical := t.getTSO()
	tsoGauge.WithLabelValues("tso", t.dcLocation).Set(float64(prevPhysical.UnixNano() / int64(time.Millisecond)))
	tsoGap.WithLabelValues(t.dcLocation).Set(float64(time.Since(prevPhysical).Milliseconds()))

	now := time.Now()
	failpoint.Inject("fallBackUpdate", func() {
		now = now.Add(time.Hour)
	})
	failpoint.Inject("systemTimeSlow", func() {
		now = now.Add(-time.Hour)
	})

	tsoCounter.WithLabelValues("save", t.dcLocation).Inc()

	jetLag := typeutil.SubRealTimeByWallClock(now, prevPhysical)
	if jetLag > 3*t.updatePhysicalInterval && jetLag > jetLagWarningThreshold {
		log.Warn("clock offset", zap.Duration("jet-lag", jetLag), zap.Time("prev-physical", prevPhysical), zap.Time("now", now), zap.Duration("update-physical-interval", t.updatePhysicalInterval))
		tsoCounter.WithLabelValues("slow_save", t.dcLocation).Inc()
	}

	if jetLag < 0 {
		tsoCounter.WithLabelValues("system_time_slow", t.dcLocation).Inc()
	}

	var next time.Time
	// If the system time is greater, it will be synchronized with the system time.
	if jetLag > UpdateTimestampGuard {
		next = now
	} else if prevLogical > maxLogical/2 {
		// The reason choosing maxLogical/2 here is that it's big enough for common cases.
		// Because there is enough timestamp can be allocated before next update.
		log.Warn("the logical time may be not enough", zap.Int64("prev-logical", prevLogical))
		next = prevPhysical.Add(time.Millisecond)
	} else {
		// It will still use the previous physical time to alloc the timestamp.
		tsoCounter.WithLabelValues("skip_save", t.dcLocation).Inc()
		return nil
	}

	// It is not safe to increase the physical time to `next`.
	// The time window needs to be updated and saved to etcd.
	if typeutil.SubRealTimeByWallClock(t.lastSavedTime.Load().(time.Time), next) <= UpdateTimestampGuard {
		save := next.Add(t.saveInterval)
		if err := t.storage.SaveTimestamp(t.getTimestampPath(), save); err != nil {
			tsoCounter.WithLabelValues("err_save_update_ts", t.dcLocation).Inc()
			return err
		}
		t.lastSavedTime.Store(save)
	}
	// save into memory
	t.setTSOPhysical(next, false)

	return nil
}

var maxRetryCount = 10

// getTS is used to get a timestamp.
func (t *taasNode) getTS(leadership *election.Leadership, count uint32, suffixBits int) (pdpb.Timestamp, error) {
	var resp pdpb.Timestamp
	if count == 0 {
		return resp, errs.ErrGenerateTimestamp.FastGenByArgs("tso count should be positive")
	}
	for i := 0; i < maxRetryCount; i++ {
		currentPhysical, _ := t.getTSO()
		if currentPhysical == typeutil.ZeroTime {
			// If it's leader, maybe SyncTimestamp hasn't completed yet
			if leadership.Check() {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			tsoCounter.WithLabelValues("not_leader_anymore", t.dcLocation).Inc()
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
		}
		// Get a new TSO result with the given count
		resp.Physical, resp.Logical, _ = t.generateTSO(int64(count), suffixBits)
		if resp.GetPhysical() == 0 {
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory has been reset")
		}
		if resp.GetLogical() >= maxLogical {
			log.Warn("logical part outside of max logical interval, please check ntp time, or adjust config item `tso-update-physical-interval`",
				zap.Reflect("response", resp),
				zap.Int("retry-count", i), errs.ZapError(errs.ErrLogicOverflow))
			tsoCounter.WithLabelValues("logical_overflow", t.dcLocation).Inc()
			time.Sleep(t.updatePhysicalInterval)
			continue
		}
		// In case lease expired after the first check.
		if !leadership.Check() {
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("not the pd or local tso allocator leader anymore")
		}
		resp.SuffixBits = uint32(suffixBits)
		return resp, nil
	}
	tsoCounter.WithLabelValues("exceeded_max_retry", t.dcLocation).Inc()
	return resp, errs.ErrGenerateTimestamp.FastGenByArgs(fmt.Sprintf("generate %s tso maximum number of retries exceeded", t.dcLocation))
}

// ResetTimestamp is used to reset the timestamp in memory.
func (t *taasNode) ResetTimestamp() {
	t.taasMux.Lock()
	defer t.taasMux.Unlock()
	log.Info("reset the timestamp in memory")
	t.taasMux.physical = typeutil.ZeroTime
	t.taasMux.logical = 0
	t.setTSOUpdateTimeLocked(typeutil.ZeroTime)
}
