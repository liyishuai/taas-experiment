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
	"path"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// using physical and logical to represent taas logical timestamp <physical, logical>
type taasObject struct {
	syncutil.RWMutex
	tsHigh  int64
	tsLow   int64
	tsLimit int64
}

// taasNode is used to maintain the logic timestamp in memory and limit in etcd
type taasNode struct {
	client   *clientv3.Client
	nodeId   int64
	rootPath string
	// When ltsPath is empty, it means that it is a global timestampOracle.
	ttsPath string
	storage endpoint.TSOStorage
	// TODO: remove saveInterval
	saveInterval           time.Duration
	updatePhysicalInterval time.Duration
	maxResetTSGap          func() time.Duration
	// tso info stored in the memory
	taasMux    *taasObject
	dcLocation string

	reservationLock sync.Mutex
}

var (
	taasLimitWarningLevel = int64(1 << 24)
	taasLimitUpdateLevel  = int64(1 << 28)
)

func (t *taasNode) setTaasHigh(syncTs int64) error {
	if syncTs > t.taasMux.tsLimit {
		t.reservationLock.Lock()
		if syncTs > t.taasMux.tsLimit {
			if err := t.reserveTaasLimit(syncTs + taasLimitUpdateLevel); err != nil {
				return err
			}
		} else {
			t.reservationLock.Unlock()
		}
	} else if syncTs > t.taasMux.tsLimit-taasLimitWarningLevel && t.reservationLock.TryLock() {
		go t.reserveTaasLimit(syncTs + taasLimitUpdateLevel)
	}
	t.taasMux.tsHigh = syncTs
	return nil
}

func (t *taasNode) setTaasLow(syncTs int64) {
	t.taasMux.tsLow = syncTs
}

func (t *taasNode) setTaasLimit(syncTs int64) {
	if t.taasMux.tsLimit < syncTs {
		t.taasMux.tsLimit = syncTs
	}
}

func (t *taasNode) getTSO() (pdpb.Timestamp, error) {
	t.taasMux.RLock()
	defer t.taasMux.RUnlock()
	timestamp := &pdpb.Timestamp{
		Physical:   t.taasMux.tsHigh,
		Logical:    t.taasMux.tsLow,
		SuffixBits: 0,
	}
	return *timestamp, nil
}

func (t *taasNode) generateTaasTSO(count uint32, ts *pdpb.Timestamp) (pdpb.Timestamp, error) {
	t.taasMux.Lock()
	defer t.taasMux.Unlock()
	// log.Info("zghtag", zap.Int64("taas generate tso", t.taasMux.tsHigh))

	newTaasLevel := t.taasMux.tsHigh + int64(count)
	if newTaasLevel <= ts.Physical {
		newTaasLevel = ts.Physical + 1
	}
	if newTaasLevel+taasLimitWarningLevel > t.taasMux.tsLimit {
		// log.Info("TaasTag", zap.Int64("taas high", t.taasMux.tsHigh), zap.Int64("taas limit", t.taasMux.tsLimit))
		t.reserveTaasLimit(newTaasLevel + taasLimitUpdateLevel)
	}
	t.taasMux.tsHigh = newTaasLevel
	timestamp := &pdpb.Timestamp{
		Physical:   t.taasMux.tsHigh,
		Logical:    t.taasMux.tsLow,
		SuffixBits: 0,
	}
	return *timestamp, nil
}

func (t *taasNode) getTimestampPath() string {
	return path.Join(t.ttsPath, timestampKey)
}

func (t *taasNode) Initialize() error {
	if upperBound, err := t.storage.LoadTaasTimestamp(t.getTimestampPath()); err != nil {
		return err
	} else {
		t.setTaasLow(t.nodeId)
		return t.setTaasHigh(upperBound + 1)
	}
}

func (t *taasNode) isInitialized() bool {
	t.taasMux.RLock()
	defer t.taasMux.RUnlock()
	return t.taasMux.tsLow > 0
}

func (t *taasNode) reserveTaasLimit(newLimit int64) error {
	defer t.reservationLock.Unlock()
	if err := t.storage.BumpTaasTimestamp(t.getTimestampPath(), newLimit); err != nil {
		log.Error("TaasTag: update taas limit failed", zap.Error(err))
		return err
	} else {
		t.setTaasLimit(newLimit)
		return nil
	}
}
