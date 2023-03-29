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

package tso

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// TaasTSOAllocator is a global TSO allocator using TaaS algorithm.
type TaasTSOAllocator struct {
	// for global TSO synchronization
	allocatorManager *AllocatorManager
	// leadership is used to get etcd client 
	leadership      *election.Leadership
	taasNode	 	*taasNode
}

// NewTaasTSOAllocator creates a new Taas TSO allocator.
func NewTaasTSOAllocator(
	am *AllocatorManager,
	leadership *election.Leadership,
) Allocator {
	tta := &TaasTSOAllocator{
		leadership: leadership,
		taasNode: &taasNode{
			client:                 leadership.GetClient(),
			rootPath:               am.rootPath,
			ttsPath:                "",
			storage:                am.storage,
			saveInterval:           am.saveInterval,
			updatePhysicalInterval: am.updatePhysicalInterval,
			maxResetTSGap:          am.maxResetTSGap,
			dcLocation:             GlobalDCLocation,
			taasMux:                &taasObject{},
		},
	}
	return tta
}

// Initialize will initialize the created taas allocator.
func (tta *TaasTSOAllocator) Initialize(int) error {
	// keep same with pd global tso
	tta.taasNode.suffix = 0
	// initialize from etcd
	return tta.taasNode.SyncTimestamp(gta.leadership)
}

// IsInitialize is used to indicates whether this allocator is initialized.
func (tta *TaasTSOAllocator) IsInitialize() bool {
	return tta.taasNode.isInitialized()
}

// UpdateTSO is used to update the TSO in memory and the time window in etcd.
func (tta *TaasTSOAllocator) UpdateTSO() error {
	return gta.timestampOracle.UpdateTimestamp(gta.leadership)
}

// SetTSO sets the physical part with given TSO.
func (tta *TaasTSOAllocator) SetTSO(tso uint64, ignoreSmaller, skipUpperBoundCheck bool) error {
	return gta.timestampOracle.resetUserTimestampInner(gta.leadership, tso, ignoreSmaller, skipUpperBoundCheck)
}

// GenerateTSO is used to generate the given number of TSOs.
// Make sure you have initialized the TSO allocator before calling this method.
// Basically, there are two ways to generate a Global TSO:
//  1. The old way to generate a normal TSO from memory directly, which makes the TSO service node become single point.
//  2. The new way to generate a Global TSO by synchronizing with all other Local TSO Allocators.
//
// And for the new way, there are two different strategies:
//  1. Collect the max Local TSO from all Local TSO Allocator leaders and write it back to them as MaxTS.
//  2. Estimate a MaxTS and try to write it to all Local TSO Allocator leaders directly to reduce the RTT.
//     During the process, if the estimated MaxTS is not accurate, it will fallback to the collecting way.
func (tta *TaasTSOAllocator) GenerateTSO(count uint32) (pdpb.Timestamp, error) {
	
	return tta.timestampOracle.getTS(tta.leadership, count, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Persist MaxTS into memory, and etcd if needed
	var currentGlobalTSO *pdpb.Timestamp
	if currentGlobalTSO, err = gta.getCurrentTSO(); err != nil {
		log.Error("global tso allocator gets the current global tso in memory failed", errs.ZapError(err))
		continue
	}
	if tsoutil.CompareTimestamp(currentGlobalTSO, &globalTSOResp) < 0 {
		// Update the Global TSO in memory
		if err = tta.timestampOracle.resetUserTimestamp(gta.leadership, tsoutil.GenerateTS(&globalTSOResp), true); err != nil {
			log.Error("update tso in memory failed", errs.ZapError(err))
			continue
			}
		}
		}
		// 5. Check leadership again before we returning the response.
		if !gta.leadership.Check() {
			tsoCounter.WithLabelValues("not_leader_anymore", gta.timestampOracle.dcLocation).Inc()
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("not the pd leader anymore")
			}
		// 5. Check leadership again before we returning the response.
		if !gta.leadership.Check() {
			tsoCounter.WithLabelValues("not_leader_anymore", gta.timestampOracle.dcLocation).Inc()
			return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("not the pd leader anymore")
		}
	}

	// 6. Differentiate the logical part to make the TSO unique globally by giving it a unique suffix in the whole cluster
	globalTSOResp.Logical = gta.timestampOracle.differentiateLogical(globalTSOResp.GetLogical(), suffixBits)
	globalTSOResp.SuffixBits = uint32(suffixBits)
	return globalTSOResp, nil
	}
	return pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("global tso allocator maximum number of retries exceeded")
}


// If not, it will be replaced with the new max Local TSO and return.
func (tta *TaasTSOAllocator) SyncMaxTS(
	ctx context.Context,
	dcLocationMap map[string]DCLocationInfo,
	maxTSO *pdpb.Timestamp,
	skipCheck bool,
) error {
	originalMaxTSO := *maxTSO
	for i := 0; i < syncMaxRetryCount; i++ {
		// Collect all allocator leaders' client URLs
		allocatorLeaders := make(map[string]*pdpb.Member)
		for dcLocation := range dcLocationMap {
			allocator, err := gta.allocatorManager.GetAllocator(dcLocation)
			if err != nil {
				return err
			}
			allocatorLeader := allocator.(*LocalTSOAllocator).GetAllocatorLeader()
			if allocatorLeader.GetMemberId() == 0 {
				return errs.ErrSyncMaxTS.FastGenByArgs(fmt.Sprintf("%s does not have the local allocator leader yet", dcLocation))
			}
			allocatorLeaders[dcLocation] = allocatorLeader
		}
		leaderURLs := make([]string, 0)
		for _, allocator := range allocatorLeaders {
			// Check if its client URLs are empty
			if len(allocator.GetClientUrls()) < 1 {
				continue
			}
			leaderURL := allocator.GetClientUrls()[0]
			if slice.NoneOf(leaderURLs, func(i int) bool { return leaderURLs[i] == leaderURL }) {
				leaderURLs = append(leaderURLs, leaderURL)
			}
		}
		// Prepare to make RPC requests concurrently
		respCh := make(chan *syncResp, len(leaderURLs))
		wg := sync.WaitGroup{}
		request := &pdpb.SyncMaxTSRequest{
			Header: &pdpb.RequestHeader{
				SenderId: gta.allocatorManager.member.ID(),
			},
			SkipCheck: skipCheck,
			MaxTs:     maxTSO,
		}
		for _, leaderURL := range leaderURLs {
			leaderConn, err := gta.allocatorManager.getOrCreateGRPCConn(ctx, leaderURL)
			if err != nil {
				return err
			}
			// Send SyncMaxTSRequest to all allocator leaders concurrently.
			wg.Add(1)
			go func(ctx context.Context, conn *grpc.ClientConn, respCh chan<- *syncResp) {
				defer logutil.LogPanic()
				defer wg.Done()
				syncMaxTSResp := &syncResp{}
				syncCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
				startTime := time.Now()
				syncMaxTSResp.rpcRes, syncMaxTSResp.err = pdpb.NewPDClient(conn).SyncMaxTS(syncCtx, request)
				// Including RPC request -> RPC processing -> RPC response
				syncMaxTSResp.rtt = time.Since(startTime)
				cancel()
				respCh <- syncMaxTSResp
				if syncMaxTSResp.err != nil {
					log.Error("sync max ts rpc failed, got an error", zap.String("local-allocator-leader-url", leaderConn.Target()), errs.ZapError(err))
					return
				}
				if syncMaxTSResp.rpcRes.GetHeader().GetError() != nil {
					log.Error("sync max ts rpc failed, got an error", zap.String("local-allocator-leader-url", leaderConn.Target()),
						errs.ZapError(errors.Errorf("%s", syncMaxTSResp.rpcRes.GetHeader().GetError().String())))
					return
				}
			}(ctx, leaderConn, respCh)
		}
		wg.Wait()
		close(respCh)
		var (
			errList   []error
			syncedDCs []string
			maxTSORtt time.Duration
		)
		// Iterate each response to handle the error and compare MaxTSO.
		for resp := range respCh {
			if resp.err != nil {
				errList = append(errList, resp.err)
			}
			// If any error occurs, just jump out of the loop.
			if len(errList) != 0 {
				break
			}
			if resp.rpcRes == nil {
				return errs.ErrSyncMaxTS.FastGenByArgs("got nil response")
			}
			if skipCheck {
				// Set all the Local TSOs to the maxTSO unconditionally, so the MaxLocalTS in response should be nil.
				if resp.rpcRes.GetMaxLocalTs() != nil {
					return errs.ErrSyncMaxTS.FastGenByArgs("got non-nil max local ts in the second sync phase")
				}
				syncedDCs = append(syncedDCs, resp.rpcRes.GetSyncedDcs()...)
			} else {
				// Compare and get the max one
				if tsoutil.CompareTimestamp(resp.rpcRes.GetMaxLocalTs(), maxTSO) > 0 {
					*maxTSO = *(resp.rpcRes.GetMaxLocalTs())
					if resp.rtt > maxTSORtt {
						maxTSORtt = resp.rtt
					}
				}
				syncedDCs = append(syncedDCs, resp.rpcRes.GetSyncedDcs()...)
			}
		}
		// We need to collect all info needed to ensure the consistency of TSO.
		// So if any error occurs, the synchronization process will fail directly.
		if len(errList) != 0 {
			return errs.ErrSyncMaxTS.FastGenWithCause(errList)
		}
		// Check whether all dc-locations have been considered during the synchronization and retry once if any dc-location missed.
		if ok, unsyncedDCs := gta.checkSyncedDCs(dcLocationMap, syncedDCs); !ok {
			log.Info("unsynced dc-locations found, will retry", zap.Bool("skip-check", skipCheck), zap.Strings("synced-DCs", syncedDCs), zap.Strings("unsynced-DCs", unsyncedDCs))
			if i < syncMaxRetryCount-1 {
				// maxTSO should remain the same.
				*maxTSO = originalMaxTSO
				// To make sure we have the latest dc-location info
				gta.allocatorManager.ClusterDCLocationChecker()
				continue
			}
			return errs.ErrSyncMaxTS.FastGenByArgs(fmt.Sprintf("unsynced dc-locations found, skip-check: %t, synced dc-locations: %+v, unsynced dc-locations: %+v", skipCheck, syncedDCs, unsyncedDCs))
		}
		// Update the sync RTT to help estimate MaxTS later.
		if maxTSORtt != 0 {
			gta.setSyncRTT(maxTSORtt.Milliseconds())
		}
	}
	return nil
}

func (tta *TaasTSOAllocator) checkSyncedDCs(dcLocationMap map[string]DCLocationInfo, syncedDCs []string) (bool, []string) {
	var unsyncedDCs []string
	for dcLocation := range dcLocationMap {
		if slice.NoneOf(syncedDCs, func(i int) bool { return syncedDCs[i] == dcLocation }) {
			unsyncedDCs = append(unsyncedDCs, dcLocation)
		}
	}
	log.Debug("check unsynced dc-locations", zap.Strings("unsynced-DCs", unsyncedDCs), zap.Strings("synced-DCs", syncedDCs))
	return len(unsyncedDCs) == 0, unsyncedDCs
}

func (tta *TaasTSOAllocator) getCurrentTSO() (*pdpb.Timestamp, error) {
	currentPhysical, currentLogical := gta.timestampOracle.getTSO()
	if currentPhysical == typeutil.ZeroTime {
		return &pdpb.Timestamp{}, errs.ErrGenerateTimestamp.FastGenByArgs("timestamp in memory isn't initialized")
	}
	return tsoutil.GenerateTimestamp(currentPhysical, uint64(currentLogical)), nil
}

// Reset is used to reset the TSO allocator.
func (tta *TaasTSOAllocator) Reset() {
	tsoAllocatorRole.WithLabelValues(gta.timestampOracle.dcLocation).Set(0)
	gta.timestampOracle.ResetTimestamp()
}
