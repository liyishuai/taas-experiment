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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/election"
)

// TaasTSOAllocator is a global TSO allocator using TaaS algorithm.
type TaasTSOAllocator struct {
	// for global TSO synchronization
	allocatorManager *AllocatorManager
	// leadership is used to get etcd client
	taasNode *taasNode
}

// NewTaasTSOAllocator creates a new Taas TSO allocator.
func NewTaasTSOAllocator(
	am *AllocatorManager,
	leadership *election.Leadership,
) Allocator {
	tta := &TaasTSOAllocator{
		allocatorManager: am,
		taasNode: &taasNode{
			client:                 leadership.GetClient(),
			nodeId:                 int64(am.member.ID()),
			rootPath:               am.rootPath,
			ttsPath:                "tts",
			storage:                am.storage,
			saveInterval:           am.saveInterval,
			updatePhysicalInterval: am.updatePhysicalInterval,
			maxResetTSGap:          am.maxResetTSGap,
			dcLocation:             TaaSLocation,
			taasMux: &taasObject{
				tsHigh:  0,
				tsLow:   int64(am.member.ID()),
				tsLimit: taasLimitUpdateLevel,
			},
		},
	}
	return tta
}

// Initialize will initialize the created taas allocator.
func (tta *TaasTSOAllocator) Initialize(int) error {
	// keep same with pd global tso
	// initialize from etcd
	return tta.taasNode.Initialize()
}

// IsInitialize is used to indicates whether this allocator is initialized.
func (tta *TaasTSOAllocator) IsInitialize() bool {
	return tta.taasNode.isInitialized()
}

// empty func
func (tta *TaasTSOAllocator) UpdateTSO() error {
	return nil
}

// SetTSO sets the physical part with given TSO.
func (tta *TaasTSOAllocator) SetTSO(tso uint64, ignoreSmaller, skipUpperBoundCheck bool) error {
	return tta.taasNode.setTaasHigh(int64(tso))
}

// Unused
func (tta *TaasTSOAllocator) GenerateTSO(count uint32) (pdpb.Timestamp, error) {
	panic("Unexpected calling of GenerateTSO on TaasTSOAllocator")
}

// Reset is used to reset the TSO allocator.
func (tta *TaasTSOAllocator) Reset() {
	tta.taasNode.reserveTaasLimit(0)
}

// For taas
func (tta *TaasTSOAllocator) GenerateTaasTSO(ts *pdpb.Timestamp) (pdpb.Timestamp, error) {
	return tta.taasNode.generateTaasTSO(ts)
}
