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

package storelimit

import (
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	// minSnapSize is the min value to check the windows has enough size.
	minSnapSize = 10
)

// SlidingWindows is a multi sliding windows
type SlidingWindows struct {
	mu      syncutil.RWMutex
	windows []*window
}

// NewSlidingWindows is the construct of SlidingWindows.
func NewSlidingWindows(cap float64) *SlidingWindows {
	if cap < 0 {
		cap = minSnapSize
	}
	windows := make([]*window, constant.PriorityLevelLen)
	for i := 0; i < int(constant.PriorityLevelLen); i++ {
		windows[i] = newWindow(int64(cap) >> i)
	}
	return &SlidingWindows{
		windows: windows,
	}
}

// Reset resets the capacity of the sliding windows.
// It doesn't clear all the used, only set the capacity.
func (s *SlidingWindows) Reset(cap float64, typ Type) {
	if typ != SendSnapshot {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cap < 0 {
		cap = minSnapSize
	}
	for i, v := range s.windows {
		v.reset(int64(cap) >> i)
	}
}

// GetUsed returns the used size in the sliding windows.
func (s *SlidingWindows) GetUsed() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	used := int64(0)
	for _, v := range s.windows {
		used += v.getUsed()
	}
	return used
}

// Available returns whether the token can be taken.
// The order of checking windows is from low to high.
// It checks the given window finally if the lower window has no free size.
func (s *SlidingWindows) Available(_ int64, typ Type, level constant.PriorityLevel) bool {
	if typ != SendSnapshot {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := 0; i <= int(level); i++ {
		if s.windows[i].available() {
			return true
		}
	}
	return false
}

// Take tries to take the token.
// It will consume the given window finally if the lower window has no free size.
func (s *SlidingWindows) Take(token int64, typ Type, level constant.PriorityLevel) bool {
	if typ != SendSnapshot {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i <= int(level); i++ {
		if s.windows[i].take(token) {
			return true
		}
	}
	return false
}

// Ack indicates that some executing operator has been finished.
// The order of refilling windows is from high to low.
// It will refill the highest window first.
func (s *SlidingWindows) Ack(token int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := constant.PriorityLevelLen - 1; i >= 0; i-- {
		if token = s.windows[i].ack(token); token <= 0 {
			break
		}
	}
}

// window is a sliding window.
// |---used---|----available---|
// |-------- capacity  --------|
type window struct {
	// capacity is the max size of the window.
	capacity int64
	// used is the size of the operators that are executing.
	// the used can be larger than the capacity.but allow
	// one operator to exceed the capacity at most.
	used int64
	// the count of operators in the window.
	count uint
}

func newWindow(capacity int64) *window {
	// the min capacity is minSnapSize to allow one operator at least.
	if capacity < minSnapSize {
		capacity = minSnapSize
	}
	return &window{capacity: capacity, used: 0, count: 0}
}

func (s *window) reset(capacity int64) {
	// the min capacity is minSnapSize to allow one operator at least.
	if capacity < minSnapSize {
		capacity = minSnapSize
	}
	s.capacity = capacity
}

// Ack indicates that some executing operator has been finished.
func (s *window) ack(token int64) int64 {
	if s.used == 0 {
		return token
	}
	available := int64(0)
	if s.used > token {
		s.used -= token
	} else {
		available = token - s.used
		s.used = 0
	}
	s.count--
	return available
}

// getUsed returns the used size in the sliding windows.
func (s *window) getUsed() int64 {
	return s.used
}

func (s *window) available() bool {
	return s.used+minSnapSize <= s.capacity
}

func (s *window) take(token int64) bool {
	if !s.available() {
		return false
	}
	s.used += token
	s.count++
	return true
}
