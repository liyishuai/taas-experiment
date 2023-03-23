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

package storelimit

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core/constant"
)

func TestStoreLimit(t *testing.T) {
	re := require.New(t)
	rate := int64(15)
	limit := NewStoreRateLimit(float64(rate))
	re.True(limit.Available(influence*rate, AddPeer, constant.Low))
	re.True(limit.Take(influence*rate, AddPeer, constant.Low))
	re.False(limit.Take(influence, AddPeer, constant.Low))

	limit.Reset(float64(rate), AddPeer)
	re.False(limit.Available(influence, AddPeer, constant.Low))
	re.False(limit.Take(influence, AddPeer, constant.Low))

	limit.Reset(0, AddPeer)
	re.True(limit.Available(influence, AddPeer, constant.Low))
	re.True(limit.Take(influence, AddPeer, constant.Low))
}

func TestSlidingWindow(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	capacity := int64(10)
	s := NewSlidingWindows(float64(capacity))
	re.Len(s.windows, int(constant.PriorityLevelLen))
	// capacity:[10, 10, 10, 10]
	for i, v := range s.windows {
		cap := capacity >> i
		if cap < minSnapSize {
			cap = minSnapSize
		}
		re.EqualValues(v.capacity, cap)
	}
	// case 0: test core.Low level
	re.True(s.Available(capacity, SendSnapshot, constant.Low))
	re.True(s.Take(capacity, SendSnapshot, constant.Low))
	re.False(s.Available(capacity, SendSnapshot, constant.Low))
	s.Ack(capacity)
	re.True(s.Available(capacity, SendSnapshot, constant.Low))

	// case 1: it will occupy the normal window size not the core.High window.
	re.True(s.Take(capacity, SendSnapshot, constant.High))
	re.EqualValues(capacity, s.GetUsed())
	re.EqualValues(0, s.windows[constant.High].getUsed())
	s.Ack(capacity)
	re.EqualValues(s.GetUsed(), 0)

	// case 2: it will occupy the core.High window size if the normal window is full.
	capacity = 2000
	s.Reset(float64(capacity), SendSnapshot)
	re.True(s.Take(capacity-minSnapSize, SendSnapshot, constant.Low))
	re.True(s.Take(capacity-minSnapSize, SendSnapshot, constant.Low))
	re.False(s.Take(capacity, SendSnapshot, constant.Low))
	re.True(s.Take(capacity-minSnapSize, SendSnapshot, constant.Medium))
	re.False(s.Take(capacity-minSnapSize, SendSnapshot, constant.Medium))
	re.EqualValues(s.GetUsed(), capacity+capacity+capacity-minSnapSize*3)
	s.Ack(capacity - minSnapSize)
	s.Ack(capacity - minSnapSize)
	re.Equal(s.GetUsed(), capacity-minSnapSize)

	// case 3: skip the type is not the SendSnapshot
	for i := 0; i < 10; i++ {
		re.True(s.Take(capacity, AddPeer, constant.Low))
	}
}

func TestWindow(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	capacity := int64(100 * 10)
	s := newWindow(capacity)

	//case1: token maybe greater than the capacity.
	token := capacity + 10
	re.True(s.take(token))
	re.False(s.take(token))
	re.EqualValues(s.ack(token), 0)
	re.True(s.take(token))
	re.EqualValues(s.ack(token), 0)
	re.Equal(s.ack(token), token)
	re.EqualValues(s.getUsed(), 0)

	// case2: the capacity of the window must greater than the minSnapSize.
	s.reset(minSnapSize - 1)
	re.EqualValues(s.capacity, minSnapSize)
	re.True(s.take(minSnapSize))
	re.EqualValues(s.ack(minSnapSize*2), minSnapSize)
	re.EqualValues(s.getUsed(), 0)
}
