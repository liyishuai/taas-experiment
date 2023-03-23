// Copyright 2017 TiKV Project Authors.
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

package typeutil

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMinUint64(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	re.Equal(uint64(1), MinUint64(1, 2))
	re.Equal(uint64(1), MinUint64(2, 1))
	re.Equal(uint64(1), MinUint64(1, 1))
}

func TestMaxUint64(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	re.Equal(uint64(2), MaxUint64(1, 2))
	re.Equal(uint64(2), MaxUint64(2, 1))
	re.Equal(uint64(1), MaxUint64(1, 1))
}

func TestMinDuration(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	re.Equal(time.Second, MinDuration(time.Minute, time.Second))
	re.Equal(time.Second, MinDuration(time.Second, time.Minute))
	re.Equal(time.Second, MinDuration(time.Second, time.Second))
}

func TestEqualFloat(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	f1 := rand.Float64()
	re.True(Float64Equal(f1, f1*1.000))
	re.True(Float64Equal(f1, f1/1.000))
}
