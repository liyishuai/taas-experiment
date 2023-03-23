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
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
)

func TestRetryQuota(t *testing.T) {
	re := require.New(t)

	q := newRetryQuota()
	store1 := core.NewStoreInfo(&metapb.Store{Id: 1})
	store2 := core.NewStoreInfo(&metapb.Store{Id: 2})
	keepStores := []*core.StoreInfo{store1}

	// test GetLimit
	re.Equal(10, q.GetLimit(store1))

	// test Attenuate
	for _, expected := range []int{5, 2, 1, 1, 1} {
		q.Attenuate(store1)
		re.Equal(expected, q.GetLimit(store1))
	}

	// test GC
	re.Equal(10, q.GetLimit(store2))
	q.Attenuate(store2)
	re.Equal(5, q.GetLimit(store2))
	q.GC(keepStores)
	re.Equal(1, q.GetLimit(store1))
	re.Equal(10, q.GetLimit(store2))

	// test ResetLimit
	q.ResetLimit(store1)
	re.Equal(10, q.GetLimit(store1))
}
