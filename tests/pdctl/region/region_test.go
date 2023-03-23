// Copyright 2019 TiKV Project Authors.
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

package region_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func TestRegionKeyFormat(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	url := cluster.GetConfig().GetClientURL()
	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	pdctl.MustPutStore(re, leaderServer.GetServer(), store)

	cmd := pdctlCmd.GetRootCmd()
	output, err := pdctl.ExecuteCommand(cmd, "-u", url, "region", "key", "--format=raw", " ")
	re.NoError(err)
	re.NotContains(string(output), "unknown flag")
}

func TestRegion(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	pdctl.MustPutStore(re, leaderServer.GetServer(), store)

	downPeer := &metapb.Peer{Id: 8, StoreId: 3}
	r1 := pdctl.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"),
		core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1),
		core.SetRegionVersion(1), core.SetApproximateSize(1), core.SetApproximateKeys(100),
		core.SetPeers([]*metapb.Peer{
			{Id: 1, StoreId: 1},
			{Id: 5, StoreId: 2},
			{Id: 6, StoreId: 3},
			{Id: 7, StoreId: 4},
		}))
	r2 := pdctl.MustPutRegion(re, cluster, 2, 1, []byte("b"), []byte("c"),
		core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2),
		core.SetRegionVersion(3), core.SetApproximateSize(144), core.SetApproximateKeys(14400),
	)
	r3 := pdctl.MustPutRegion(re, cluster, 3, 1, []byte("c"), []byte("d"),
		core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3),
		core.SetRegionVersion(2), core.SetApproximateSize(30), core.SetApproximateKeys(3000),
		core.WithDownPeers([]*pdpb.PeerStats{{Peer: downPeer, DownSeconds: 3600}}),
		core.WithPendingPeers([]*metapb.Peer{downPeer}), core.WithLearners([]*metapb.Peer{{Id: 3, StoreId: 1}}))
	r4 := pdctl.MustPutRegion(re, cluster, 4, 1, []byte("d"), []byte("e"),
		core.SetWrittenBytes(100), core.SetReadBytes(100), core.SetRegionConfVer(1),
		core.SetRegionVersion(1), core.SetApproximateSize(10), core.SetApproximateKeys(1000),
	)
	defer cluster.Destroy()

	var testRegionsCases = []struct {
		args   []string
		expect []*core.RegionInfo
	}{
		// region command
		{[]string{"region"}, leaderServer.GetRegions()},
		// region sibling <region_id> command
		{[]string{"region", "sibling", "2"}, leaderServer.GetAdjacentRegions(leaderServer.GetRegionInfoByID(2))},
		// region store <store_id> command
		{[]string{"region", "store", "1"}, leaderServer.GetStoreRegions(1)},
		{[]string{"region", "store", "1"}, []*core.RegionInfo{r1, r2, r3, r4}},
		// region topread [limit] command
		{[]string{"region", "topread", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool { return a.GetBytesRead() < b.GetBytesRead() }, 2)},
		// region topwrite [limit] command
		{[]string{"region", "topwrite", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() }, 2)},
		// region topconfver [limit] command
		{[]string{"region", "topconfver", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
			return a.GetMeta().GetRegionEpoch().GetConfVer() < b.GetMeta().GetRegionEpoch().GetConfVer()
		}, 2)},
		// region topversion [limit] command
		{[]string{"region", "topversion", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
			return a.GetMeta().GetRegionEpoch().GetVersion() < b.GetMeta().GetRegionEpoch().GetVersion()
		}, 2)},
		// region topsize [limit] command
		{[]string{"region", "topsize", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
			return a.GetApproximateSize() < b.GetApproximateSize()
		}, 2)},
		// region topkeys [limit] command
		{[]string{"region", "topkeys", "2"}, api.TopNRegions(leaderServer.GetRegions(), func(a, b *core.RegionInfo) bool {
			return a.GetApproximateKeys() < b.GetApproximateKeys()
		}, 2)},
		// region check extra-peer command
		{[]string{"region", "check", "extra-peer"}, []*core.RegionInfo{r1}},
		// region check miss-peer command
		{[]string{"region", "check", "miss-peer"}, []*core.RegionInfo{r2, r3, r4}},
		// region check pending-peer command
		{[]string{"region", "check", "pending-peer"}, []*core.RegionInfo{r3}},
		// region check down-peer command
		{[]string{"region", "check", "down-peer"}, []*core.RegionInfo{r3}},
		// region check learner-peer command
		{[]string{"region", "check", "learner-peer"}, []*core.RegionInfo{r3}},
		// region check empty-region command
		{[]string{"region", "check", "empty-region"}, []*core.RegionInfo{r1}},
		// region check undersized-region command
		{[]string{"region", "check", "undersized-region"}, []*core.RegionInfo{r1, r4}},
		// region check oversized-region command
		{[]string{"region", "check", "oversized-region"}, []*core.RegionInfo{r2}},
		// region keys --format=raw <start_key> <end_key> <limit> command
		{[]string{"region", "keys", "--format=raw", "b"}, []*core.RegionInfo{r2, r3, r4}},
		// region keys --format=raw <start_key> <end_key> <limit> command
		{[]string{"region", "keys", "--format=raw", "b", "", "2"}, []*core.RegionInfo{r2, r3}},
		// region keys --format=hex <start_key> <end_key> <limit> command
		{[]string{"region", "keys", "--format=hex", "63", "", "2"}, []*core.RegionInfo{r3, r4}},
		// region keys --format=hex <start_key> <end_key> <limit> command
		{[]string{"region", "keys", "--format=hex", "", "63", "2"}, []*core.RegionInfo{r1, r2}},
		// region keys --format=raw <start_key> <end_key> <limit> command
		{[]string{"region", "keys", "--format=raw", "b", "d"}, []*core.RegionInfo{r2, r3}},
		// region keys --format=hex <start_key> <end_key> <limit> command
		{[]string{"region", "keys", "--format=hex", "63", "65"}, []*core.RegionInfo{r3, r4}},
		// region keys --format=hex <start_key> <end_key> <limit> command
		{[]string{"region", "keys", "--format=hex", "63", "65", "1"}, []*core.RegionInfo{r3}},
	}

	for _, testCase := range testRegionsCases {
		args := append([]string{"-u", pdAddr}, testCase.args...)
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		regions := &api.RegionsInfo{}
		re.NoError(json.Unmarshal(output, regions))
		pdctl.CheckRegionsInfo(re, regions, testCase.expect)
	}

	var testRegionCases = []struct {
		args   []string
		expect *core.RegionInfo
	}{
		// region <region_id> command
		{[]string{"region", "1"}, leaderServer.GetRegionInfoByID(1)},
		// region key --format=raw <key> command
		{[]string{"region", "key", "--format=raw", "b"}, r2},
		// region key --format=hex <key> command
		{[]string{"region", "key", "--format=hex", "62"}, r2},
		// issue #2351
		{[]string{"region", "key", "--format=hex", "622f62"}, r2},
	}

	for _, testCase := range testRegionCases {
		args := append([]string{"-u", pdAddr}, testCase.args...)
		output, err := pdctl.ExecuteCommand(cmd, args...)
		re.NoError(err)
		region := &api.RegionInfo{}
		re.NoError(json.Unmarshal(output, region))
		pdctl.CheckRegionInfo(re, region, testCase.expect)
	}

	// Test region range-holes.
	r5 := pdctl.MustPutRegion(re, cluster, 5, 1, []byte("x"), []byte("z"))
	output, err := pdctl.ExecuteCommand(cmd, []string{"-u", pdAddr, "region", "range-holes"}...)
	re.NoError(err)
	rangeHoles := new([][]string)
	re.NoError(json.Unmarshal(output, rangeHoles))
	re.Equal([][]string{
		{"", core.HexRegionKeyStr(r1.GetStartKey())},
		{core.HexRegionKeyStr(r4.GetEndKey()), core.HexRegionKeyStr(r5.GetStartKey())},
		{core.HexRegionKeyStr(r5.GetEndKey()), ""},
	}, *rangeHoles)
}
