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

package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/storelimit"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func TestStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	stores := []*api.StoreInfo{
		{
			Store: &api.MetaStore{
				Store: &metapb.Store{
					Id:            1,
					State:         metapb.StoreState_Up,
					NodeState:     metapb.NodeState_Serving,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
		{
			Store: &api.MetaStore{
				Store: &metapb.Store{
					Id:            3,
					State:         metapb.StoreState_Up,
					NodeState:     metapb.NodeState_Serving,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Up.String(),
			},
		},
		{
			Store: &api.MetaStore{
				Store: &metapb.Store{
					Id:            2,
					State:         metapb.StoreState_Tombstone,
					NodeState:     metapb.NodeState_Removed,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Tombstone.String(),
			},
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())

	for _, store := range stores {
		pdctl.MustPutStore(re, leaderServer.GetServer(), store.Store.Store)
	}
	defer cluster.Destroy()

	// store command
	args := []string{"-u", pdAddr, "store"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storesInfo := new(api.StoresInfo)
	re.NoError(json.Unmarshal(output, &storesInfo))

	pdctl.CheckStoresInfo(re, storesInfo.Stores, stores[:2])

	// store --state=<query states> command
	args = []string{"-u", pdAddr, "store", "--state", "Up,Tombstone"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NotContains(string(output), "\"state\":")
	storesInfo = new(api.StoresInfo)
	re.NoError(json.Unmarshal(output, &storesInfo))

	pdctl.CheckStoresInfo(re, storesInfo.Stores, stores)

	// store <store_id> command
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storeInfo := new(api.StoreInfo)
	re.NoError(json.Unmarshal(output, &storeInfo))

	pdctl.CheckStoresInfo(re, []*api.StoreInfo{storeInfo}, stores[:1])
	re.Nil(storeInfo.Store.Labels)

	// store <store_id> label command
	labelTestCases := []struct {
		args              []string
		newArgs           []string
		expectLabelLength int
		expectKeys        []string
		expectValues      []string
	}{
		{ // add label
			args:              []string{"-u", pdAddr, "store", "label", "1", "zone", "cn"},
			newArgs:           []string{"-u", pdAddr, "store", "label", "1", "zone=cn"},
			expectLabelLength: 1,
			expectKeys:        []string{"zone"},
			expectValues:      []string{"cn"},
		},
		{ // update label
			args:              []string{"-u", pdAddr, "store", "label", "1", "zone", "us", "language", "English"},
			newArgs:           []string{"-u", pdAddr, "store", "label", "1", "zone=us", "language=English"},
			expectLabelLength: 2,
			expectKeys:        []string{"zone", "language"},
			expectValues:      []string{"us", "English"},
		},
		{ // rewrite label
			args:              []string{"-u", pdAddr, "store", "label", "1", "zone", "uk", "-f"},
			newArgs:           []string{"-u", pdAddr, "store", "label", "1", "zone=uk", "--rewrite"},
			expectLabelLength: 1,
			expectKeys:        []string{"zone"},
			expectValues:      []string{"uk"},
		},
		{ // delete label
			args:              []string{"-u", pdAddr, "store", "label", "1", "zone", "--delete"},
			newArgs:           []string{"-u", pdAddr, "store", "label", "1", "zone", "--delete"},
			expectLabelLength: 0,
			expectKeys:        []string{""},
			expectValues:      []string{""},
		},
	}
	for i := 0; i <= 1; i++ {
		for _, testcase := range labelTestCases {
			switch {
			case i == 0: // old way
				args = testcase.args
			case i == 1: // new way
				args = testcase.newArgs
			}
			cmd := ctl.GetRootCmd()
			storeInfo := new(api.StoreInfo)
			_, err = pdctl.ExecuteCommand(cmd, args...)
			re.NoError(err)
			args = []string{"-u", pdAddr, "store", "1"}
			output, err = pdctl.ExecuteCommand(cmd, args...)
			re.NoError(err)
			re.NoError(json.Unmarshal(output, &storeInfo))
			labels := storeInfo.Store.Labels
			re.Len(labels, testcase.expectLabelLength)
			for i := 0; i < testcase.expectLabelLength; i++ {
				re.Equal(testcase.expectKeys[i], labels[i].Key)
				re.Equal(testcase.expectValues[i], labels[i].Value)
			}
		}
	}

	// store weight <store_id> <leader_weight> <region_weight> command
	re.Equal(float64(1), storeInfo.Status.LeaderWeight)
	re.Equal(float64(1), storeInfo.Status.RegionWeight)
	args = []string{"-u", pdAddr, "store", "weight", "1", "5", "10"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NoError(json.Unmarshal(output, &storeInfo))

	re.Equal(float64(5), storeInfo.Status.LeaderWeight)
	re.Equal(float64(10), storeInfo.Status.RegionWeight)

	// store limit <store_id> <rate>
	args = []string{"-u", pdAddr, "store", "limit", "1", "10"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	limit := leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.AddPeer)
	re.Equal(float64(10), limit)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	re.Equal(float64(10), limit)

	// store limit <store_id> <rate> <type>
	args = []string{"-u", pdAddr, "store", "limit", "1", "5", "remove-peer"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	re.Equal(float64(5), limit)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.AddPeer)
	re.Equal(float64(10), limit)

	// store limit all <rate>
	args = []string{"-u", pdAddr, "store", "limit", "all", "20"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	limit1 := leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.AddPeer)
	limit2 := leaderServer.GetRaftCluster().GetStoreLimitByType(2, storelimit.AddPeer)
	limit3 := leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.AddPeer)
	re.Equal(float64(20), limit1)
	re.Equal(float64(20), limit2)
	re.Equal(float64(20), limit3)
	limit1 = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	limit2 = leaderServer.GetRaftCluster().GetStoreLimitByType(2, storelimit.RemovePeer)
	limit3 = leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.RemovePeer)
	re.Equal(float64(20), limit1)
	re.Equal(float64(20), limit2)
	re.Equal(float64(20), limit3)

	re.NoError(leaderServer.Stop())
	re.NoError(leaderServer.Run())

	cluster.WaitLeader()
	storesLimit := leaderServer.GetPersistOptions().GetAllStoresLimit()
	re.Equal(float64(20), storesLimit[1].AddPeer)
	re.Equal(float64(20), storesLimit[1].RemovePeer)

	// store limit all <rate> <type>
	args = []string{"-u", pdAddr, "store", "limit", "all", "25", "remove-peer"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	limit1 = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	limit3 = leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.RemovePeer)
	re.Equal(float64(25), limit1)
	re.Equal(float64(25), limit3)
	limit2 = leaderServer.GetRaftCluster().GetStoreLimitByType(2, storelimit.RemovePeer)
	re.Equal(float64(25), limit2)

	// store limit all <key> <value> <rate> <type>
	args = []string{"-u", pdAddr, "store", "label", "1", "zone=uk"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "limit", "all", "zone", "uk", "20", "remove-peer"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	limit1 = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	re.Equal(float64(20), limit1)

	// store limit all 0 is invalid
	args = []string{"-u", pdAddr, "store", "limit", "all", "0"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "rate should be a number that > 0")

	// store limit <type>
	args = []string{"-u", pdAddr, "store", "limit"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)

	allAddPeerLimit := make(map[string]map[string]interface{})
	json.Unmarshal(output, &allAddPeerLimit)
	re.Equal(float64(20), allAddPeerLimit["1"]["add-peer"].(float64))
	re.Equal(float64(20), allAddPeerLimit["3"]["add-peer"].(float64))
	_, ok := allAddPeerLimit["2"]["add-peer"]
	re.False(ok)

	args = []string{"-u", pdAddr, "store", "limit", "remove-peer"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)

	allRemovePeerLimit := make(map[string]map[string]interface{})
	json.Unmarshal(output, &allRemovePeerLimit)
	re.Equal(float64(20), allRemovePeerLimit["1"]["remove-peer"].(float64))
	re.Equal(float64(25), allRemovePeerLimit["3"]["remove-peer"].(float64))
	_, ok = allRemovePeerLimit["2"]["add-peer"]
	re.False(ok)

	// put enough stores for replica.
	for id := 1000; id <= 1005; id++ {
		store2 := &metapb.Store{
			Id:            uint64(id),
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		}
		pdctl.MustPutStore(re, leaderServer.GetServer(), store2)
	}

	// store delete <store_id> command
	storeInfo.Store.State = metapb.StoreState(metapb.StoreState_value[storeInfo.Store.StateName])
	re.Equal(metapb.StoreState_Up, storeInfo.Store.State)
	args = []string{"-u", pdAddr, "store", "delete", "1"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storeInfo = new(api.StoreInfo)
	re.NoError(json.Unmarshal(output, &storeInfo))

	storeInfo.Store.State = metapb.StoreState(metapb.StoreState_value[storeInfo.Store.StateName])
	re.Equal(metapb.StoreState_Offline, storeInfo.Store.State)

	// store check status
	args = []string{"-u", pdAddr, "store", "check", "Offline"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "\"id\": 1,")
	args = []string{"-u", pdAddr, "store", "check", "Tombstone"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "\"id\": 2,")
	args = []string{"-u", pdAddr, "store", "check", "Up"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "\"id\": 3,")
	args = []string{"-u", pdAddr, "store", "check", "Invalid_State"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "Unknown state: Invalid_state")

	// store cancel-delete <store_id> command
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	re.Equal(storelimit.Unlimited, limit)
	args = []string{"-u", pdAddr, "store", "cancel-delete", "1"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "1"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storeInfo = new(api.StoreInfo)
	re.NoError(json.Unmarshal(output, &storeInfo))

	re.Equal(metapb.StoreState_Up, storeInfo.Store.State)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(1, storelimit.RemovePeer)
	re.Equal(20.0, limit)

	// store delete addr <address>
	args = []string{"-u", pdAddr, "store", "delete", "addr", "tikv3"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.Equal("Success!\n", string(output))
	re.NoError(err)

	args = []string{"-u", pdAddr, "store", "3"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storeInfo = new(api.StoreInfo)
	re.NoError(json.Unmarshal(output, &storeInfo))

	storeInfo.Store.State = metapb.StoreState(metapb.StoreState_value[storeInfo.Store.StateName])
	re.Equal(metapb.StoreState_Offline, storeInfo.Store.State)

	// store cancel-delete addr <address>
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.RemovePeer)
	re.Equal(storelimit.Unlimited, limit)
	args = []string{"-u", pdAddr, "store", "cancel-delete", "addr", "tikv3"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.Equal("Success!\n", string(output))
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "3"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storeInfo = new(api.StoreInfo)
	re.NoError(json.Unmarshal(output, &storeInfo))

	re.Equal(metapb.StoreState_Up, storeInfo.Store.State)
	limit = leaderServer.GetRaftCluster().GetStoreLimitByType(3, storelimit.RemovePeer)
	re.Equal(25.0, limit)

	// store remove-tombstone
	args = []string{"-u", pdAddr, "store", "check", "Tombstone"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storesInfo = new(api.StoresInfo)
	re.NoError(json.Unmarshal(output, &storesInfo))

	re.Equal(1, storesInfo.Count)
	args = []string{"-u", pdAddr, "store", "remove-tombstone"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "check", "Tombstone"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	storesInfo = new(api.StoresInfo)
	re.NoError(json.Unmarshal(output, &storesInfo))

	re.Equal(0, storesInfo.Count)

	// It should be called after stores remove-tombstone.
	args = []string{"-u", pdAddr, "stores", "show", "limit"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NotContains(string(output), "PANIC")

	args = []string{"-u", pdAddr, "stores", "show", "limit", "remove-peer"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NotContains(string(output), "PANIC")

	args = []string{"-u", pdAddr, "stores", "show", "limit", "add-peer"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.NotContains(string(output), "PANIC")
	// store limit-scene
	args = []string{"-u", pdAddr, "store", "limit-scene"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	scene := &storelimit.Scene{}
	err = json.Unmarshal(output, scene)
	re.NoError(err)
	re.Equal(storelimit.DefaultScene(storelimit.AddPeer), scene)

	// store limit-scene <scene> <rate>
	args = []string{"-u", pdAddr, "store", "limit-scene", "idle", "200"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "limit-scene"}
	scene = &storelimit.Scene{}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	err = json.Unmarshal(output, scene)
	re.NoError(err)
	re.Equal(200, scene.Idle)

	// store limit-scene <scene> <rate> <type>
	args = []string{"-u", pdAddr, "store", "limit-scene", "idle", "100", "remove-peer"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	args = []string{"-u", pdAddr, "store", "limit-scene", "remove-peer"}
	scene = &storelimit.Scene{}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	err = json.Unmarshal(output, scene)
	re.NoError(err)
	re.Equal(100, scene.Idle)

	// store limit all 201 is invalid for all
	args = []string{"-u", pdAddr, "store", "limit", "all", "201"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "rate should less than")

	// store limit all 201 is invalid for label
	args = []string{"-u", pdAddr, "store", "limit", "all", "engine", "key", "201", "add-peer"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	re.Contains(string(output), "rate should less than")
}

// https://github.com/tikv/pd/issues/5024
func TestTombstoneStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	stores := []*api.StoreInfo{
		{
			Store: &api.MetaStore{
				Store: &metapb.Store{
					Id:            2,
					State:         metapb.StoreState_Tombstone,
					NodeState:     metapb.NodeState_Removed,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Tombstone.String(),
			},
		},
		{
			Store: &api.MetaStore{
				Store: &metapb.Store{
					Id:            3,
					State:         metapb.StoreState_Tombstone,
					NodeState:     metapb.NodeState_Removed,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Tombstone.String(),
			},
		},
		{
			Store: &api.MetaStore{
				Store: &metapb.Store{
					Id:            4,
					State:         metapb.StoreState_Tombstone,
					NodeState:     metapb.NodeState_Removed,
					LastHeartbeat: time.Now().UnixNano(),
				},
				StateName: metapb.StoreState_Tombstone.String(),
			},
		},
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())

	for _, store := range stores {
		pdctl.MustPutStore(re, leaderServer.GetServer(), store.Store.Store)
	}
	defer cluster.Destroy()
	pdctl.MustPutRegion(re, cluster, 1, 2, []byte("a"), []byte("b"), core.SetWrittenBytes(3000000000), core.SetReportInterval(0, statistics.WriteReportInterval))
	pdctl.MustPutRegion(re, cluster, 2, 3, []byte("b"), []byte("c"), core.SetWrittenBytes(3000000000), core.SetReportInterval(0, statistics.WriteReportInterval))
	// store remove-tombstone
	args := []string{"-u", pdAddr, "store", "remove-tombstone"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	message := string(output)
	re.Contains(message, "2")
	re.Contains(message, "3")
}
