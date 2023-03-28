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

//go:build tso_full_test || tso_function_test
// +build tso_full_test tso_function_test

package tso_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"gitlab.alibaba-inc.com/zelu.wjz/taasplugin/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

// There are three kinds of ways to generate a TSO:
// 1. Normal Global TSO, the normal way to get a global TSO from the PD leader,
//    a.k.a the single Global TSO Allocator.
// 2. Normal Local TSO, the new way to get a local TSO may from any of PD servers,
//    a.k.a the Local TSO Allocator leader.
// 3. Synchronized global TSO, the new way to get a global TSO from the PD leader,
//    which will coordinate and synchronize a TSO with other Local TSO Allocator
//    leaders.

func TestRequestFollower(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()

	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()

	var followerServer *tests.TestServer
	for _, s := range cluster.GetServers() {
		if s.GetConfig().Name != cluster.GetLeader() {
			followerServer = s
		}
	}
	re.NotNil(followerServer)

	grpcPDClient := testutil.MustNewGrpcClient(re, followerServer.GetAddr())
	clusterID := followerServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      1,
		DcLocation: tso.GlobalDCLocation,
	}
	ctx = grpcutil.BuildForwardContext(ctx, followerServer.GetAddr())
	tsoClient, err := grpcPDClient.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()

	start := time.Now()
	re.NoError(tsoClient.Send(req))
	_, err = tsoClient.Recv()
	re.Error(err)
	re.Contains(err.Error(), "generate timestamp failed")

	// Requesting follower should fail fast, or the unavailable time will be
	// too long.
	re.Less(time.Since(start), time.Second)
}

// In some cases, when a TSO request arrives, the SyncTimestamp may not finish yet.
// This test is used to simulate this situation and verify that the retry mechanism.
func TestDelaySyncTimestamp(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 2)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	cluster.WaitLeader()

	var leaderServer, nextLeaderServer *tests.TestServer
	leaderServer = cluster.GetServer(cluster.GetLeader())
	re.NotNil(leaderServer)
	for _, s := range cluster.GetServers() {
		if s.GetConfig().Name != cluster.GetLeader() {
			nextLeaderServer = s
		}
	}
	re.NotNil(nextLeaderServer)

	grpcPDClient := testutil.MustNewGrpcClient(re, nextLeaderServer.GetAddr())
	clusterID := nextLeaderServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      1,
		DcLocation: tso.GlobalDCLocation,
	}

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp", `return(true)`))

	// Make the old leader resign and wait for the new leader to get a lease
	leaderServer.ResignLeader()
	re.True(nextLeaderServer.WaitLeader())

	ctx = grpcutil.BuildForwardContext(ctx, nextLeaderServer.GetAddr())
	tsoClient, err := grpcPDClient.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()
	re.NoError(tsoClient.Send(req))
	resp, err := tsoClient.Recv()
	re.NoError(err)
	re.NotNil(checkAndReturnTimestampResponse(re, req, resp))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp"))
}

func TestLogicalOverflow(t *testing.T) {
	re := require.New(t)

	runCase := func(updateInterval time.Duration) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, serverName string) {
			conf.TSOUpdatePhysicalInterval = typeutil.Duration{Duration: updateInterval}
		})
		defer cluster.Destroy()
		re.NoError(err)
		re.NoError(cluster.RunInitialServers())
		cluster.WaitLeader()

		leaderServer := cluster.GetServer(cluster.GetLeader())
		grpcPDClient := testutil.MustNewGrpcClient(re, leaderServer.GetAddr())
		clusterID := leaderServer.GetClusterID()

		tsoClient, err := grpcPDClient.Tso(ctx)
		re.NoError(err)
		defer tsoClient.CloseSend()

		begin := time.Now()
		for i := 0; i < 2; i += 1 { // the 2nd request may (but not must) overflow, as max logical interval is 262144
			req := &pdpb.TsoRequest{
				Header:     testutil.NewRequestHeader(clusterID),
				Count:      150000,
				DcLocation: tso.GlobalDCLocation,
			}
			re.NoError(tsoClient.Send(req))
			_, err = tsoClient.Recv()
			re.NoError(err)
		}
		elapse := time.Since(begin)
		if updateInterval >= 20*time.Millisecond { // on small interval, the physical may update before overflow
			re.GreaterOrEqual(elapse, updateInterval)
		}
		re.Less(elapse, updateInterval+20*time.Millisecond) // additional 20ms for gRPC latency
	}

	for _, updateInterval := range []int{1, 5, 30, 50} {
		runCase(time.Duration(updateInterval) * time.Millisecond)
	}
}
