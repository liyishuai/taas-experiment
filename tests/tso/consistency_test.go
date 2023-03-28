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

package tso

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"gitlab.alibaba-inc.com/zelu.wjz/taasplugin/pkg/pdpb"
	"gitlab.alibaba-inc.com/zelu.wjz/taasplugin/pkg/tsopb"
	"github.com/stretchr/testify/suite"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	tsopkg "github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/tempurl"
	pd "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/mcs"
	"google.golang.org/grpc"
)

type tsoConsistencyTestSuite struct {
	suite.Suite
	legacy bool

	ctx    context.Context
	cancel context.CancelFunc

	// The PD cluster.
	cluster *tests.TestCluster
	// pdLeaderServer is the leader server of the PD cluster.
	pdLeaderServer *tests.TestServer
	// tsoServer is the TSO service provider.
	tsoServer        *tso.Server
	tsoServerCleanup func()
	tsoClientConn    *grpc.ClientConn

	pdClient  pdpb.PDClient
	tsoClient tsopb.TSOClient
}

func TestLegacyTSOConsistency(t *testing.T) {
	suite.Run(t, &tsoConsistencyTestSuite{
		legacy: true,
	})
}

func TestMicroserviceTSOConsistency(t *testing.T) {
	suite.Run(t, &tsoConsistencyTestSuite{
		legacy: false,
	})
}

func (suite *tsoConsistencyTestSuite) SetupSuite() {
	re := suite.Require()

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	if suite.legacy {
		suite.cluster, err = tests.NewTestCluster(suite.ctx, serverCount)
	} else {
		suite.cluster, err = tests.NewTestAPICluster(suite.ctx, serverCount)
	}
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	backendEndpoints := suite.pdLeaderServer.GetAddr()
	if suite.legacy {
		suite.pdClient = pd.MustNewGrpcClient(re, backendEndpoints)
	} else {
		suite.tsoServer, suite.tsoServerCleanup = mcs.StartSingleTSOTestServer(suite.ctx, re, backendEndpoints, tempurl.Alloc())
		suite.tsoClientConn, suite.tsoClient = tso.MustNewGrpcClient(re, suite.tsoServer.GetAddr())
	}
}

func (suite *tsoConsistencyTestSuite) TearDownSuite() {
	suite.cancel()
	if !suite.legacy {
		suite.tsoClientConn.Close()
		suite.tsoServerCleanup()
	}
	suite.cluster.Destroy()
}

func (suite *tsoConsistencyTestSuite) getClusterID() uint64 {
	if suite.legacy {
		return suite.pdLeaderServer.GetServer().ClusterID()
	}
	return suite.tsoServer.ClusterID()
}

func (suite *tsoConsistencyTestSuite) request(ctx context.Context, count uint32) *pdpb.Timestamp {
	re := suite.Require()
	clusterID := suite.getClusterID()
	if suite.legacy {
		req := &pdpb.TsoRequest{
			Header:     &pdpb.RequestHeader{ClusterId: clusterID},
			DcLocation: tsopkg.GlobalDCLocation,
			Count:      count,
		}
		tsoClient, err := suite.pdClient.Tso(ctx)
		re.NoError(err)
		defer tsoClient.CloseSend()
		re.NoError(tsoClient.Send(req))
		resp, err := tsoClient.Recv()
		re.NoError(err)
		return checkAndReturnTimestampResponse(re, resp)
	}
	req := &tsopb.TsoRequest{
		Header:     &tsopb.RequestHeader{ClusterId: clusterID},
		DcLocation: tsopkg.GlobalDCLocation,
		Count:      count,
	}
	tsoClient, err := suite.tsoClient.Tso(ctx)
	re.NoError(err)
	defer tsoClient.CloseSend()
	re.NoError(tsoClient.Send(req))
	resp, err := tsoClient.Recv()
	re.NoError(err)
	return checkAndReturnTimestampResponse(re, resp)
}

func (suite *tsoConsistencyTestSuite) TestRequestTSOConcurrently() {
	suite.requestTSOConcurrently()
	// Test Global TSO after the leader change
	suite.pdLeaderServer.GetServer().GetMember().ResetLeader()
	suite.cluster.WaitLeader()
	suite.requestTSOConcurrently()
}

func (suite *tsoConsistencyTestSuite) requestTSOConcurrently() {
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}
			var ts *pdpb.Timestamp
			for j := 0; j < tsoRequestRound; j++ {
				ts = suite.request(ctx, tsoCount)
				// Check whether the TSO fallbacks
				suite.Equal(1, tsoutil.CompareTimestamp(ts, last))
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

func (suite *tsoConsistencyTestSuite) TestFallbackTSOConsistency() {
	re := suite.Require()

	// Re-create the cluster to enable the failpoints.
	suite.TearDownSuite()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fallBackSync", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fallBackUpdate", `return(true)`))
	suite.SetupSuite()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fallBackSync"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fallBackUpdate"))

	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}
			var ts *pdpb.Timestamp
			for j := 0; j < tsoRequestRound; j++ {
				ts = suite.request(ctx, tsoCount)
				re.Equal(1, tsoutil.CompareTimestamp(ts, last))
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}
