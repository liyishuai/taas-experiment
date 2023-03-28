// Copyright 2016 TiKV Project Authors.
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

package api

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"gitlab.alibaba-inc.com/zelu.wjz/taasplugin/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"go.uber.org/goleak"
)

var (
	// testDialClient used to dial http request. only used for test.
	testDialClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	store = &metapb.Store{
		Id:        1,
		Address:   "localhost",
		NodeState: metapb.NodeState_Serving,
	}
	peers = []*metapb.Peer{
		{
			Id:      2,
			StoreId: store.GetId(),
		},
	}
	region = &metapb.Region{
		Id: 8,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func mustNewServer(re *require.Assertions, opts ...func(cfg *config.Config)) (*server.Server, testutil.CleanupFunc) {
	_, svrs, cleanup := mustNewCluster(re, 1, opts...)
	return svrs[0], cleanup
}

var zapLogOnce sync.Once

func mustNewCluster(re *require.Assertions, num int, opts ...func(cfg *config.Config)) ([]*config.Config, []*server.Server, testutil.CleanupFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	svrs := make([]*server.Server, 0, num)
	cfgs := server.NewTestMultiConfig(assertutil.CheckerWithNilAssert(re), num)

	ch := make(chan *server.Server, num)
	for _, cfg := range cfgs {
		go func(cfg *config.Config) {
			err := logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
			re.NoError(err)
			zapLogOnce.Do(func() {
				log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
			})
			for _, opt := range opts {
				opt(cfg)
			}
			s, err := server.CreateServer(ctx, cfg, nil, NewHandler)
			re.NoError(err)
			err = s.Run()
			re.NoError(err)
			ch <- s
		}(cfg)
	}

	for i := 0; i < num; i++ {
		svr := <-ch
		svrs = append(svrs, svr)
	}
	close(ch)
	// wait etcd and http servers
	server.MustWaitLeader(re, svrs)

	// clean up
	clean := func() {
		cancel()
		for _, s := range svrs {
			s.Close()
		}
		for _, cfg := range cfgs {
			testutil.CleanServer(cfg.DataDir)
		}
	}

	return cfgs, svrs, clean
}

func mustBootstrapCluster(re *require.Assertions, s *server.Server) {
	grpcPDClient := testutil.MustNewGrpcClient(re, s.GetAddr())
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(s.ClusterID()),
		Store:  store,
		Region: region,
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Equal(pdpb.ErrorType_OK, resp.GetHeader().GetError().GetType())
}

type serviceTestSuite struct {
	suite.Suite
	svr     *server.Server
	cleanup testutil.CleanupFunc
}

func TestServiceTestSuite(t *testing.T) {
	suite.Run(t, new(serviceTestSuite))
}

func (suite *serviceTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	mustBootstrapCluster(re, suite.svr)
	mustPutStore(re, suite.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
}

func (suite *serviceTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *serviceTestSuite) TestServiceLabels() {
	accessPaths := suite.svr.GetServiceLabels("Profile")
	suite.Len(accessPaths, 1)
	suite.Equal("/pd/api/v1/debug/pprof/profile", accessPaths[0].Path)
	suite.Equal("", accessPaths[0].Method)
	serviceLabel := suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/debug/pprof/profile", ""))
	suite.Equal("Profile", serviceLabel)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/debug/pprof/profile", http.MethodGet))
	suite.Equal("Profile", serviceLabel)

	accessPaths = suite.svr.GetServiceLabels("GetSchedulerConfig")
	suite.Len(accessPaths, 1)
	suite.Equal("/pd/api/v1/scheduler-config", accessPaths[0].Path)
	suite.Equal("", accessPaths[0].Method)

	accessPaths = suite.svr.GetServiceLabels("ResignLeader")
	suite.Len(accessPaths, 1)
	suite.Equal("/pd/api/v1/leader/resign", accessPaths[0].Path)
	suite.Equal(http.MethodPost, accessPaths[0].Method)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/leader/resign", http.MethodPost))
	suite.Equal("ResignLeader", serviceLabel)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/leader/resign", http.MethodGet))
	suite.Equal("", serviceLabel)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/leader/resign", ""))
	suite.Equal("", serviceLabel)

	accessPaths = suite.svr.GetServiceLabels("QueryMetric")
	suite.Len(accessPaths, 4)
	sort.Slice(accessPaths, func(i, j int) bool {
		if accessPaths[i].Path == accessPaths[j].Path {
			return accessPaths[i].Method < accessPaths[j].Method
		}
		return accessPaths[i].Path < accessPaths[j].Path
	})
	suite.Equal("/pd/api/v1/metric/query", accessPaths[0].Path)
	suite.Equal(http.MethodGet, accessPaths[0].Method)
	suite.Equal("/pd/api/v1/metric/query", accessPaths[1].Path)
	suite.Equal(http.MethodPost, accessPaths[1].Method)
	suite.Equal("/pd/api/v1/metric/query_range", accessPaths[2].Path)
	suite.Equal(http.MethodGet, accessPaths[2].Method)
	suite.Equal("/pd/api/v1/metric/query_range", accessPaths[3].Path)
	suite.Equal(http.MethodPost, accessPaths[3].Method)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/metric/query", http.MethodPost))
	suite.Equal("QueryMetric", serviceLabel)
	serviceLabel = suite.svr.GetAPIAccessServiceLabel(
		apiutil.NewAccessPath("/pd/api/v1/metric/query", http.MethodGet))
	suite.Equal("QueryMetric", serviceLabel)
}
