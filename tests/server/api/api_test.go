// Copyright 2018 TiKV Project Authors.
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

package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/utils/apiutil/serverapi"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	"go.uber.org/goleak"
)

// dialClient used to dial http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestReconnect(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, serverName string) {
		conf.TickInterval = typeutil.Duration{Duration: 50 * time.Millisecond}
		conf.ElectionInterval = typeutil.Duration{Duration: 250 * time.Millisecond}
	})
	re.NoError(err)
	defer cluster.Destroy()

	re.NoError(cluster.RunInitialServers())

	// Make connections to followers.
	// Make sure they proxy requests to the leader.
	leader := cluster.WaitLeader()
	for name, s := range cluster.GetServers() {
		if name != leader {
			res, err := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
			re.NoError(err)
			res.Body.Close()
			re.Equal(http.StatusOK, res.StatusCode)
		}
	}

	// Close the leader and wait for a new one.
	err = cluster.GetServer(leader).Stop()
	re.NoError(err)
	newLeader := cluster.WaitLeader()
	re.NotEmpty(newLeader)

	// Make sure they proxy requests to the new leader.
	for name, s := range cluster.GetServers() {
		if name != leader {
			testutil.Eventually(re, func() bool {
				res, err := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				re.NoError(err)
				defer res.Body.Close()
				return res.StatusCode == http.StatusOK
			})
		}
	}

	// Close the new leader and then we have only one node.
	re.NoError(cluster.GetServer(newLeader).Stop())

	// Request will fail with no leader.
	for name, s := range cluster.GetServers() {
		if name != leader && name != newLeader {
			testutil.Eventually(re, func() bool {
				res, err := http.Get(s.GetConfig().AdvertiseClientUrls + "/pd/api/v1/version")
				re.NoError(err)
				defer res.Body.Close()
				return res.StatusCode == http.StatusServiceUnavailable
			})
		}
	}
}

type middlewareTestSuite struct {
	suite.Suite
	cleanup func()
	cluster *tests.TestCluster
}

func TestMiddlewareTestSuite(t *testing.T) {
	suite.Run(t, new(middlewareTestSuite))
}

func (suite *middlewareTestSuite) SetupSuite() {
	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/api/enableFailpointAPI", "return(true)"))
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 3)
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.cluster = cluster
}

func (suite *middlewareTestSuite) TearDownSuite() {
	suite.NoError(failpoint.Disable("github.com/tikv/pd/server/api/enableFailpointAPI"))
	suite.cleanup()
	suite.cluster.Destroy()
}

func (suite *middlewareTestSuite) TestRequestInfoMiddleware() {
	suite.NoError(failpoint.Enable("github.com/tikv/pd/server/api/addRequestInfoMiddleware", "return(true)"))
	leader := suite.cluster.GetServer(suite.cluster.GetLeader())

	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	suite.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	suite.NoError(err)
	resp.Body.Close()
	suite.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())

	labels := make(map[string]interface{})
	labels["testkey"] = "testvalue"
	data, _ = json.Marshal(labels)
	resp, err = dialClient.Post(leader.GetAddr()+"/pd/api/v1/debug/pprof/profile?force=true", "application/json", bytes.NewBuffer(data))
	suite.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	suite.NoError(err)
	suite.Equal(http.StatusOK, resp.StatusCode)

	suite.Equal("Profile", resp.Header.Get("service-label"))
	suite.Equal("{\"force\":[\"true\"]}", resp.Header.Get("url-param"))
	suite.Equal("{\"testkey\":\"testvalue\"}", resp.Header.Get("body-param"))
	suite.Equal("HTTP/1.1/POST:/pd/api/v1/debug/pprof/profile", resp.Header.Get("method"))
	suite.Equal("anonymous", resp.Header.Get("component"))
	suite.Equal("127.0.0.1", resp.Header.Get("ip"))

	input = map[string]interface{}{
		"enable-audit": "false",
	}
	data, err = json.Marshal(input)
	suite.NoError(err)
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err = dialClient.Do(req)
	suite.NoError(err)
	resp.Body.Close()
	suite.False(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())

	header := mustRequestSuccess(suite.Require(), leader.GetServer())
	suite.Equal("", header.Get("service-label"))

	suite.NoError(failpoint.Disable("github.com/tikv/pd/server/api/addRequestInfoMiddleware"))
}

func BenchmarkDoRequestWithServiceMiddleware(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequestWithLogAudit(leader)
	}
	cancel()
	cluster.Destroy()
}

func (suite *middlewareTestSuite) TestRateLimitMiddleware() {
	leader := suite.cluster.GetServer(suite.cluster.GetLeader())
	input := map[string]interface{}{
		"enable-rate-limit": "true",
	}
	data, err := json.Marshal(input)
	suite.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	suite.NoError(err)
	resp.Body.Close()
	suite.Equal(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled(), true)

	// returns StatusOK when no rate-limit config
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
	resp, err = dialClient.Do(req)
	suite.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	suite.NoError(err)
	suite.Equal(resp.StatusCode, http.StatusOK)
	input = make(map[string]interface{})
	input["type"] = "label"
	input["label"] = "SetLogLevel"
	input["qps"] = 0.5
	input["concurrency"] = 1
	jsonBody, err := json.Marshal(input)
	suite.NoError(err)
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config/rate-limit", bytes.NewBuffer(jsonBody))
	resp, err = dialClient.Do(req)
	suite.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	suite.NoError(err)
	suite.Equal(resp.StatusCode, http.StatusOK)

	for i := 0; i < 3; i++ {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = dialClient.Do(req)
		suite.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		suite.NoError(err)
		if i > 0 {
			suite.Equal(resp.StatusCode, http.StatusTooManyRequests)
			suite.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
		} else {
			suite.Equal(resp.StatusCode, http.StatusOK)
		}
	}

	// qps = 0.5, so sleep 2s
	time.Sleep(time.Second * 2)
	for i := 0; i < 2; i++ {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = dialClient.Do(req)
		suite.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		suite.NoError(err)
		if i > 0 {
			suite.Equal(resp.StatusCode, http.StatusTooManyRequests)
			suite.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
		} else {
			suite.Equal(resp.StatusCode, http.StatusOK)
		}
	}

	// test only sleep 1s
	time.Sleep(time.Second)
	for i := 0; i < 2; i++ {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = dialClient.Do(req)
		suite.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		suite.NoError(err)
		suite.Equal(resp.StatusCode, http.StatusTooManyRequests)
		suite.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
	}

	// resign leader
	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	var servers []*server.Server
	for _, s := range suite.cluster.GetServers() {
		servers = append(servers, s.GetServer())
	}
	server.MustWaitLeader(suite.Require(), servers)
	leader = suite.cluster.GetServer(suite.cluster.GetLeader())
	suite.Equal(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled(), true)
	cfg, ok := leader.GetServer().GetRateLimitConfig().LimiterConfig["SetLogLevel"]
	suite.Equal(ok, true)
	suite.Equal(cfg.ConcurrencyLimit, uint64(1))
	suite.Equal(cfg.QPS, 0.5)
	suite.Equal(cfg.QPSBurst, 1)

	for i := 0; i < 3; i++ {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = dialClient.Do(req)
		suite.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		suite.NoError(err)
		if i > 0 {
			suite.Equal(resp.StatusCode, http.StatusTooManyRequests)
			suite.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
		} else {
			suite.Equal(resp.StatusCode, http.StatusOK)
		}
	}

	// qps = 0.5, so sleep 2s
	time.Sleep(time.Second * 2)
	for i := 0; i < 2; i++ {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = dialClient.Do(req)
		suite.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		suite.NoError(err)
		if i > 0 {
			suite.Equal(resp.StatusCode, http.StatusTooManyRequests)
			suite.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
		} else {
			suite.Equal(resp.StatusCode, http.StatusOK)
		}
	}

	// test only sleep 1s
	time.Sleep(time.Second)
	for i := 0; i < 2; i++ {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = dialClient.Do(req)
		suite.NoError(err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		suite.NoError(err)
		suite.Equal(resp.StatusCode, http.StatusTooManyRequests)
		suite.Equal(string(data), fmt.Sprintf("%s\n", http.StatusText(http.StatusTooManyRequests)))
	}

	input = map[string]interface{}{
		"enable-rate-limit": "false",
	}
	data, err = json.Marshal(input)
	suite.NoError(err)
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err = dialClient.Do(req)
	suite.NoError(err)
	resp.Body.Close()
	suite.Equal(leader.GetServer().GetServiceMiddlewarePersistOptions().IsRateLimitEnabled(), false)

	for i := 0; i < 3; i++ {
		req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
		resp, err = dialClient.Do(req)
		suite.NoError(err)
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		suite.NoError(err)
		suite.Equal(resp.StatusCode, http.StatusOK)
	}
}

func (suite *middlewareTestSuite) TestAuditPrometheusBackend() {
	leader := suite.cluster.GetServer(suite.cluster.GetLeader())
	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	suite.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	suite.NoError(err)
	resp.Body.Close()
	suite.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())
	timeUnix := time.Now().Unix() - 20
	req, _ = http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/api/v1/trend?from=%d", leader.GetAddr(), timeUnix), nil)
	resp, err = dialClient.Do(req)
	suite.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	suite.NoError(err)

	req, _ = http.NewRequest(http.MethodGet, leader.GetAddr()+"/metrics", nil)
	resp, err = dialClient.Do(req)
	suite.NoError(err)
	defer resp.Body.Close()
	content, _ := io.ReadAll(resp.Body)
	output := string(content)
	suite.Contains(output, "pd_service_audit_handling_seconds_count{component=\"anonymous\",ip=\"127.0.0.1\",method=\"HTTP\",service=\"GetTrend\"} 1")

	// resign to test persist config
	oldLeaderName := leader.GetServer().Name()
	leader.GetServer().GetMember().ResignEtcdLeader(leader.GetServer().Context(), oldLeaderName, "")
	var servers []*server.Server
	for _, s := range suite.cluster.GetServers() {
		servers = append(servers, s.GetServer())
	}
	server.MustWaitLeader(suite.Require(), servers)
	leader = suite.cluster.GetServer(suite.cluster.GetLeader())

	timeUnix = time.Now().Unix() - 20
	req, _ = http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/api/v1/trend?from=%d", leader.GetAddr(), timeUnix), nil)
	resp, err = dialClient.Do(req)
	suite.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	suite.NoError(err)

	req, _ = http.NewRequest(http.MethodGet, leader.GetAddr()+"/metrics", nil)
	resp, err = dialClient.Do(req)
	suite.NoError(err)
	defer resp.Body.Close()
	content, _ = io.ReadAll(resp.Body)
	output = string(content)
	suite.Contains(output, "pd_service_audit_handling_seconds_count{component=\"anonymous\",ip=\"127.0.0.1\",method=\"HTTP\",service=\"GetTrend\"} 2")

	input = map[string]interface{}{
		"enable-audit": "false",
	}
	data, err = json.Marshal(input)
	suite.NoError(err)
	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err = dialClient.Do(req)
	suite.NoError(err)
	resp.Body.Close()
	suite.False(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())
}

func (suite *middlewareTestSuite) TestAuditLocalLogBackend() {
	tempStdoutFile, _ := os.CreateTemp("/tmp", "pd_tests")
	cfg := &log.Config{}
	cfg.File.Filename = tempStdoutFile.Name()
	cfg.Level = "info"
	lg, p, _ := log.InitLogger(cfg)
	log.ReplaceGlobals(lg, p)
	leader := suite.cluster.GetServer(suite.cluster.GetLeader())
	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, err := json.Marshal(input)
	suite.NoError(err)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, err := dialClient.Do(req)
	suite.NoError(err)
	resp.Body.Close()
	suite.True(leader.GetServer().GetServiceMiddlewarePersistOptions().IsAuditEnabled())

	req, _ = http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/admin/log", strings.NewReader("\"info\""))
	resp, err = dialClient.Do(req)
	suite.NoError(err)
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	b, _ := os.ReadFile(tempStdoutFile.Name())
	suite.Contains(string(b), "audit log")
	suite.NoError(err)
	suite.Equal(http.StatusOK, resp.StatusCode)

	os.Remove(tempStdoutFile.Name())
}

func BenchmarkDoRequestWithLocalLogAudit(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequestWithLogAudit(leader)
	}
	cancel()
	cluster.Destroy()
}

func BenchmarkDoRequestWithPrometheusAudit(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	input := map[string]interface{}{
		"enable-audit": "true",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequestWithPrometheus(leader)
	}
	cancel()
	cluster.Destroy()
}

func BenchmarkDoRequestWithoutServiceMiddleware(b *testing.B) {
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	cluster, _ := tests.NewTestCluster(ctx, 1)
	cluster.RunInitialServers()
	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	input := map[string]interface{}{
		"enable-audit": "false",
	}
	data, _ := json.Marshal(input)
	req, _ := http.NewRequest(http.MethodPost, leader.GetAddr()+"/pd/api/v1/service-middleware/config", bytes.NewBuffer(data))
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		doTestRequestWithLogAudit(leader)
	}
	cancel()
	cluster.Destroy()
}

func doTestRequestWithLogAudit(srv *tests.TestServer) {
	req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/pd/api/v1/admin/cache/regions", srv.GetAddr()), nil)
	req.Header.Set("component", "test")
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
}

func doTestRequestWithPrometheus(srv *tests.TestServer) {
	timeUnix := time.Now().Unix() - 20
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/pd/api/v1/trend?from=%d", srv.GetAddr(), timeUnix), nil)
	req.Header.Set("component", "test")
	resp, _ := dialClient.Do(req)
	resp.Body.Close()
}

type redirectorTestSuite struct {
	suite.Suite
	cleanup func()
	cluster *tests.TestCluster
}

func TestRedirectorTestSuite(t *testing.T) {
	suite.Run(t, new(redirectorTestSuite))
}

func (suite *redirectorTestSuite) SetupSuite() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, serverName string) {
		conf.TickInterval = typeutil.Duration{Duration: 50 * time.Millisecond}
		conf.ElectionInterval = typeutil.Duration{Duration: 250 * time.Millisecond}
	})
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader(), 0)
	suite.cluster = cluster
}

func (suite *redirectorTestSuite) TearDownSuite() {
	suite.cleanup()
	suite.cluster.Destroy()
}

func (suite *redirectorTestSuite) TestRedirect() {
	re := suite.Require()
	leader := suite.cluster.GetServer(suite.cluster.GetLeader())
	suite.NotNil(leader)
	header := mustRequestSuccess(re, leader.GetServer())
	header.Del("Date")
	for _, svr := range suite.cluster.GetServers() {
		if svr != leader {
			h := mustRequestSuccess(re, svr.GetServer())
			h.Del("Date")
			suite.Equal(h, header)
		}
	}
}

func (suite *redirectorTestSuite) TestAllowFollowerHandle() {
	// Find a follower.
	var follower *server.Server
	leader := suite.cluster.GetServer(suite.cluster.GetLeader())
	for _, svr := range suite.cluster.GetServers() {
		if svr != leader {
			follower = svr.GetServer()
			break
		}
	}

	addr := follower.GetAddr() + "/pd/api/v1/version"
	request, err := http.NewRequest(http.MethodGet, addr, nil)
	suite.NoError(err)
	request.Header.Add(serverapi.PDAllowFollowerHandle, "true")
	resp, err := dialClient.Do(request)
	suite.NoError(err)
	suite.Equal("", resp.Header.Get(serverapi.PDRedirectorHeader))
	defer resp.Body.Close()
	suite.Equal(http.StatusOK, resp.StatusCode)
	_, err = io.ReadAll(resp.Body)
	suite.NoError(err)
}

func (suite *redirectorTestSuite) TestNotLeader() {
	// Find a follower.
	var follower *server.Server
	leader := suite.cluster.GetServer(suite.cluster.GetLeader())
	for _, svr := range suite.cluster.GetServers() {
		if svr != leader {
			follower = svr.GetServer()
			break
		}
	}

	addr := follower.GetAddr() + "/pd/api/v1/version"
	// Request to follower without redirectorHeader is OK.
	request, err := http.NewRequest(http.MethodGet, addr, nil)
	suite.NoError(err)
	resp, err := dialClient.Do(request)
	suite.NoError(err)
	defer resp.Body.Close()
	suite.Equal(http.StatusOK, resp.StatusCode)
	_, err = io.ReadAll(resp.Body)
	suite.NoError(err)

	// Request to follower with redirectorHeader will fail.
	request.RequestURI = ""
	request.Header.Set(serverapi.PDRedirectorHeader, "pd")
	resp1, err := dialClient.Do(request)
	suite.NoError(err)
	defer resp1.Body.Close()
	suite.NotEqual(http.StatusOK, resp1.StatusCode)
	_, err = io.ReadAll(resp1.Body)
	suite.NoError(err)
}

func mustRequestSuccess(re *require.Assertions, s *server.Server) http.Header {
	resp, err := dialClient.Get(s.GetAddr() + "/pd/api/v1/version")
	re.NoError(err)
	defer resp.Body.Close()
	_, err = io.ReadAll(resp.Body)
	re.NoError(err)
	re.Equal(http.StatusOK, resp.StatusCode)
	return resp.Header
}

func TestRemovingProgress(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, serverName string) {
		conf.Replication.MaxReplicas = 1
	})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(re, leader.GetAddr())
	clusterID := leader.GetClusterID()
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  &metapb.Store{Id: 1, Address: "127.0.0.1:0"},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            3,
			State:         metapb.StoreState_Up,
			NodeState:     metapb.NodeState_Serving,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	for _, store := range stores {
		pdctl.MustPutStore(re, leader.GetServer(), store)
	}
	pdctl.MustPutRegion(re, cluster, 1000, 1, []byte("a"), []byte("b"), core.SetApproximateSize(60))
	pdctl.MustPutRegion(re, cluster, 1001, 2, []byte("c"), []byte("d"), core.SetApproximateSize(30))
	pdctl.MustPutRegion(re, cluster, 1002, 1, []byte("e"), []byte("f"), core.SetApproximateSize(50))
	pdctl.MustPutRegion(re, cluster, 1003, 2, []byte("g"), []byte("h"), core.SetApproximateSize(40))

	// no store removing
	output := sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=removing", http.MethodGet, http.StatusNotFound)
	re.Contains((string(output)), "no progress found for the action")
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=2", http.MethodGet, http.StatusNotFound)
	re.Contains((string(output)), "no progress found for the given store ID")

	// remove store 1 and store 2
	_ = sendRequest(re, leader.GetAddr()+"/pd/api/v1/store/1", http.MethodDelete, http.StatusOK)
	_ = sendRequest(re, leader.GetAddr()+"/pd/api/v1/store/2", http.MethodDelete, http.StatusOK)

	// size is not changed.
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=removing", http.MethodGet, http.StatusOK)
	var p api.Progress
	re.NoError(json.Unmarshal(output, &p))
	re.Equal("removing", p.Action)
	re.Equal(0.0, p.Progress)
	re.Equal(0.0, p.CurrentSpeed)
	re.Equal(math.MaxFloat64, p.LeftSeconds)

	// update size
	pdctl.MustPutRegion(re, cluster, 1000, 1, []byte("a"), []byte("b"), core.SetApproximateSize(20))
	pdctl.MustPutRegion(re, cluster, 1001, 2, []byte("c"), []byte("d"), core.SetApproximateSize(10))

	// is not prepared
	time.Sleep(2 * time.Second)
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=removing", http.MethodGet, http.StatusOK)
	re.NoError(json.Unmarshal(output, &p))
	re.Equal("removing", p.Action)
	re.Equal(0.0, p.Progress)
	re.Equal(0.0, p.CurrentSpeed)
	re.Equal(math.MaxFloat64, p.LeftSeconds)

	leader.GetRaftCluster().SetPrepared()
	time.Sleep(2 * time.Second)
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=removing", http.MethodGet, http.StatusOK)
	re.NoError(json.Unmarshal(output, &p))
	re.Equal("removing", p.Action)
	// store 1: (60-20)/(60+50) ~= 0.36
	// store 2: (30-10)/(30+40) ~= 0.28
	// average progress ~= (0.36+0.28)/2 = 0.32
	re.Equal("0.32", fmt.Sprintf("%.2f", p.Progress))
	// store 1: 40/10s = 4
	// store 2: 20/10s = 2
	// average speed = (2+4)/2 = 33
	re.Equal(3.0, p.CurrentSpeed)
	// store 1: (20+50)/4 = 17.5s
	// store 2: (10+40)/2 = 25s
	// average time = (17.5+25)/2 = 21.25s
	re.Equal(21.25, p.LeftSeconds)

	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=2", http.MethodGet, http.StatusOK)
	re.NoError(json.Unmarshal(output, &p))
	re.Equal("removing", p.Action)
	// store 2: (30-10)/(30+40) ~= 0.285
	re.Equal("0.29", fmt.Sprintf("%.2f", p.Progress))
	// store 2: 20/10s = 2
	re.Equal(2.0, p.CurrentSpeed)
	// store 2: (10+40)/2 = 25s
	re.Equal(25.0, p.LeftSeconds)

	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
}

func TestPreparingProgress(t *testing.T) {
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, serverName string) {
		conf.Replication.MaxReplicas = 1
	})
	re.NoError(err)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(re, leader.GetAddr())
	clusterID := leader.GetClusterID()
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  &metapb.Store{Id: 1, Address: "127.0.0.1:0"},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	re.NoError(err)
	re.Nil(resp.GetHeader().GetError())
	stores := []*metapb.Store{
		{
			Id:             1,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Serving,
			LastHeartbeat:  time.Now().UnixNano(),
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             2,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Serving,
			LastHeartbeat:  time.Now().UnixNano(),
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             3,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Serving,
			LastHeartbeat:  time.Now().UnixNano(),
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             4,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Preparing,
			LastHeartbeat:  time.Now().UnixNano(),
			StartTimestamp: time.Now().UnixNano() - 100,
		},
		{
			Id:             5,
			State:          metapb.StoreState_Up,
			NodeState:      metapb.NodeState_Preparing,
			LastHeartbeat:  time.Now().UnixNano(),
			StartTimestamp: time.Now().UnixNano() - 100,
		},
	}

	for _, store := range stores {
		pdctl.MustPutStore(re, leader.GetServer(), store)
	}
	for i := 0; i < 100; i++ {
		pdctl.MustPutRegion(re, cluster, uint64(i+1), uint64(i)%3+1, []byte(fmt.Sprintf("p%d", i)), []byte(fmt.Sprintf("%d", i+1)), core.SetApproximateSize(10))
	}
	// no store preparing
	output := sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusNotFound)
	re.Contains((string(output)), "no progress found for the action")
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=4", http.MethodGet, http.StatusNotFound)
	re.Contains((string(output)), "no progress found for the given store ID")

	// is not prepared
	time.Sleep(2 * time.Second)
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusNotFound)
	re.Contains((string(output)), "no progress found for the action")
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=4", http.MethodGet, http.StatusNotFound)
	re.Contains((string(output)), "no progress found for the given store ID")

	// size is not changed.
	leader.GetRaftCluster().SetPrepared()
	time.Sleep(2 * time.Second)
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusOK)
	var p api.Progress
	re.NoError(json.Unmarshal(output, &p))
	re.Equal("preparing", p.Action)
	re.Equal(0.0, p.Progress)
	re.Equal(0.0, p.CurrentSpeed)
	re.Equal(math.MaxFloat64, p.LeftSeconds)

	// update size
	pdctl.MustPutRegion(re, cluster, 1000, 4, []byte(fmt.Sprintf("%d", 1000)), []byte(fmt.Sprintf("%d", 1001)), core.SetApproximateSize(10))
	pdctl.MustPutRegion(re, cluster, 1001, 5, []byte(fmt.Sprintf("%d", 1001)), []byte(fmt.Sprintf("%d", 1002)), core.SetApproximateSize(40))
	time.Sleep(2 * time.Second)
	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?action=preparing", http.MethodGet, http.StatusOK)
	re.NoError(json.Unmarshal(output, &p))
	re.Equal("preparing", p.Action)
	// store 4: 10/(210*0.9) ~= 0.05
	// store 5: 40/(210*0.9) ~= 0.21
	// average progress ~= (0.05+0.21)/2 = 0.13
	re.Equal("0.13", fmt.Sprintf("%.2f", p.Progress))
	// store 4: 10/10s = 1
	// store 5: 40/10s = 4
	// average speed = (1+4)/2 = 2.5
	re.Equal(2.5, p.CurrentSpeed)
	// store 4: 179/1 ~= 179
	// store 5: 149/4 ~= 37.25
	// average time ~= (179+37.25)/2 = 108.125
	re.Equal(108.125, p.LeftSeconds)

	output = sendRequest(re, leader.GetAddr()+"/pd/api/v1/stores/progress?id=4", http.MethodGet, http.StatusOK)
	re.NoError(json.Unmarshal(output, &p))
	re.Equal("preparing", p.Action)
	re.Equal("0.05", fmt.Sprintf("%.2f", p.Progress))
	re.Equal(1.0, p.CurrentSpeed)
	re.Equal(179.0, p.LeftSeconds)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"))
}

func sendRequest(re *require.Assertions, url string, method string, statusCode int) []byte {
	req, _ := http.NewRequest(method, url, nil)
	resp, err := dialClient.Do(req)
	re.NoError(err)
	re.Equal(statusCode, resp.StatusCode)
	output, err := io.ReadAll(resp.Body)
	re.NoError(err)
	resp.Body.Close()
	return output
}
