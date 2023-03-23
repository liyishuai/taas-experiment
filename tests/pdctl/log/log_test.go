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

package log_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

type logTestSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	cluster *tests.TestCluster
	svr     *server.Server
	pdAddrs []string
}

func TestLogTestSuite(t *testing.T) {
	suite.Run(t, new(logTestSuite))
}

func (suite *logTestSuite) SetupSuite() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	var err error
	suite.cluster, err = tests.NewTestCluster(suite.ctx, 3)
	suite.NoError(err)
	suite.NoError(suite.cluster.RunInitialServers())
	suite.cluster.WaitLeader()
	suite.pdAddrs = suite.cluster.GetConfig().GetClientURLs()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := suite.cluster.GetServer(suite.cluster.GetLeader())
	suite.NoError(leaderServer.BootstrapCluster())
	suite.svr = leaderServer.GetServer()
	pdctl.MustPutStore(suite.Require(), suite.svr, store)
}

func (suite *logTestSuite) TearDownSuite() {
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *logTestSuite) TestLog() {
	cmd := pdctlCmd.GetRootCmd()
	var testCases = []struct {
		cmd    []string
		expect string
	}{
		// log [fatal|error|warn|info|debug]
		{
			cmd:    []string{"-u", suite.pdAddrs[0], "log", "fatal"},
			expect: "fatal",
		},
		{
			cmd:    []string{"-u", suite.pdAddrs[0], "log", "error"},
			expect: "error",
		},
		{
			cmd:    []string{"-u", suite.pdAddrs[0], "log", "warn"},
			expect: "warn",
		},
		{
			cmd:    []string{"-u", suite.pdAddrs[0], "log", "info"},
			expect: "info",
		},
		{
			cmd:    []string{"-u", suite.pdAddrs[0], "log", "debug"},
			expect: "debug",
		},
	}

	for _, testCase := range testCases {
		_, err := pdctl.ExecuteCommand(cmd, testCase.cmd...)
		suite.NoError(err)
		suite.Equal(testCase.expect, suite.svr.GetConfig().Log.Level)
	}
}

func (suite *logTestSuite) TestInstanceLog() {
	cmd := pdctlCmd.GetRootCmd()
	var testCases = []struct {
		cmd      []string
		instance string
		expect   string
	}{
		// log [fatal|error|warn|info|debug] [address]
		{
			cmd:      []string{"-u", suite.pdAddrs[0], "log", "debug", suite.pdAddrs[0]},
			instance: suite.pdAddrs[0],
			expect:   "debug",
		},
		{
			cmd:      []string{"-u", suite.pdAddrs[0], "log", "error", suite.pdAddrs[1]},
			instance: suite.pdAddrs[1],
			expect:   "error",
		},
		{
			cmd:      []string{"-u", suite.pdAddrs[0], "log", "warn", suite.pdAddrs[2]},
			instance: suite.pdAddrs[2],
			expect:   "warn",
		},
	}

	for _, testCase := range testCases {
		_, err := pdctl.ExecuteCommand(cmd, testCase.cmd...)
		suite.NoError(err)
		svrs := suite.cluster.GetServers()
		for _, svr := range svrs {
			if svr.GetAddr() == testCase.instance {
				suite.Equal(testCase.expect, svr.GetConfig().Log.Level)
			}
		}
	}
}
