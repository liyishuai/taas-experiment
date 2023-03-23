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

package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	tu "github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
)

type clusterTestSuite struct {
	suite.Suite
	svr       *server.Server
	cleanup   tu.CleanupFunc
	urlPrefix string
}

func TestClusterTestSuite(t *testing.T) {
	suite.Run(t, new(clusterTestSuite))
}

func (suite *clusterTestSuite) SetupSuite() {
	re := suite.Require()
	suite.svr, suite.cleanup = mustNewServer(re)
	server.MustWaitLeader(re, []*server.Server{suite.svr})

	addr := suite.svr.GetAddr()
	suite.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (suite *clusterTestSuite) TearDownSuite() {
	suite.cleanup()
}

func (suite *clusterTestSuite) TestCluster() {
	// Test get cluster status, and bootstrap cluster
	suite.testGetClusterStatus()
	suite.svr.GetPersistOptions().SetPlacementRuleEnabled(true)
	suite.svr.GetPersistOptions().GetReplicationConfig().LocationLabels = []string{"host"}
	rm := suite.svr.GetRaftCluster().GetRuleManager()
	rule := rm.GetRule("pd", "default")
	rule.LocationLabels = []string{"host"}
	rule.Count = 1
	rm.SetRule(rule)

	// Test set the config
	url := fmt.Sprintf("%s/cluster", suite.urlPrefix)
	c1 := &metapb.Cluster{}
	re := suite.Require()
	err := tu.ReadGetJSON(re, testDialClient, url, c1)
	suite.NoError(err)

	c2 := &metapb.Cluster{}
	r := config.ReplicationConfig{
		MaxReplicas:          6,
		EnablePlacementRules: true,
	}
	suite.NoError(suite.svr.SetReplicationConfig(r))

	err = tu.ReadGetJSON(re, testDialClient, url, c2)
	suite.NoError(err)

	c1.MaxPeerCount = 6
	suite.Equal(c2, c1)
	suite.Equal(int(r.MaxReplicas), suite.svr.GetRaftCluster().GetRuleManager().GetRule("pd", "default").Count)
}

func (suite *clusterTestSuite) testGetClusterStatus() {
	url := fmt.Sprintf("%s/cluster/status", suite.urlPrefix)
	status := cluster.Status{}
	re := suite.Require()
	err := tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.True(status.RaftBootstrapTime.IsZero())
	suite.False(status.IsInitialized)
	now := time.Now()
	mustBootstrapCluster(re, suite.svr)
	err = tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.True(status.RaftBootstrapTime.After(now))
	suite.False(status.IsInitialized)
	suite.svr.SetReplicationConfig(config.ReplicationConfig{MaxReplicas: 1})
	err = tu.ReadGetJSON(re, testDialClient, url, &status)
	suite.NoError(err)
	suite.True(status.RaftBootstrapTime.After(now))
	suite.True(status.IsInitialized)
}
