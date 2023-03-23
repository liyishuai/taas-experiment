// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package register_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/mcs"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type serverRegisterTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
}

func TestServerRegisterTestSuite(t *testing.T) {
	suite.Run(t, new(serverRegisterTestSuite))
}

func (suite *serverRegisterTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
}

func (suite *serverRegisterTestSuite) TearDownSuite() {
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *serverRegisterTestSuite) TestServerRegister() {
	// test register, primary and unregister when start tso and resource-manager with only one server
	for i := 0; i < 3; i++ {
		suite.checkServerRegister(utils.TSOServiceName)
	}
	// TODO: uncomment after resource-manager is ready
	// for i := 0; i < 3; i++ {
	// suite.checkServerRegister(utils.ResourceManagerServiceName)
	// }
}

func (suite *serverRegisterTestSuite) checkServerRegister(serviceName string) {
	re := suite.Require()
	s, cleanup := suite.addServer(serviceName)

	addr := s.GetAddr()
	client := suite.pdLeader.GetEtcdClient()

	// test API server discovery
	endpoints, err := discovery.Discover(client, serviceName)
	re.NoError(err)
	returnedEntry := &discovery.ServiceRegistryEntry{}
	returnedEntry.Deserialize([]byte(endpoints[0]))
	re.Equal(addr, returnedEntry.ServiceAddr)

	// test primary when only one server
	primary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.True(exist)
	re.Equal(primary, addr)

	// test API server discovery after unregister
	cleanup()
	endpoints, err = discovery.Discover(client, serviceName)
	re.NoError(err)
	re.Empty(endpoints)
}

func (suite *serverRegisterTestSuite) TestServerPrimaryChange() {
	suite.checkServerPrimaryChange(utils.TSOServiceName, 3)
	// TODO: uncomment after resource-manager is ready
	// suite.checkServerPrimaryChange(utils.ResourceManagerServiceName, 3)
}

func (suite *serverRegisterTestSuite) checkServerPrimaryChange(serviceName string, serverNum int) {
	re := suite.Require()
	primary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.False(exist)
	re.Empty(primary)

	serverMap := make(map[string]bs.Server)
	for i := 0; i < serverNum; i++ {
		s, cleanup := suite.addServer(serviceName)
		defer cleanup()
		serverMap[s.GetAddr()] = s
	}

	expectedPrimary := mcs.WaitForPrimaryServing(suite.Require(), serverMap)
	primary, exist = suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.True(exist)
	re.Equal(expectedPrimary, primary)
	// close old primary
	serverMap[primary].Close()
	delete(serverMap, primary)

	expectedPrimary = mcs.WaitForPrimaryServing(suite.Require(), serverMap)
	// test API server discovery
	client := suite.pdLeader.GetEtcdClient()
	endpoints, err := discovery.Discover(client, serviceName)
	re.NoError(err)
	re.Len(endpoints, serverNum-1)

	// test primary changed
	primary, exist = suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, serviceName)
	re.True(exist)
	re.Equal(expectedPrimary, primary)
}

func (suite *serverRegisterTestSuite) addServer(serviceName string) (bs.Server, func()) {
	re := suite.Require()
	switch serviceName {
	case utils.TSOServiceName:
		return mcs.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	case utils.ResourceManagerServiceName:
		return mcs.StartSingleResourceManagerTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())
	default:
		return nil, nil
	}
}
