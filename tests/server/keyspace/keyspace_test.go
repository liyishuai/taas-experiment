// Copyright 2022 TiKV Project Authors.
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

package keyspace

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/keyspace"
	"github.com/tikv/pd/tests"
	"gitlab.alibaba-inc.com/zelu.wjz/taasplugin/pkg/keyspacepb"
)

type keyspaceTestSuite struct {
	suite.Suite
	cancel  context.CancelFunc
	cluster *tests.TestCluster
	server  *tests.TestServer
	manager *keyspace.Manager
}

// preAllocKeyspace is used to test keyspace pre-allocation.
var preAllocKeyspace = []string{"pre-alloc0", "pre-alloc1", "pre-alloc2"}

func TestKeyspaceTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceTestSuite))
}

func (suite *keyspaceTestSuite) SetupTest() {
	ctx, cancel := context.WithCancel(context.Background())
	suite.cancel = cancel
	cluster, err := tests.NewTestCluster(ctx, 3, func(conf *config.Config, serverName string) {
		conf.Keyspace.PreAlloc = preAllocKeyspace
	})
	suite.cluster = cluster
	suite.NoError(err)
	suite.NoError(cluster.RunInitialServers())
	suite.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetServer(cluster.GetLeader())
	suite.manager = suite.server.GetKeyspaceManager()
	suite.NoError(suite.server.BootstrapCluster())
}

func (suite *keyspaceTestSuite) TearDownTest() {
	suite.cancel()
	suite.cluster.Destroy()
}

func (suite *keyspaceTestSuite) TestRegionLabeler() {
	re := suite.Require()
	regionLabeler := suite.server.GetRaftCluster().GetRegionLabeler()

	// Create test keyspaces.
	count := 20
	now := time.Now().Unix()
	keyspaces := make([]*keyspacepb.KeyspaceMeta, count)
	manager := suite.manager
	var err error
	for i := 0; i < count; i++ {
		keyspaces[i], err = manager.CreateKeyspace(&keyspace.CreateKeyspaceRequest{
			Name: fmt.Sprintf("test_keyspace%d", i),
			Now:  now,
		})
		re.NoError(err)
	}
	// Check for region labels.
	for _, meta := range keyspaces {
		checkLabelRule(re, meta.GetId(), regionLabeler)
	}
}

func checkLabelRule(re *require.Assertions, id uint32, regionLabeler *labeler.RegionLabeler) {
	labelID := "keyspaces/" + strconv.FormatUint(uint64(id), endpoint.SpaceIDBase)
	loadedLabel := regionLabeler.GetLabelRule(labelID)
	re.NotNil(loadedLabel)

	rangeRule, ok := loadedLabel.Data.([]*labeler.KeyRangeRule)
	re.True(ok)
	re.Equal(2, len(rangeRule))

	keyspaceIDBytes := make([]byte, 4)
	nextKeyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, id)
	binary.BigEndian.PutUint32(nextKeyspaceIDBytes, id+1)
	rawLeftBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'r'}, keyspaceIDBytes[1:]...)))
	rawRightBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'r'}, nextKeyspaceIDBytes[1:]...)))
	txnLeftBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'x'}, keyspaceIDBytes[1:]...)))
	txnRightBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'x'}, nextKeyspaceIDBytes[1:]...)))

	re.Equal(rawLeftBound, rangeRule[0].StartKeyHex)
	re.Equal(rawRightBound, rangeRule[0].EndKeyHex)
	re.Equal(txnLeftBound, rangeRule[1].StartKeyHex)
	re.Equal(txnRightBound, rangeRule[1].EndKeyHex)
}

func (suite *keyspaceTestSuite) TestPreAlloc() {
	re := suite.Require()
	regionLabeler := suite.server.GetRaftCluster().GetRegionLabeler()
	for _, keyspaceName := range preAllocKeyspace {
		// Check pre-allocated keyspaces are correctly allocated.
		meta, err := suite.manager.LoadKeyspace(keyspaceName)
		re.NoError(err)
		// Check pre-allocated keyspaces also have the correct region label.
		checkLabelRule(re, meta.GetId(), regionLabeler)
	}
}
