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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"testing"

	"gitlab.alibaba-inc.com/zelu.wjz/taasplugin/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

type memberTestSuite struct {
	suite.Suite
	cfgs    []*config.Config
	servers []*server.Server
	clean   testutil.CleanupFunc
}

func TestMemberTestSuite(t *testing.T) {
	suite.Run(t, new(memberTestSuite))
}

func (suite *memberTestSuite) SetupSuite() {
	suite.cfgs, suite.servers, suite.clean = mustNewCluster(suite.Require(), 3, func(cfg *config.Config) {
		cfg.EnableLocalTSO = true
		cfg.Labels = map[string]string{
			config.ZoneLabel: "dc-1",
		}
	})
}

func (suite *memberTestSuite) TearDownSuite() {
	suite.clean()
}

func relaxEqualStings(re *require.Assertions, a, b []string) {
	sort.Strings(a)
	sortedStringA := strings.Join(a, "")

	sort.Strings(b)
	sortedStringB := strings.Join(b, "")

	re.Equal(sortedStringB, sortedStringA)
}

func (suite *memberTestSuite) checkListResponse(body []byte, cfgs []*config.Config) {
	got := make(map[string][]*pdpb.Member)
	json.Unmarshal(body, &got)
	suite.Len(cfgs, len(got["members"]))
	re := suite.Require()
	for _, member := range got["members"] {
		for _, cfg := range cfgs {
			if member.GetName() != cfg.Name {
				continue
			}
			suite.Equal("dc-1", member.DcLocation)
			relaxEqualStings(re, member.ClientUrls, strings.Split(cfg.ClientUrls, ","))
			relaxEqualStings(re, member.PeerUrls, strings.Split(cfg.PeerUrls, ","))
		}
	}
}

func (suite *memberTestSuite) TestMemberList() {
	for _, cfg := range suite.cfgs {
		addr := cfg.ClientUrls + apiPrefix + "/api/v1/members"
		resp, err := testDialClient.Get(addr)
		suite.NoError(err)
		buf, err := io.ReadAll(resp.Body)
		suite.NoError(err)
		resp.Body.Close()
		suite.checkListResponse(buf, suite.cfgs)
	}
}

func (suite *memberTestSuite) TestMemberLeader() {
	leader := suite.servers[0].GetLeader()
	addr := suite.cfgs[rand.Intn(len(suite.cfgs))].ClientUrls + apiPrefix + "/api/v1/leader"
	resp, err := testDialClient.Get(addr)
	suite.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	suite.NoError(err)

	var got pdpb.Member
	suite.NoError(json.Unmarshal(buf, &got))
	suite.Equal(leader.GetClientUrls(), got.GetClientUrls())
	suite.Equal(leader.GetMemberId(), got.GetMemberId())
}

func (suite *memberTestSuite) TestChangeLeaderPeerUrls() {
	leader := suite.servers[0].GetLeader()
	addr := suite.cfgs[rand.Intn(len(suite.cfgs))].ClientUrls + apiPrefix + "/api/v1/leader"
	resp, err := testDialClient.Get(addr)
	suite.NoError(err)
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	suite.NoError(err)

	var got pdpb.Member
	suite.NoError(json.Unmarshal(buf, &got))
	id := got.GetMemberId()
	peerUrls := got.GetPeerUrls()

	newPeerUrls := []string{"http://127.0.0.1:1111"}
	suite.changeLeaderPeerUrls(leader, id, newPeerUrls)
	addr = suite.cfgs[rand.Intn(len(suite.cfgs))].ClientUrls + apiPrefix + "/api/v1/members"
	resp, err = testDialClient.Get(addr)
	suite.NoError(err)
	buf, err = io.ReadAll(resp.Body)
	suite.NoError(err)
	resp.Body.Close()
	got1 := make(map[string]*pdpb.Member)
	json.Unmarshal(buf, &got1)
	suite.Equal(newPeerUrls, got1["leader"].GetPeerUrls())
	suite.Equal(newPeerUrls, got1["etcd_leader"].GetPeerUrls())

	// reset
	suite.changeLeaderPeerUrls(leader, id, peerUrls)
}

func (suite *memberTestSuite) changeLeaderPeerUrls(leader *pdpb.Member, id uint64, urls []string) {
	data := map[string][]string{"peerURLs": urls}
	postData, err := json.Marshal(data)
	suite.NoError(err)
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/v2/members/%s", leader.GetClientUrls()[0], fmt.Sprintf("%x", id)), bytes.NewBuffer(postData))
	suite.NoError(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := testDialClient.Do(req)
	suite.NoError(err)
	suite.Equal(204, resp.StatusCode)
	resp.Body.Close()
}

type resignTestSuite struct {
	suite.Suite
	cfgs    []*config.Config
	servers []*server.Server
	clean   testutil.CleanupFunc
}

func TestResignTestSuite(t *testing.T) {
	suite.Run(t, new(resignTestSuite))
}

func (suite *resignTestSuite) SetupSuite() {
	suite.cfgs, suite.servers, suite.clean = mustNewCluster(suite.Require(), 1)
}

func (suite *resignTestSuite) TearDownSuite() {
	suite.clean()
}

func (suite *resignTestSuite) TestResignMyself() {
	addr := suite.cfgs[0].ClientUrls + apiPrefix + "/api/v1/leader/resign"
	resp, err := testDialClient.Post(addr, "", nil)
	suite.NoError(err)
	suite.Equal(http.StatusOK, resp.StatusCode)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}
