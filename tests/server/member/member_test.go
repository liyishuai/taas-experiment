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

package member_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

func TestMemberDelete(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	leader := cluster.GetServer(leaderName)
	var members []*tests.TestServer
	for _, s := range cluster.GetConfig().InitialServers {
		if s.Name != leaderName {
			members = append(members, cluster.GetServer(s.Name))
		}
	}
	re.Len(members, 2)

	var tables = []struct {
		path    string
		status  int
		members []*config.Config
	}{
		{path: "name/foobar", status: http.StatusNotFound},
		{path: "name/" + members[0].GetConfig().Name, members: []*config.Config{leader.GetConfig(), members[1].GetConfig()}},
		{path: "name/" + members[0].GetConfig().Name, status: http.StatusNotFound},
		{path: fmt.Sprintf("id/%d", members[1].GetServerID()), members: []*config.Config{leader.GetConfig()}},
	}

	httpClient := &http.Client{Timeout: 15 * time.Second}
	for _, table := range tables {
		t.Log(time.Now(), "try to delete:", table.path)
		testutil.Eventually(re, func() bool {
			addr := leader.GetConfig().ClientUrls + "/pd/api/v1/members/" + table.path
			req, err := http.NewRequest(http.MethodDelete, addr, nil)
			re.NoError(err)
			res, err := httpClient.Do(req)
			re.NoError(err)
			defer res.Body.Close()
			// Check by status.
			if table.status != 0 {
				if res.StatusCode != table.status {
					time.Sleep(time.Second)
					return false
				}
				return true
			}
			// Check by member list.
			cluster.WaitLeader()
			if err = checkMemberList(re, leader.GetConfig().ClientUrls, table.members); err != nil {
				t.Logf("check member fail: %v", err)
				time.Sleep(time.Second)
				return false
			}
			return true
		})
	}
	// Check whether the dc-location info of the corresponding member is deleted.
	for _, member := range members {
		key := member.GetServer().GetMember().GetDCLocationPath(member.GetServerID())
		resp, err := etcdutil.EtcdKVGet(leader.GetEtcdClient(), key)
		re.NoError(err)
		re.Empty(resp.Kvs)
	}
}

func checkMemberList(re *require.Assertions, clientURL string, configs []*config.Config) error {
	httpClient := &http.Client{Timeout: 15 * time.Second}
	addr := clientURL + "/pd/api/v1/members"
	res, err := httpClient.Get(addr)
	re.NoError(err)
	defer res.Body.Close()
	buf, err := io.ReadAll(res.Body)
	re.NoError(err)
	if res.StatusCode != http.StatusOK {
		return errors.Errorf("load members failed, status: %v, data: %q", res.StatusCode, buf)
	}
	data := make(map[string][]*pdpb.Member)
	json.Unmarshal(buf, &data)
	if len(data["members"]) != len(configs) {
		return errors.Errorf("member length not match, %v vs %v", len(data["members"]), len(configs))
	}
	for _, member := range data["members"] {
		for _, cfg := range configs {
			if member.GetName() == cfg.Name {
				re.Equal([]string{cfg.ClientUrls}, member.ClientUrls)
				re.Equal([]string{cfg.PeerUrls}, member.PeerUrls)
			}
		}
	}
	return nil
}

func TestLeaderPriority(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	cluster.WaitLeader()

	leader1, err := cluster.GetServer("pd1").GetEtcdLeader()
	re.NoError(err)
	server1 := cluster.GetServer(leader1)
	addr := server1.GetConfig().ClientUrls
	// PD leader should sync with etcd leader.
	testutil.Eventually(re, func() bool {
		return cluster.GetLeader() == leader1
	})
	// Bind a lower priority to current leader.
	post(t, re, addr+"/pd/api/v1/members/name/"+leader1, `{"leader-priority": -1}`)
	// Wait etcd leader change.
	leader2 := waitEtcdLeaderChange(re, server1, leader1)
	// PD leader should sync with etcd leader again.
	testutil.Eventually(re, func() bool {
		return cluster.GetLeader() == leader2
	})
}

func post(t *testing.T, re *require.Assertions, url string, body string) {
	testutil.Eventually(re, func() bool {
		res, err := http.Post(url, "", bytes.NewBufferString(body)) // #nosec
		re.NoError(err)
		b, err := io.ReadAll(res.Body)
		res.Body.Close()
		re.NoError(err)
		t.Logf("post %s, status: %v res: %s", url, res.StatusCode, string(b))
		return res.StatusCode == http.StatusOK
	})
}

func waitEtcdLeaderChange(re *require.Assertions, server *tests.TestServer, old string) string {
	var leader string
	testutil.Eventually(re, func() bool {
		var err error
		leader, err = server.GetEtcdLeader()
		if err != nil {
			return false
		}
		return leader != old
	}, testutil.WithWaitFor(90*time.Second), testutil.WithTickInterval(time.Second))
	return leader
}

func TestLeaderResign(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader1 := cluster.WaitLeader()
	addr1 := cluster.GetServer(leader1).GetConfig().ClientUrls

	post(t, re, addr1+"/pd/api/v1/leader/resign", "")
	leader2 := waitLeaderChange(re, cluster, leader1)
	t.Log("leader2:", leader2)
	addr2 := cluster.GetServer(leader2).GetConfig().ClientUrls
	post(t, re, addr2+"/pd/api/v1/leader/transfer/"+leader1, "")
	leader3 := waitLeaderChange(re, cluster, leader2)
	re.Equal(leader1, leader3)
}

func TestLeaderResignWithBlock(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)

	leader1 := cluster.WaitLeader()
	addr1 := cluster.GetServer(leader1).GetConfig().ClientUrls

	re.NoError(failpoint.Enable("github.com/tikv/pd/server/raftclusterIsBusy", `pause`))
	post(t, re, addr1+"/pd/api/v1/leader/resign", "")
	leader2 := waitLeaderChange(re, cluster, leader1)
	t.Log("leader2:", leader2)
	re.NotEqual(leader1, leader2)
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/raftclusterIsBusy"))
}

func waitLeaderChange(re *require.Assertions, cluster *tests.TestCluster, old string) string {
	var leader string
	testutil.Eventually(re, func() bool {
		leader = cluster.GetLeader()
		if leader == old || leader == "" {
			return false
		}
		return true
	})
	return leader
}

func TestMoveLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 5)
	defer cluster.Destroy()
	re.NoError(err)

	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()

	var wg sync.WaitGroup
	wg.Add(5)
	for _, s := range cluster.GetServers() {
		go func(s *tests.TestServer) {
			defer wg.Done()
			if s.IsLeader() {
				s.ResignLeader()
			} else {
				old, _ := s.GetEtcdLeaderID()
				s.MoveEtcdLeader(old, s.GetServerID())
			}
		}(s)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("move etcd leader does not return in 10 seconds")
	}
}

func TestGetLeader(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := server.NewTestSingleConfig(assertutil.CheckerWithNilAssert(re))
	wg := &sync.WaitGroup{}
	wg.Add(1)
	done := make(chan bool)
	svr, err := server.CreateServer(ctx, cfg, nil, server.CreateMockHandler(re, "127.0.0.1"))
	re.NoError(err)
	defer svr.Close()
	re.NoError(svr.Run())
	// Send requests after server has started.
	go sendRequest(re, wg, done, cfg.ClientUrls)
	time.Sleep(100 * time.Millisecond)

	server.MustWaitLeader(re, []*server.Server{svr})

	re.NotNil(svr.GetLeader())

	done <- true
	wg.Wait()

	testutil.CleanServer(cfg.DataDir)
}

func sendRequest(re *require.Assertions, wg *sync.WaitGroup, done <-chan bool, addr string) {
	defer wg.Done()

	req := &pdpb.AllocIDRequest{Header: testutil.NewRequestHeader(0)}

	for {
		select {
		case <-done:
			return
		default:
			// We don't need to check the response and error,
			// just make sure the server will not panic.
			grpcPDClient := testutil.MustNewGrpcClient(re, addr)
			if grpcPDClient != nil {
				_, _ = grpcPDClient.AllocID(context.Background(), req)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
