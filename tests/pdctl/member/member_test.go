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

package member_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"gitlab.alibaba-inc.com/zelu.wjz/taasplugin/pkg/pdpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func TestMember(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 3)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	re.NoError(leaderServer.BootstrapCluster())
	pdAddr := cluster.GetConfig().GetClientURL()
	re.NoError(err)
	cmd := pdctlCmd.GetRootCmd()
	svr := cluster.GetServer("pd2")
	id := svr.GetServerID()
	name := svr.GetServer().Name()
	client := cluster.GetServer("pd1").GetEtcdClient()
	defer cluster.Destroy()

	// member leader show
	args := []string{"-u", pdAddr, "member", "leader", "show"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	leader := pdpb.Member{}
	re.NoError(json.Unmarshal(output, &leader))
	re.Equal(svr.GetLeader(), &leader)

	// member leader transfer <member_name>
	args = []string{"-u", pdAddr, "member", "leader", "transfer", "pd2"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return svr.GetLeader().GetName() == "pd2"
	})

	// member leader resign
	cluster.WaitLeader()
	args = []string{"-u", pdAddr, "member", "leader", "resign"}
	output, err = pdctl.ExecuteCommand(cmd, args...)
	re.Contains(string(output), "Success")
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		return svr.GetLeader().GetName() != "pd2"
	})

	// member leader_priority <member_name> <priority>
	cluster.WaitLeader()
	args = []string{"-u", pdAddr, "member", "leader_priority", name, "100"}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	priority, err := svr.GetServer().GetMember().GetMemberLeaderPriority(id)
	re.NoError(err)
	re.Equal(100, priority)

	// member delete name <member_name>
	err = svr.Destroy()
	re.NoError(err)
	members, err := etcdutil.ListEtcdMembers(client)
	re.NoError(err)
	re.Len(members.Members, 3)
	args = []string{"-u", pdAddr, "member", "delete", "name", name}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		members, err = etcdutil.ListEtcdMembers(client)
		re.NoError(err)
		return len(members.Members) == 2
	})

	// member delete id <member_id>
	args = []string{"-u", pdAddr, "member", "delete", "id", fmt.Sprint(id)}
	_, err = pdctl.ExecuteCommand(cmd, args...)
	re.NoError(err)
	testutil.Eventually(re, func() bool {
		members, err = etcdutil.ListEtcdMembers(client)
		re.NoError(err)
		return len(members.Members) == 2
	})
}
