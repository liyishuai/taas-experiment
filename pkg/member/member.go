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

package member

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

const (
	// The timeout to wait transfer etcd leader to complete.
	moveLeaderTimeout          = 5 * time.Second
	dcLocationConfigEtcdPrefix = "dc-location"
)

// EmbeddedEtcdMember is used for the election related logic. It implements Member interface.
type EmbeddedEtcdMember struct {
	leadership *election.Leadership
	leader     atomic.Value // stored as *pdpb.Member
	// etcd and cluster information.
	etcd     *embed.Etcd
	client   *clientv3.Client
	id       uint64       // etcd server id.
	member   *pdpb.Member // current PD's info.
	rootPath string
	// memberValue is the serialized string of `member`. It will be save in
	// etcd leader key when the PD node is successfully elected as the PD leader
	// of the cluster. Every write will use it to check PD leadership.
	memberValue string
}

// NewMember create a new Member.
func NewMember(etcd *embed.Etcd, client *clientv3.Client, id uint64) *EmbeddedEtcdMember {
	return &EmbeddedEtcdMember{
		etcd:   etcd,
		client: client,
		id:     id,
	}
}

// ID returns the unique etcd ID for this server in etcd cluster.
func (m *EmbeddedEtcdMember) ID() uint64 {
	return m.id
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (m *EmbeddedEtcdMember) Name() string {
	return m.member.Name
}

// GetMember returns the member.
func (m *EmbeddedEtcdMember) GetMember() interface{} {
	return m.member
}

// MemberValue returns the member value.
func (m *EmbeddedEtcdMember) MemberValue() string {
	return m.memberValue
}

// Member returns the member.
func (m *EmbeddedEtcdMember) Member() *pdpb.Member {
	return m.member
}

// Etcd returns etcd related information.
func (m *EmbeddedEtcdMember) Etcd() *embed.Etcd {
	return m.etcd
}

// Client returns the etcd client.
func (m *EmbeddedEtcdMember) Client() *clientv3.Client {
	return m.client
}

// IsLeader returns whether the server is PD leader or not by checking its leadership's lease and leader info.
func (m *EmbeddedEtcdMember) IsLeader() bool {
	return m.leadership.Check() && m.GetLeader().GetMemberId() == m.member.GetMemberId()
}

// IsLeaderElected returns true if the leader exists; otherwise false
func (m *EmbeddedEtcdMember) IsLeaderElected() bool {
	return m.GetLeader() != nil
}

// GetLeaderListenUrls returns current leader's listen urls
func (m *EmbeddedEtcdMember) GetLeaderListenUrls() []string {
	return m.GetLeader().GetClientUrls()
}

// GetLeaderID returns current PD leader's member ID.
func (m *EmbeddedEtcdMember) GetLeaderID() uint64 {
	return m.GetLeader().GetMemberId()
}

// GetLeader returns current PD leader of PD cluster.
func (m *EmbeddedEtcdMember) GetLeader() *pdpb.Member {
	leader := m.leader.Load()
	if leader == nil {
		return nil
	}
	member := leader.(*pdpb.Member)
	if member.GetMemberId() == 0 {
		return nil
	}
	return member
}

// setLeader sets the member's PD leader.
func (m *EmbeddedEtcdMember) setLeader(member *pdpb.Member) {
	m.leader.Store(member)
}

// unsetLeader unsets the member's PD leader.
func (m *EmbeddedEtcdMember) unsetLeader() {
	m.leader.Store(&pdpb.Member{})
}

// EnableLeader sets the member itself to a PD leader.
func (m *EmbeddedEtcdMember) EnableLeader() {
	m.setLeader(m.member)
}

// GetLeaderPath returns the path of the PD leader.
func (m *EmbeddedEtcdMember) GetLeaderPath() string {
	return path.Join(m.rootPath, "leader")
}

// GetLeadership returns the leadership of the PD member.
func (m *EmbeddedEtcdMember) GetLeadership() *election.Leadership {
	return m.leadership
}

// CampaignLeader is used to campaign a PD member's leadership
// and make it become a PD leader.
func (m *EmbeddedEtcdMember) CampaignLeader(leaseTimeout int64) error {
	return m.leadership.Campaign(leaseTimeout, m.MemberValue())
}

// KeepLeader is used to keep the PD leader's leadership.
func (m *EmbeddedEtcdMember) KeepLeader(ctx context.Context) {
	m.leadership.Keep(ctx)
}

// PrecheckLeader does some pre-check before checking whether or not it's the leader.
func (m *EmbeddedEtcdMember) PrecheckLeader() error {
	if m.GetEtcdLeader() == 0 {
		return errs.ErrEtcdLeaderNotFound
	}
	return nil
}

// getPersistentLeader gets the corresponding leader from etcd by given leaderPath (as the key).
func (m *EmbeddedEtcdMember) getPersistentLeader() (*pdpb.Member, int64, error) {
	leader := &pdpb.Member{}
	ok, rev, err := etcdutil.GetProtoMsgWithModRev(m.client, m.GetLeaderPath(), leader)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, nil
	}

	return leader, rev, nil
}

// CheckLeader checks returns true if it is needed to check later.
func (m *EmbeddedEtcdMember) CheckLeader() (*pdpb.Member, int64, bool) {
	if err := m.PrecheckLeader(); err != nil {
		log.Error("failed to pass pre-check, check pd leader later", errs.ZapError(err))
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}

	leader, rev, err := m.getPersistentLeader()
	if err != nil {
		log.Error("getting pd leader meets error", errs.ZapError(err))
		time.Sleep(200 * time.Millisecond)
		return nil, 0, true
	}
	if leader != nil {
		if m.IsSameLeader(leader) {
			// oh, we are already a PD leader, which indicates we may meet something wrong
			// in previous CampaignLeader. We should delete the leadership and campaign again.
			log.Warn("the pd leader has not changed, delete and campaign again", zap.Stringer("old-pd-leader", leader))
			// Delete the leader itself and let others start a new election again.
			if err = m.leadership.DeleteLeaderKey(); err != nil {
				log.Error("deleting pd leader key meets error", errs.ZapError(err))
				time.Sleep(200 * time.Millisecond)
				return nil, 0, true
			}
			// Return nil and false to make sure the campaign will start immediately.
			return nil, 0, false
		}
	}
	return leader, rev, false
}

// WatchLeader is used to watch the changes of the leader.
func (m *EmbeddedEtcdMember) WatchLeader(serverCtx context.Context, leader *pdpb.Member, revision int64) {
	m.setLeader(leader)
	m.leadership.Watch(serverCtx, revision)
	m.unsetLeader()
}

// ResetLeader is used to reset the PD member's current leadership.
// Basically it will reset the leader lease and unset leader info.
func (m *EmbeddedEtcdMember) ResetLeader() {
	m.leadership.Reset()
	m.unsetLeader()
}

// CheckPriority checks whether the etcd leader should be moved according to the priority.
func (m *EmbeddedEtcdMember) CheckPriority(ctx context.Context) {
	etcdLeader := m.GetEtcdLeader()
	if etcdLeader == m.ID() || etcdLeader == 0 {
		return
	}
	myPriority, err := m.GetMemberLeaderPriority(m.ID())
	if err != nil {
		log.Error("failed to load leader priority", errs.ZapError(err))
		return
	}
	leaderPriority, err := m.GetMemberLeaderPriority(etcdLeader)
	if err != nil {
		log.Error("failed to load etcd leader priority", errs.ZapError(err))
		return
	}
	if myPriority > leaderPriority {
		err := m.MoveEtcdLeader(ctx, etcdLeader, m.ID())
		if err != nil {
			log.Error("failed to transfer etcd leader", errs.ZapError(err))
		} else {
			log.Info("transfer etcd leader",
				zap.Uint64("from", etcdLeader),
				zap.Uint64("to", m.ID()))
		}
	}
}

// MoveEtcdLeader tries to transfer etcd leader.
func (m *EmbeddedEtcdMember) MoveEtcdLeader(ctx context.Context, old, new uint64) error {
	moveCtx, cancel := context.WithTimeout(ctx, moveLeaderTimeout)
	defer cancel()
	err := m.etcd.Server.MoveLeader(moveCtx, old, new)
	if err != nil {
		return errs.ErrEtcdMoveLeader.Wrap(err).GenWithStackByCause()
	}
	return nil
}

// GetEtcdLeader returns the etcd leader ID.
func (m *EmbeddedEtcdMember) GetEtcdLeader() uint64 {
	return m.etcd.Server.Lead()
}

// IsSameLeader checks whether a server is the leader itself.
func (m *EmbeddedEtcdMember) IsSameLeader(leader *pdpb.Member) bool {
	return leader.GetMemberId() == m.ID()
}

// InitMemberInfo initializes the member info.
func (m *EmbeddedEtcdMember) InitMemberInfo(advertiseClientUrls, advertisePeerUrls, name string, rootPath string) {
	leader := &pdpb.Member{
		Name:       name,
		MemberId:   m.ID(),
		ClientUrls: strings.Split(advertiseClientUrls, ","),
		PeerUrls:   strings.Split(advertisePeerUrls, ","),
	}

	data, err := leader.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatal("marshal pd leader meet error", zap.Stringer("pd-leader", leader), errs.ZapError(errs.ErrMarshalLeader, err))
	}
	m.member = leader
	m.memberValue = string(data)
	m.rootPath = rootPath
	m.leadership = election.NewLeadership(m.client, m.GetLeaderPath(), "leader election")
}

// ResignEtcdLeader resigns current PD's etcd leadership. If nextLeader is empty, all
// other pd-servers can campaign.
func (m *EmbeddedEtcdMember) ResignEtcdLeader(ctx context.Context, from string, nextEtcdLeader string) error {
	log.Info("try to resign etcd leader to next pd-server", zap.String("from", from), zap.String("to", nextEtcdLeader))
	// Determine next etcd leader candidates.
	var etcdLeaderIDs []uint64
	res, err := etcdutil.ListEtcdMembers(m.client)
	if err != nil {
		return err
	}

	// Do nothing when I am the only member of cluster.
	if len(res.Members) == 1 && res.Members[0].ID == m.id && nextEtcdLeader == "" {
		return nil
	}

	for _, member := range res.Members {
		if (nextEtcdLeader == "" && member.ID != m.id) || (nextEtcdLeader != "" && member.Name == nextEtcdLeader) {
			etcdLeaderIDs = append(etcdLeaderIDs, member.GetID())
		}
	}
	if len(etcdLeaderIDs) == 0 {
		return errors.New("no valid pd to transfer etcd leader")
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	nextEtcdLeaderID := etcdLeaderIDs[r.Intn(len(etcdLeaderIDs))]
	return m.MoveEtcdLeader(ctx, m.ID(), nextEtcdLeaderID)
}

func (m *EmbeddedEtcdMember) getMemberLeaderPriorityPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/leader_priority", id))
}

// GetDCLocationPathPrefix returns the dc-location path prefix of the cluster.
func (m *EmbeddedEtcdMember) GetDCLocationPathPrefix() string {
	return path.Join(m.rootPath, dcLocationConfigEtcdPrefix)
}

// GetDCLocationPath returns the dc-location path of a member with the given member ID.
func (m *EmbeddedEtcdMember) GetDCLocationPath(id uint64) string {
	return path.Join(m.GetDCLocationPathPrefix(), fmt.Sprint(id))
}

// SetMemberLeaderPriority saves a member's priority to be elected as the etcd leader.
func (m *EmbeddedEtcdMember) SetMemberLeaderPriority(id uint64, priority int) error {
	key := m.getMemberLeaderPriorityPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpPut(key, strconv.Itoa(priority))).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !res.Succeeded {
		log.Error("save etcd leader priority failed, maybe not pd leader")
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// DeleteMemberLeaderPriority removes a member's etcd leader priority config.
func (m *EmbeddedEtcdMember) DeleteMemberLeaderPriority(id uint64) error {
	key := m.getMemberLeaderPriorityPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !res.Succeeded {
		log.Error("delete etcd leader priority failed, maybe not pd leader")
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// DeleteMemberDCLocationInfo removes a member's dc-location info.
func (m *EmbeddedEtcdMember) DeleteMemberDCLocationInfo(id uint64) error {
	key := m.GetDCLocationPath(id)
	res, err := m.leadership.LeaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStackByCause()
	}
	if !res.Succeeded {
		log.Error("delete dc-location info failed, maybe not pd leader")
		return errs.ErrEtcdTxnConflict.FastGenByArgs()
	}
	return nil
}

// GetMemberLeaderPriority loads a member's priority to be elected as the etcd leader.
func (m *EmbeddedEtcdMember) GetMemberLeaderPriority(id uint64) (int, error) {
	key := m.getMemberLeaderPriorityPath(id)
	res, err := etcdutil.EtcdKVGet(m.client, key)
	if err != nil {
		return 0, err
	}
	if len(res.Kvs) == 0 {
		return 0, nil
	}
	priority, err := strconv.ParseInt(string(res.Kvs[0].Value), 10, 32)
	if err != nil {
		return 0, errs.ErrStrconvParseInt.Wrap(err).GenWithStackByCause()
	}
	return int(priority), nil
}

func (m *EmbeddedEtcdMember) getMemberBinaryDeployPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/deploy_path", id))
}

// GetMemberDeployPath loads a member's binary deploy path.
func (m *EmbeddedEtcdMember) GetMemberDeployPath(id uint64) (string, error) {
	key := m.getMemberBinaryDeployPath(id)
	res, err := etcdutil.EtcdKVGet(m.client, key)
	if err != nil {
		return "", err
	}
	if len(res.Kvs) == 0 {
		return "", errs.ErrEtcdKVGetResponse.FastGenByArgs("no value")
	}
	return string(res.Kvs[0].Value), nil
}

// SetMemberDeployPath saves a member's binary deploy path.
func (m *EmbeddedEtcdMember) SetMemberDeployPath(id uint64) error {
	key := m.getMemberBinaryDeployPath(id)
	txn := kv.NewSlowLogTxn(m.client)
	execPath, err := os.Executable()
	deployPath := filepath.Dir(execPath)
	if err != nil {
		return errors.WithStack(err)
	}
	res, err := txn.Then(clientv3.OpPut(key, deployPath)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("failed to save deploy path")
	}
	return nil
}

func (m *EmbeddedEtcdMember) getMemberGitHashPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/git_hash", id))
}

func (m *EmbeddedEtcdMember) getMemberBinaryVersionPath(id uint64) string {
	return path.Join(m.rootPath, fmt.Sprintf("member/%d/binary_version", id))
}

// GetMemberBinaryVersion loads a member's binary version.
func (m *EmbeddedEtcdMember) GetMemberBinaryVersion(id uint64) (string, error) {
	key := m.getMemberBinaryVersionPath(id)
	res, err := etcdutil.EtcdKVGet(m.client, key)
	if err != nil {
		return "", err
	}
	if len(res.Kvs) == 0 {
		return "", errs.ErrEtcdKVGetResponse.FastGenByArgs("no value")
	}
	return string(res.Kvs[0].Value), nil
}

// GetMemberGitHash loads a member's git hash.
func (m *EmbeddedEtcdMember) GetMemberGitHash(id uint64) (string, error) {
	key := m.getMemberGitHashPath(id)
	res, err := etcdutil.EtcdKVGet(m.client, key)
	if err != nil {
		return "", err
	}
	if len(res.Kvs) == 0 {
		return "", errs.ErrEtcdKVGetResponse.FastGenByArgs("no value")
	}
	return string(res.Kvs[0].Value), nil
}

// SetMemberBinaryVersion saves a member's binary version.
func (m *EmbeddedEtcdMember) SetMemberBinaryVersion(id uint64, releaseVersion string) error {
	key := m.getMemberBinaryVersionPath(id)
	txn := kv.NewSlowLogTxn(m.client)
	res, err := txn.Then(clientv3.OpPut(key, releaseVersion)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("failed to save binary version")
	}
	return nil
}

// SetMemberGitHash saves a member's git hash.
func (m *EmbeddedEtcdMember) SetMemberGitHash(id uint64, gitHash string) error {
	key := m.getMemberGitHashPath(id)
	txn := kv.NewSlowLogTxn(m.client)
	res, err := txn.Then(clientv3.OpPut(key, gitHash)).Commit()
	if err != nil {
		return errors.WithStack(err)
	}
	if !res.Succeeded {
		return errors.New("failed to save git hash")
	}
	return nil
}

// Close gracefully shuts down all servers/listeners.
func (m *EmbeddedEtcdMember) Close() {
	m.Etcd().Close()
}
