// Copyright 2015 The etcd Authors
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

package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/raft/v3/confchange"
	"go.etcd.io/raft/v3/quorum"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

const (
	// None is a placeholder node ID used when there is no leader.
	None uint64 = 0
	// LocalAppendThread is a reference to a local thread that saves unstable
	// log entries and snapshots to stable storage. The identifier is used as a
	// target for MsgStorageAppend messages when AsyncStorageWrites is enabled.
	LocalAppendThread uint64 = math.MaxUint64
	// LocalApplyThread is a reference to a local thread that applies committed
	// log entries to the local state machine. The identifier is used as a
	// target for MsgStorageApply messages when AsyncStorageWrites is enabled.
	LocalApplyThread uint64 = math.MaxUint64 - 1
)

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

const noLimit = math.MaxUint64

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[st]
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	// ID 是本地 raft 的身份标识。不能为 0
	ID uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64

	// AsyncStorageWrites configures the raft node to write to its local storage
	// (raft log and state machine) using a request/response message passing
	// interface instead of the default Ready/Advance function call interface.
	// Local storage messages can be pipelined and processed asynchronously
	// (with respect to Ready iteration), facilitating reduced interference
	// between Raft proposals and increased batching of log appends and state
	// machine application. As a result, use of asynchronous storage writes can
	// reduce end-to-end commit latency and increase maximum throughput.
	//
	// When true, the Ready.Message slice will include MsgStorageAppend and
	// MsgStorageApply messages. The messages will target a LocalAppendThread
	// and a LocalApplyThread, respectively. Messages to the same target must be
	// reliably processed in order. In other words, they can't be dropped (like
	// messages over the network) and those targeted at the same thread can't be
	// reordered. Messages to different targets can be processed in any order.
	//
	// MsgStorageAppend carries Raft log entries to append, election votes /
	// term changes / updated commit indexes to persist, and snapshots to apply.
	// All writes performed in service of a MsgStorageAppend must be durable
	// before response messages are delivered. However, if the MsgStorageAppend
	// carries no response messages, durability is not required. The message
	// assumes the role of the Entries, HardState, and Snapshot fields in Ready.
	//
	// MsgStorageApply carries committed entries to apply. Writes performed in
	// service of a MsgStorageApply need not be durable before response messages
	// are delivered. The message assumes the role of the CommittedEntries field
	// in Ready.
	//
	// Local messages each carry one or more response messages which should be
	// delivered after the corresponding storage write has been completed. These
	// responses may target the same node or may target other nodes. The storage
	// threads are not responsible for understanding the response messages, only
	// for delivering them to the correct target after performing the storage
	// write.
	AsyncStorageWrites bool

	// MaxSizePerMsg limits the max byte size of each append message. Smaller
	// value lowers the raft recovery cost(initial probing and message lost
	// during normal operation). On the other side, it might affect the
	// throughput during normal replication. Note: math.MaxUint64 for unlimited,
	// 0 for at most one entry per message.
	MaxSizePerMsg uint64
	// MaxCommittedSizePerReady limits the size of the committed entries which
	// can be applying at the same time.
	//
	// Despite its name (preserved for compatibility), this quota applies across
	// Ready structs to encompass all outstanding entries in unacknowledged
	// MsgStorageApply messages when AsyncStorageWrites is enabled.
	MaxCommittedSizePerReady uint64
	// MaxUncommittedEntriesSize limits the aggregate byte size of the
	// uncommitted entries that may be appended to a leader's log. Once this
	// limit is exceeded, proposals will begin to return ErrProposalDropped
	// errors. Note: 0 for no limit.
	MaxUncommittedEntriesSize uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?
	MaxInflightMsgs int
	// MaxInflightBytes limits the number of in-flight bytes in append messages.
	// Complements MaxInflightMsgs. Ignored if zero.
	//
	// This effectively bounds the bandwidth-delay product. Note that especially
	// in high-latency deployments setting this too low can lead to a dramatic
	// reduction in throughput. For example, with a peer that has a round-trip
	// latency of 100ms to the leader and this setting is set to 1 MB, there is a
	// throughput limit of 10 MB/s for this group. With RTT of 400ms, this drops
	// to 2.5 MB/s. See Little's law to understand the maths behind.
	MaxInflightBytes uint64

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	PreVote bool

	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way. Forwarding
	// should be disabled to prevent a follower with an inaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	DisableProposalForwarding bool
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}
	if IsLocalMsgTarget(c.ID) {
		return errors.New("cannot use local target as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxUncommittedEntriesSize == 0 {
		c.MaxUncommittedEntriesSize = noLimit
	}

	// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
	// previously the same parameter.
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = c.MaxSizePerMsg
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}
	if c.MaxInflightBytes == 0 {
		c.MaxInflightBytes = noLimit
	} else if c.MaxInflightBytes < c.MaxSizePerMsg {
		return errors.New("max inflight bytes must be >= max message size")
	}

	if c.Logger == nil {
		c.Logger = getLogger()
	}

	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}

	return nil
}

type raft struct {
	// 当前节点集群ID
	id uint64

	// 当前任期号
	// 如果 Message 的 Term 字段为 0，则表示该消息是本地消息，例如，后面提到的 MsgHup、 MsgProp、 MsgReadlndex 等消息，都属于本地消息
	Term uint64
	// 当前任期投票给哪个节点，未投票时，字段为 None
	Vote uint64

	readStates []ReadState

	// the log
	//
	// 本地 log
	raftLog *raftLog

	// 单条消息最大字节数
	maxMsgSize         entryEncodingSize
	maxUncommittedSize entryPayloadSize

	// TODO(tbg): rename to trk.
	// 对等节点日志复制情况（NextIndex，MarchIndex）
	prs tracker.ProgressTracker

	// 当前节点角色 "StateFollower","StateCandidate","StateLeader","StatePreCandidate"
	state StateType

	// isLearner is true if the local raft node is a learner.
	//
	// 为 true 则当前 raft 节点为 learner
	isLearner bool

	// msgs contains the list of messages that should be sent out immediately to
	// other nodes.
	//
	// Messages in this list must target other nodes.
	//
	// 等待发送的消息队列
	msgs []pb.Message

	// msgsAfterAppend contains the list of messages that should be sent after
	// the accumulated unstable state (e.g. term, vote, []entry, and snapshot)
	// has been persisted to durable storage. This includes waiting for any
	// unstable state that is already in the process of being persisted (i.e.
	// has already been handed out in a prior Ready struct) to complete.
	//
	// Messages in this list may target other nodes or may target this node.
	//
	// Messages in this list have the type MsgAppResp, MsgVoteResp, or
	// MsgPreVoteResp. See the comment in raft.send for details.
	msgsAfterAppend []pb.Message



	// the leader id
	lead uint64
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	//
	// 用于集群 Leader 节点的转移，记录此次节点转移的目标节点ID
	leadTransferee uint64
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	//
	// 每次只能有一个 conf 变更是待定的（在日志中，但尚未应用）。
	// 这一点通过 pendingConfIndex 强制执行，该值被设置为 >= 最新待定配置变更（如果有的话）的日志索引。
	// 只有当领导者的应用索引大于此值时，才允许提出配置变更。
	pendingConfIndex uint64
	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
	//
	// 对 Raft 日志中未提交的尾部大小的估计。
	// - 用来防止日志的无限制增长。
	// - 仅由领导者维护。
	// - 在 Term 变化时重置。
	uncommittedSize entryPayloadSize

	readOnly *readOnly

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	//
	// 当它是领导者或候选人时，它达到最后一次选举时间的刻度数。
	// 当它是追随者时，它达到最后一次选举时间的刻度数或收到当前领导者的有效信息的刻度数。
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	//
	//
	heartbeatElapsed int

	// leader 每隔 election timeout 检查其他节点的活跃情况，若少于 majority 活跃，则自动 step down 为 follower。
	checkQuorum bool
	// 节点进入 candidate 之前，需要连接其他节点发送消息询问是否参与选举，当超过半数节点响应并参与新一轮的选举，则可以发起新一轮选举
	preVote     bool

	// 心跳超时时间， 当 heartbeatElapsed 字段值到达该值时，就会触发 Leader 节点发送一条心跳消息。
	heartbeatTimeout int
	electionTimeout  int

	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	disableProposalForwarding bool

	tick func()
	step stepFunc

	logger Logger

	// pendingReadIndexMessages is used to store messages of type MsgReadIndex
	// that can't be answered as new leader didn't committed any log in
	// current term. Those will be handled as fast as first log is committed in
	// current term.
	pendingReadIndexMessages []pb.Message
}

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}


	raftlog := newLogWithSize(c.Storage, c.Logger, entryEncodingSize(c.MaxCommittedSizePerReady))


	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}

	r := &raft{
		id:                        c.ID,
		lead:                      None,
		isLearner:                 false,
		raftLog:                   raftlog,
		maxMsgSize:                entryEncodingSize(c.MaxSizePerMsg),
		maxUncommittedSize:        entryPayloadSize(c.MaxUncommittedEntriesSize),
		prs:                       tracker.MakeProgressTracker(c.MaxInflightMsgs, c.MaxInflightBytes),
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		logger:                    c.Logger,
		checkQuorum:               c.CheckQuorum,
		preVote:                   c.PreVote,
		readOnly:                  newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: c.DisableProposalForwarding,
	}

	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prs,
		LastIndex: raftlog.lastIndex(),
	}, cs)
	if err != nil {
		panic(err)
	}
	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))

	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied, 0 /* size */)
	}
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.prs.VoterNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) softState() SoftState { return SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

// send schedules persisting state to a stable storage and AFTER that
// sending the message (as part of next Ready message processing).
func (r *raft) send(m pb.Message) {

	//
	if m.From == None {
		m.From = r.id
	}

	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			// - MsgPreVote: m.Term is the term the node will campaign,
			//   non-zero as we use m.Term to indicate the next term we'll be
			//   campaigning for
			// - MsgPreVoteResp: m.Term is the term received in the original
			//   MsgPreVote if the pre-vote was granted, non-zero for the
			//   same reasons MsgPreVote is
			r.logger.Panicf("term should be set when sending %s", m.Type)
		}
	} else {
		if m.Term != 0 {
			r.logger.Panicf("term should not be set when sending %s (was %d)", m.Type, m.Term)
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}


	if m.Type == pb.MsgAppResp || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVoteResp {
		// If async storage writes are enabled, messages added to the msgs slice
		// are allowed to be sent out before unstable state (e.g. log entry
		// writes and election votes) have been durably synced to the local
		// disk.
		//
		// For most message types, this is not an issue. However, response
		// messages that relate to "voting" on either leader election or log
		// appends require durability before they can be sent. It would be
		// incorrect to publish a vote in an election before that vote has been
		// synced to stable storage locally. Similarly, it would be incorrect to
		// acknowledge a log append to the leader before that entry has been
		// synced to stable storage locally.
		//
		// Per the Raft thesis, section 3.8 Persisted state and server restarts:
		//
		// > Raft servers must persist enough information to stable storage to
		// > survive server restarts safely. In particular, each server persists
		// > its current term and vote; this is necessary to prevent the server
		// > from voting twice in the same term or replacing log entries from a
		// > newer leader with those from a deposed leader. Each server also
		// > persists new log entries before they are counted towards the entries’
		// > commitment; this prevents committed entries from being lost or
		// > “uncommitted” when servers restart
		//
		// To enforce this durability requirement, these response messages are
		// queued to be sent out as soon as the current collection of unstable
		// state (the state that the response message was predicated upon) has
		// been durably persisted. This unstable state may have already been
		// passed to a Ready struct whose persistence is in progress or may be
		// waiting for the next Ready struct to begin being written to Storage.
		// These messages must wait for all of this state to be durable before
		// being published.
		//
		// Rejected responses (m.Reject == true) present an interesting case
		// where the durability requirement is less unambiguous. A rejection may
		// be predicated upon unstable state. For instance, a node may reject a
		// vote for one peer because it has already begun syncing its vote for
		// another peer. Or it may reject a vote from one peer because it has
		// unstable log entries that indicate that the peer is behind on its
		// log. In these cases, it is likely safe to send out the rejection
		// response immediately without compromising safety in the presence of a
		// server restart. However, because these rejections are rare and
		// because the safety of such behavior has not been formally verified,
		// we err on the side of safety and omit a `&& !m.Reject` condition
		// above.
		r.msgsAfterAppend = append(r.msgsAfterAppend, m)
	} else {
		if m.To == r.id {
			r.logger.Panicf("message should not be self-addressed when sending %s", m.Type)
		}
		r.msgs = append(r.msgs, m)
	}


}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer.
//
// sendAppend 向特定的 Follower（由传入参数的 to 代表）发送日志同步命令。
// 该方法首先会找到该 Follower 上一次已同步的日志位置(pr.Next-1)，然后从 raftLog 中获取该位置以后的日志项进行同步；
// 当然，每次同步的数量不宜太多，由 maxMsgSize 限制。
// 当然，如果无法从 raftLog 获取到想要的日志项，此时需要考虑发送 Snapshot ，
// 这是因为对应的日志项可能由于已经被 commit 而丢弃了（向新加入节点同步日志的时候可能会出现这种情况）
func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

// maybeSendAppend sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The sendIfEmpty
// argument controls whether messages with no entries will be sent
// ("empty" messages are useful to convey updated Commit indexes, but
// are undesirable when we're sending multiple messages in a batch).
//
//
func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	// 目标节点 to 的信息
	pr := r.prs.Progress[to]
	// 检测当前节点是否可以向目标节点发送消息
	if pr.IsPaused() {
		return false
	}

	// [重要]
	//
	// pr.Next 表示要发给该 follower 的下一条日志的索引，pr.Next－1 表示上一次发给该 follower 的日志，
	//
	// 因此
	//
	// r.raftLog.term(pr.Next - 1) 表示上次发给该 follower 的日志的任期号，
	// r.raftLog.entries(pr.Next, r.maxMsgSize) 表示从 leader 日志中取出要发给该 follower 的日志条目


	// 上一次发送给该 follower 的日志索引、日志期号，下一次待发送的日志索引
	lastIndex, nextIndex := pr.Next-1, pr.Next
	lastTerm, errt := r.raftLog.term(lastIndex)

	// 取出待发送的日志条目
	var ents []pb.Entry
	var erre error
	// In a throttled StateReplicate only send empty MsgApp, to ensure progress.
	// Otherwise, if we had a full Inflights and all inflight messages were in
	// fact dropped, replication to that follower would stall. Instead, an empty
	// MsgApp will eventually reach the follower (heartbeats responses prompt the
	// leader to send an append), allowing it to be acked or rejected, both of
	// which will clear out Inflights.
	if pr.State != tracker.StateReplicate || !pr.Inflights.Full() {
		ents, erre = r.raftLog.entries(nextIndex, r.maxMsgSize)
	}

	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	// [重要][快照]
	// 对于一个 Follower ，如果需要同步给它的日志已经被回收了，那就直接发送 Snapshot 消息（MsgSnap）给该 Follower 。
	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries

		// 如果该节点已经不存活，则无需发送
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}

		// 拿到当前的 snapshot 信息
		snapshot, err := r.raftLog.snapshot()
		// 如果获取快照异常，则终止整个程序
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err) // TODO(bdarnell)
		}


		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]", r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		pr.BecomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)

		// 发送 snapshot 信息给节点
		r.send(pb.Message{To: to, Type: pb.MsgSnap, Snapshot: &snapshot})
		// 发送成功
		return true
	}

	// Send the actual MsgApp otherwise, and update the progress accordingly.
	if err := pr.UpdateOnEntriesSend(len(ents), uint64(payloadsSize(ents)), nextIndex); err != nil {
		r.logger.Panicf("%x: %v", r.id, err)
	}

	// NB: pr has been updated, but we make sure to only use its old values below.
	r.send(pb.Message{
		To:      to,		// 目标节点
		Type:    pb.MsgApp, // 消息类型：附加日志消息
		Index:   lastIndex,	// 上一次发送给该 follower 的日志索引
		LogTerm: lastTerm,	// 上一次发送给该 follower 的日志的任期号
		Entries: ents,		// 要发送的日志条目
		Commit:  r.raftLog.committed,	// leader的日志提交位置
	})

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(r.prs.Progress[to].Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}

	r.send(m)
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (r *raft) bcastAppend() {
	r.prs.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		r.sendAppend(id)
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.prs.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id, ctx)
	})
}

func (r *raft) appliedTo(index uint64, size entryEncodingSize) {
	oldApplied := r.raftLog.applied
	newApplied := max(index, oldApplied)
	r.raftLog.appliedTo(newApplied, size)

	if r.prs.Config.AutoLeave && newApplied >= r.pendingConfIndex && r.state == StateLeader {
		// If the current (and most recent, at least for this leader's term)
		// configuration should be auto-left, initiate that now. We use a
		// nil Data which unmarshals into an empty ConfChangeV2 and has the
		// benefit that appendEntry can never refuse it based on its size
		// (which registers as zero).
		m, err := confChangeToMsg(nil)
		if err != nil {
			panic(err)
		}
		// NB: this proposal can't be dropped due to size, but can be
		// dropped if a leadership transfer is in progress. We'll keep
		// checking this condition on each applied entry, so either the
		// leadership transfer will succeed and the new leader will leave
		// the joint configuration, or the leadership transfer will fail,
		// and we will propose the config change on the next advance.
		if err := r.Step(m); err != nil {
			r.logger.Debugf("not initiating automatic transition out of joint configuration %s: %v", r.prs.Config, err)
		} else {
			r.logger.Infof("initiating automatic transition out of joint configuration %s", r.prs.Config)
		}
	}
}

func (r *raft) appliedSnap(snap *pb.Snapshot) {
	index := snap.Metadata.Index
	r.raftLog.stableSnapTo(index)
	r.appliedTo(index, 0 /* size */)
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *raft) maybeCommit() bool {
	mci := r.prs.Committed()
	return r.raftLog.maybeCommit(mci, r.Term)
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.prs.ResetVotes()
	r.prs.Visit(func(id uint64, pr *tracker.Progress) {
		*pr = tracker.Progress{
			Match:     0,
			Next:      r.raftLog.lastIndex() + 1,
			Inflights: tracker.NewInflights(r.prs.MaxInflight, r.prs.MaxInflightBytes),
			IsLearner: pr.IsLearner,
		}
		if id == r.id {
			pr.Match = r.raftLog.lastIndex()
		}
	})

	r.pendingConfIndex = 0
	r.uncommittedSize = 0
	r.readOnly = newReadOnly(r.readOnly.option)
}

func (r *raft) appendEntry(es ...pb.Entry) (accepted bool) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}


	// Track the size of this uncommitted proposal.
	if !r.increaseUncommittedSize(es) {
		r.logger.Warningf(
			"%x appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
			r.id,
		)
		// Drop the proposal.
		return false
	}

	// use latest "last" index after truncate/append
	li = r.raftLog.append(es...)

	// The leader needs to self-ack the entries just appended once they have
	// been durably persisted (since it doesn't send an MsgApp to itself). This
	// response message will be added to msgsAfterAppend and delivered back to
	// this node after these entries have been written to stable storage. When
	// handled, this is roughly equivalent to:
	//
	//  r.prs.Progress[r.id].MaybeUpdate(e.Index)
	//  if r.maybeCommit() {
	//  	r.bcastAppend()
	//  }
	r.send(pb.Message{To: r.id, Type: pb.MsgAppResp, Index: li})


	return true
}

// tickElection is run by followers and candidates after r.electionTimeout.
//
//
// 选举流程了：
//
//	节点启动时都以 follower 状态启动，同时随机选择自己的选举超时时间。
//	之所以每个节点随机选择自己的超时时间，是为了避免同时有两个节点同时进行选举，
//	这种情况下会出现没有任何一个节点赢得半数以上的投票从而这一轮选举失败，继续再进行下一轮选举
//
//	在 follower 的 tick 函数 tickElection 函数中，当选举超时到时，节点向自己发送 HUP 消息。
//
//	在状态机函数 raft.Step 函数中，在收到 HUP 消息之后，节点首先判断当前有没有没有 apply 的配置变更消息，如果有就忽略该消息。
//	其原因在于，当有配置更新的情况下不能进行选举操作，即要保证每一次集群成员变化时只能同时变化一个，不能同时有多个集群成员的状态发生变化。
//
//	否则进入 campaign 函数中进行选举：
//	 首先将任期号 +1 ，
//	 然后广播给其他节点选举消息，带上的其它字段包括：
//	 	节点当前的最后一条日志索引（Index字段），
//	 	最后一条日志对应的任期号（LogTerm字段），
//	 	选举任期号（Term字段，即前面已经进行+1之后的任期号），
//	 	Context字段（目的是为了告知这一次是否是leader转让类需要强制进行选举的消息）。
//
//	如果在一个选举超时之内，该发起新的选举流程的节点，得到了超过半数的节点投票，那么状态就切换到 leader 状态，
//	成为leader的同时，leader 将发送一条 dummy 的 append 消息，目的是为了提交该节点上在此任期之前的值（见疑问部分如何提交之前任期的值）
//
func (r *raft) tickElection() {
	// 将选举超时递增 1 。
	r.electionElapsed++

	// 当选举超时到期，同时该节点又在集群中时，说明此时可以进行一轮新的选举。
	// 此时会向本节点发送 HUP 消息，这个消息最终会走到状态机函数 raft.Step 中进行处理。
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		if err := r.Step(pb.Message{From: r.id, Type: pb.MsgHup}); err != nil {
			r.logger.Debugf("error occurred during election: %v", err)
		}
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++	// 心跳计数
	r.electionElapsed++		// 选举计数

	// leader 每隔 election timeout 检查其他节点的活跃情况，若少于 majority 活跃，则自动 step down 为 follower。
	// 通过通信来判断节点活跃情况，在每次 check quorum 时清理，只要在下次 check quorum 之前有通信就是活跃的。
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			if err := r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum}); err != nil {
				r.logger.Debugf("error occurred during checking sending heartbeat: %v", err)
			}
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.state != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{From: r.id, Type: pb.MsgBeat}); err != nil {
			r.logger.Debugf("error occurred during checking sending heartbeat: %v", err)
		}
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.state = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

func (r *raft) becomePreCandidate() {
	// follower 首先变为 preCandidate，不会增加自己的 term .

	// Pre-Vote 和正常投票流程相同，但是其他节点收到 Pre-Vote 消息时不会改变自己的状态，且可以给多个 Pre-Vote 的 candidate 投票.

	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	r.step = stepCandidate
	r.prs.ResetVotes()
	r.tick = r.tickElection
	r.lead = None
	r.state = StatePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	// Followers enter replicate mode when they've been successfully probed
	// (perhaps after having received a snapshot as a result). The leader is
	// trivially in this state. Note that r.reset() has initialized this
	// progress with the last index already.
	pr := r.prs.Progress[r.id]
	pr.BecomeReplicate()
	// The leader always has RecentActive == true; MsgCheckQuorum makes sure to
	// preserve this.
	pr.RecentActive = true

	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	r.pendingConfIndex = r.raftLog.lastIndex()

	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		// This won't happen because we just called reset() above.
		r.logger.Panic("empty entry was dropped")
	}
	// The payloadSize of an empty entry is 0 (see TestPayloadSizeOfEmptyEntry),
	// so the preceding log append does not count against the uncommitted log
	// quota of the new leader. In other words, after the call to appendEntry,
	// r.uncommittedSize is still 0.
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *raft) hup(t CampaignType) {
	if r.state == StateLeader {
		r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		return
	}

	if !r.promotable() {
		r.logger.Warningf("%x is unpromotable and can not campaign", r.id)
		return
	}
	ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
	if err != nil {
		r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
	}
	if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
		r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
		return
	}

	r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
	r.campaign(t)
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
func (r *raft) campaign(t CampaignType) {
	if !r.promotable() {
		// This path should not be hit (callers are supposed to check), but
		// better safe than sorry.
		r.logger.Warningf("%x is unpromotable; campaign() should have been called", r.id)
	}

	var term uint64
	var voteMsg pb.MessageType

	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = pb.MsgPreVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		//
		// 这个 term 只会在正真开启选举时应用，即成为候选人时
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}

	// 票数不满足时, 这里主要处理，强制切主的情况
	var ids []uint64
	{
		idMap := r.prs.Voters.IDs()
		ids = make([]uint64, 0, len(idMap))
		for id := range idMap {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}

	for _, id := range ids {
		if id == r.id {
			// The candidate votes for itself and should account for this self
			// vote once the vote has been durably persisted (since it doesn't
			// send a MsgVote to itself). This response message will be added to
			// msgsAfterAppend and delivered back to this node after the vote
			// has been written to stable storage.
			r.send(pb.Message{To: id, Term: term, Type: voteRespMsgType(voteMsg)})
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(pb.Message{
			To: id,
			Term: term,
			Type: voteMsg, 	// 如果是强制切主，这里 voteMsg--->MsgVote ，当前节点状态为候选者
			Index: r.raftLog.lastIndex(),
			LogTerm: r.raftLog.lastTerm(),
			Context: ctx,
		})
	}
}

func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result quorum.VoteResult) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}

	// 记录投票: 节点 id 是否投票给本节点
	r.prs.RecordVote(id, v)
	// 返回投票结果:
	return r.prs.TallyVotes()
}

// Step 的主要作用是处理不同的消息
func (r *raft) Step(m pb.Message) error {
	// Handle the message term, which may result in our stepping down to a follower.
	switch {
	case m.Term == 0:
		// local message
	//
	// 消息的 Term 大于节点当前的 Term ，若满足则说明是一次新的选举。
	//
	// [重要]
	// 首先该函数会判断 msg.Term 是否大于本节点的 Term ，如果消息的任期号更大则说明是一次新的选举。
	// 这种情况下将根据 msg.Context 是否等于 “CampaignTransfer” 字符串来确定是不是一次由于 leader 迁移导致的强制选举过程。
	// 同时也会根据当前的 electionElapsed 是否小于 electionTimeout 来确定是否还在租约期以内。
	// 如果既不是强制 leader 选举又在租约期以内，那么节点将忽略该消息的处理，在论文4.2.3部分论述这样做的原因，
	// 是为了避免已经离开集群的节点在不知道自己已经不在集群内的情况下，仍然频繁的向集群内节点发起选举导致耗时在这种无效的选举流程中。
	//
	// 如果以上检查流程通过了，说明可以进行选举了，如果消息类型还不是 MsgPreVote 类型，那么此时节点会切换到 follower 状态，
	// 并且认为发送消息过来的节点 msg.From 是新的 leader 。
	//
	case m.Term > r.Term:
		// 如果收到的是投票类消息
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			// 当 context 为 campaignTransfer 时表示这是一次由于 leader 迁移导致的强制选举
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			// 根据当前节点的 electionElapsed 是否小于 electionTimeout 来确定是否还在租约期以内
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			// 如果非强制，而且又在租约期以内，就忽略当前消息，不做任何处理
			if !force && inLease {
				// 不是强制切主，且当节点在 election timeout 时间内接收到了 leader 的消息，
				// 就会忽略选举消息，不改变自己的 term 也不会给其他节点投票。
				// 详见论文的4.2.3，这是为了阻止已经离开集群的节点再次发起投票请求
				//
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}
		}

		switch {
		case m.Type == pb.MsgPreVote:
			// Never change our term in response to a PreVote
			//
			// 在应答一个 PreVote 消息时不对任期 term 做修改
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			// 如果收到的消息 Term 大于当前节点的 Term ，只要不是投票选举类消息，就将自己变为 follower
			if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}

	case m.Term < r.Term:
		if (r.checkQuorum || r.preVote) && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases, by notifying leader of this node's activeness.
			// The above comments also true for Pre-Vote
			//
			// When follower gets isolated, it soon starts an election ending
			// up with a higher term than leader, although it won't receive enough
			// votes to win the election. When it regains connectivity, this response
			// with "pb.MsgAppResp" of higher term would force leader to step down.
			// However, this disruption is inevitable to free this stuck node with
			// fresh election. This can be prevented with Pre-Vote phase.
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else if m.Type == pb.MsgPreVote {
			// Before Pre-Vote enable, there may have candidate with higher term,
			// but less log. After update to Pre-Vote, the cluster may deadlock if
			// we drop messages with a lower term.
			//
			// 当节点在预选成功但是正式选时被网络分区，此时其他节点选了新的主节点
			// 当网络恢复时，被分区的节点由于预选不会成功，因为新的集群已经写入新的日志，
			// 但是该节点不会接受 leader 的消息导致这个节点不会 stable ，此时收到的预选消息应该被拒绝，让节点明白时过境迁，有了新的主了.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: pb.MsgPreVoteResp, Reject: true})
		} else if m.Type == pb.MsgStorageAppendResp {
			if m.Index != 0 {
				// Don't consider the appended log entries to be stable because
				// they may have been overwritten in the unstable log during a
				// later term. See the comment in newStorageAppendResp for more
				// about this race.
				r.logger.Infof("%x [term: %d] ignored entry appends from a %s message with lower term [term: %d]",
					r.id, r.Term, m.Type, m.Term)
			}
			if m.Snapshot != nil {
				// Even if the snapshot applied under a different term, its
				// application is still valid. Snapshots carry committed
				// (term-independent) state.
				r.appliedSnap(m.Snapshot)
			}
		} else {
			// ignore other cases
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	switch m.Type {
	case pb.MsgHup:
		// 只有收到 majority 的投票，才会增加 term 进行正常的选举过程；
		// 若 Pre-Vote 失败，当网络恢复后，该节点收到 leader 的消息重新成为 follower，避免了 disrupt 集群。
		if r.preVote {
			r.hup(campaignPreElection)
		} else {
			r.hup(campaignElection)
		}

	case pb.MsgStorageAppendResp:
		if m.Index != 0 {
			r.raftLog.stableTo(m.Index, m.LogTerm)
		}
		if m.Snapshot != nil {
			r.appliedSnap(m.Snapshot)
		}

	case pb.MsgStorageApplyResp:
		if len(m.Entries) > 0 {
			index := m.Entries[len(m.Entries)-1].Index
			r.appliedTo(index, entsSize(m.Entries))
			r.reduceUncommittedSize(payloadsSize(m.Entries))
		}

	case pb.MsgVote, pb.MsgPreVote:
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.Vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.lead == None) ||
			// ...or this is a PreVote for a future term...
			(m.Type == pb.MsgPreVote && m.Term > r.Term)
		// ...and we believe the candidate is up to date.
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			// Note: it turns out that that learners must be allowed to cast votes.
			// This seems counter- intuitive but is necessary in the situation in which
			// a learner has been promoted (i.e. is now a voter) but has not learned
			// about this yet.
			// For example, consider a group in which id=1 is a learner and id=2 and
			// id=3 are voters. A configuration change promoting 1 can be committed on
			// the quorum `{2,3}` without the config change being appended to the
			// learner's log. If the leader (say 2) fails, there are de facto two
			// voters remaining. Only 3 can win an election (due to its log containing
			// all committed entries), but to do so it will need 1 to vote. But 1
			// considers itself a learner and will continue to do so until 3 has
			// stepped up as leader, replicates the conf change to 1, and 1 applies it.
			// Ultimately, by receiving a request to vote, the learner realizes that
			// the candidate believes it to be a voter, and that it should act
			// accordingly. The candidate's config may be stale, too; but in that case
			// it won't win the election, at least in the absence of the bug discussed
			// in:
			// https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why, consider the
			// case where a single node was previously partitioned away and
			// it's local term is now out of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			r.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// Only record real votes.
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		// 所有上面不能处理的消息都落入这里，由一个小写的 step 函数处理。
		err := r.step(r, m)
		if err != nil {
			return err
		}
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message) error

func stepLeader(r *raft, m pb.Message) error {

	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MsgCheckQuorum:
		if !r.prs.QuorumActive() {
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		// Mark everyone (but ourselves) as inactive in preparation for the next
		// CheckQuorum.
		r.prs.Visit(func(id uint64, pr *tracker.Progress) {
			if id != r.id {
				pr.RecentActive = false
			}
		})
		return nil
	case pb.MsgProp:
		// MsgProp 消息是集群中的节点向 leader 转发用户提交的数据。

		// 检查 entries 数组是否没有数据，这是一个保护性检查。
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}

		// 检查本节点是否还在集群之中，如果已经不在了则直接返回不进行下一步处理。
		// 什么情况下会出现一个 leader 节点发现自己不存在集群之中了？这种情况出现在本节点已经通过配置变化被移除出了集群的场景。
		if r.prs.Progress[r.id] == nil {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		}

		// 检查 raft.leadTransferee 字段，当这个字段不为 0 时说明正在进行 leader 迁移操作，这种情况下不允许提交数据变更操作，因此此时也是直接返回的。
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		// 检查消息的 entries 数组，看其中是否带有配置变更的数据。
		// 如果其中带有数据变更而 raft.pendingConf 为 true ，说明当前有未提交的配置变更数据。
		// 根据 raft 论文，每次不同同时进行一次以上的配置变更，因此这里会将 entries 数组中的配置变更数据置为空数据。
		for i := range m.Entries {
			e := &m.Entries[i]
			var cc pb.ConfChangeI
			if e.Type == pb.EntryConfChange {
				var ccc pb.ConfChange
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			} else if e.Type == pb.EntryConfChangeV2 {
				var ccc pb.ConfChangeV2
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			}
			if cc != nil {
				alreadyPending := r.pendingConfIndex > r.raftLog.applied
				alreadyJoint := len(r.prs.Config.Voters[1]) > 0
				wantsLeaveJoint := len(cc.AsV2().Changes) == 0

				var refused string
				if alreadyPending {
					refused = fmt.Sprintf("possible unapplied conf change at index %d (applied to %d)", r.pendingConfIndex, r.raftLog.applied)
				} else if alreadyJoint && !wantsLeaveJoint {
					refused = "must transition out of joint config first"
				} else if !alreadyJoint && wantsLeaveJoint {
					refused = "not in joint state; refusing empty conf change"
				}

				if refused != "" {
					r.logger.Infof("%x ignoring conf change %v at config %s: %s", r.id, cc, r.prs.Config, refused)
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				} else {
					r.pendingConfIndex = r.raftLog.lastIndex() + uint64(i) + 1
				}
			}
		}

		// 到了这里可以进行真正的数据 propose 操作了 。
		//
		// 在 raft 算法中，任何的数据要提交成功，首先 leader 会在本地写一份日志，再广播出去给集群的其他节点，
		// 只有在超过半数以上的节点同意，leader 才能进行提交操作。
		//
		// 将调用 raft 算法库的日志模块写入数据，根据返回的情况向其他节点广播消息。
		if !r.appendEntry(m.Entries...) {
			return ErrProposalDropped
		}

		r.bcastAppend()
		return nil
	case pb.MsgReadIndex:
		// only one voting member (the leader) in the cluster
		if r.prs.IsSingleton() {
			if resp := r.responseToReadIndexReq(m, r.raftLog.committed); resp.To != None {
				r.send(resp)
			}
			return nil
		}

		// Postpone read only request when this leader has not committed
		// any log entry at its term.
		if !r.committedEntryInCurrentTerm() {
			r.pendingReadIndexMessages = append(r.pendingReadIndexMessages, m)
			return nil
		}

		sendMsgReadIndexResponse(r, m)

		return nil
	}


	// All other message types require a progress for m.From (pr).
	pr := r.prs.Progress[m.From]
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.Type {
	case pb.MsgAppResp:
		// NB: this code path is also hit from (*raft).advance, where the leader steps
		// an MsgAppResp to acknowledge the appended entries in the last Ready.

		// 收到节点的 MsgAppResp 消息，说明该节点是活跃的，因此保存节点状态的 RecentActive 成员置为 true 。
		pr.RecentActive = true

		// 节点拒绝了前面的 MsgApp/MsgSnap 消息
		if m.Reject {
			// RejectHint is the suggested next base entry for appending (i.e.
			// we try to append entry RejectHint+1 next), and LogTerm is the
			// term that the follower has at index RejectHint. Older versions
			// of this library did not populate LogTerm for rejections and it
			// is zero for followers with an empty log.
			//
			// Under normal circumstances, the leader's log is longer than the
			// follower's and the follower's log is a prefix of the leader's
			// (i.e. there is no divergent uncommitted suffix of the log on the
			// follower). In that case, the first probe reveals where the
			// follower's log ends (RejectHint=follower's last index) and the
			// subsequent probe succeeds.
			//
			// However, when networks are partitioned or systems overloaded,
			// large divergent log tails can occur. The naive attempt, probing
			// entry by entry in decreasing order, will be the product of the
			// length of the diverging tails and the network round-trip latency,
			// which can easily result in hours of time spent probing and can
			// even cause outright outages. The probes are thus optimized as
			// described below.
			r.logger.Debugf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d",
				r.id, m.RejectHint, m.LogTerm, m.From, m.Index)

			// 根据 MsgAppResp 消息携带的 msg.RejectHint 信息回退 leader 上保存的关于该节点的日志记录状态。
			//
			// [重要]
			// 比如 leader 前面认为从日志索引为 10 的位置开始向节点 A 同步数据，但是节点 A 拒绝了这次数据同步，同时返回 RejectHint 为 2 ，
			// 说明节点 A 告知 leader 在它上面保存的最大日志索引 ID 为 2 ，这样下一次 leader 就可以直接从索引为 2 的日志数据开始同步数据到节点 A 。
			// 而如果没有这个 RejectHint 成员，leader 只能在每次被拒绝数据同步后都递减 1 进行下一次数据同步，显然这样是低效的。
			nextProbeIdx := m.RejectHint
			if m.LogTerm > 0 {
				// If the follower has an uncommitted log tail, we would end up
				// probing one by one until we hit the common prefix.
				//
				// For example, if the leader has:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 5 5 5 5 5
				//   term (F)   1 1 1 1 2 2
				//
				// Then, after sending an append anchored at (idx=9,term=5) we
				// would receive a RejectHint of 6 and LogTerm of 2. Without the
				// code below, we would try an append at index 6, which would
				// fail again.
				//
				// However, looking only at what the leader knows about its own
				// log and the rejection hint, it is clear that a probe at index
				// 6, 5, 4, 3, and 2 must fail as well:
				//
				// For all of these indexes, the leader's log term is larger than
				// the rejection's log term. If a probe at one of these indexes
				// succeeded, its log term at that index would match the leader's,
				// i.e. 3 or 5 in this example. But the follower already told the
				// leader that it is still at term 2 at index 6, and since the
				// log term only ever goes up (within a log), this is a contradiction.
				//
				// At index 1, however, the leader can draw no such conclusion,
				// as its term 1 is not larger than the term 2 from the
				// follower's rejection. We thus probe at 1, which will succeed
				// in this example. In general, with this approach we probe at
				// most once per term found in the leader's log.
				//
				// There is a similar mechanism on the follower (implemented in
				// handleAppendEntries via a call to findConflictByTerm) that is
				// useful if the follower has a large divergent uncommitted log
				// tail[1], as in this example:
				//
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 3 3 3 3 3 3 3 7
				//   term (F)   1 3 3 4 4 5 5 5 6
				//
				// Naively, the leader would probe at idx=9, receive a rejection
				// revealing the log term of 6 at the follower. Since the leader's
				// term at the previous index is already smaller than 6, the leader-
				// side optimization discussed above is ineffective. The leader thus
				// probes at index 8 and, naively, receives a rejection for the same
				// index and log term 5. Again, the leader optimization does not improve
				// over linear probing as term 5 is above the leader's term 3 for that
				// and many preceding indexes; the leader would have to probe linearly
				// until it would finally hit index 3, where the probe would succeed.
				//
				// Instead, we apply a similar optimization on the follower. When the
				// follower receives the probe at index 8 (log term 3), it concludes
				// that all of the leader's log preceding that index has log terms of
				// 3 or below. The largest index in the follower's log with a log term
				// of 3 or below is index 3. The follower will thus return a rejection
				// for index=3, log term=3 instead. The leader's next probe will then
				// succeed at that index.
				//
				// [1]: more precisely, if the log terms in the large uncommitted
				// tail on the follower are larger than the leader's. At first,
				// it may seem unintuitive that a follower could even have such
				// a large tail, but it can happen:
				//
				// 1. Leader appends (but does not commit) entries 2 and 3, crashes.
				//   idx        1 2 3 4 5 6 7 8 9
				//              -----------------
				//   term (L)   1 2 2     [crashes]
				//   term (F)   1
				//   term (F)   1
				//
				// 2. a follower becomes leader and appends entries at term 3.
				//              -----------------
				//   term (x)   1 2 2     [down]
				//   term (F)   1 3 3 3 3
				//   term (F)   1
				//
				// 3. term 3 leader goes down, term 2 leader returns as term 4
				//    leader. It commits the log & entries at term 4.
				//
				//              -----------------
				//   term (L)   1 2 2 2
				//   term (x)   1 3 3 3 3 [down]
				//   term (F)   1
				//              -----------------
				//   term (L)   1 2 2 2 4 4 4
				//   term (F)   1 3 3 3 3 [gets probed]
				//   term (F)   1 2 2 2 4 4 4
				//
				// 4. the leader will now probe the returning follower at index
				//    7, the rejection points it at the end of the follower's log
				//    which is at a higher log term than the actually committed
				//    log.
				nextProbeIdx, _ = r.raftLog.findConflictByTerm(m.RejectHint, m.LogTerm)
			}

			// 当 leader 收到附加日志拒绝消息时，说明 p.Next 太大了，
			// 导致 leader 在 p.Next-1 位置的日志没有与 follower 匹配上，需要将 pr.Next 降低为 pr.Match+1 ，
			// 然后转变为 StateProbe 状态，去探测 follower 与 leader 的日志匹配位置。

			if pr.MaybeDecrTo(m.Index, nextProbeIdx) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				// 如果对应的 Progress 处于 StateReplicate 状态，则切换成 StateProbe 状态，试探 Follower 的匹配位置；
				// 这里的试探是指发送一条消息并等待其相应之后，再发送后续的消息。
				if pr.State == tracker.StateReplicate {
					pr.BecomeProbe()
				}
				// 再次向对应 Follower 节点发送 MsgApp 消息，在 sendAppend() 方法中会将对应的 Progress.pause 字段设立为 true ，
				// 从而暂停后续消息的发送，从而实现前面说的“试探”的效果。
				r.sendAppend(m.From)
			}

		} else {

			// 当 leader 收到 Append 日志成功消息时，则要更新 pr.Match 和 pr.Next ，
			// m.Index 是 follower 的最新日志位置，要设置 pr.Match=m.Index, pr.Next=m.Index+1 。
			// 每次 Append 日志成功，就尝试提交下可以提交的日志(r.maybeCommit())，
			// 如果日志复制到了过半数 server ，说明可以提交了，便向其他 follower 发送日志提交请求(r.bcastAppend())

			oldPaused := pr.IsPaused()
			// MsgAppResp 消息的 Index 字段是对应 Follower 节点 raftLog 中最后一条 Entry 记录的索引，
			// 这里会根据该值更新其对应 Progress 实例的 Match 和 Next。
			if pr.MaybeUpdate(m.Index) {
				switch {
				case pr.State == tracker.StateProbe:
					// 一旦 MsgApp 被 Follower 节点接收，则表示已经找到其正确的 Next 和 Match ，不必再进行“试探”，
					// 这里将对应的 Progress.state 切换成 StateReplicate 。
					pr.BecomeReplicate()
				case pr.State == tracker.StateSnapshot && pr.Match >= pr.PendingSnapshot:
					// TODO(tbg): we should also enter this branch if a snapshot is
					// received that is below pr.PendingSnapshot but which makes it
					// possible to use the log again.
					r.logger.Debugf("%x recovered from needing snapshot, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					// Transition back to replicating state via probing state
					// (which takes the snapshot into account). If we didn't
					// move to replicating state, that would only happen with
					// the next round of appends (but there may not be a next
					// round for a while, exposing an inconsistent RaftStatus).
					pr.BecomeProbe()
					pr.BecomeReplicate()
				case pr.State == tracker.StateReplicate:
					// 之前向某个 Follower 节点发送 MsgApp 消息时，会将其相关信息保存到对应的 Progress.ins 中，
					// 在这里收到相应的 MsgAppResp 响应之后，会将其从 ins 中删除，这样可以实现了限流的效采，
					// 避免网络出现延迟时继续发送消息，从而导致网络更加拥堵。
					pr.Inflights.FreeLE(m.Index)
				}


				// 本地提交：
				// 收到一个 Follower 节点的 MsgAppResp 消息之后，除了修改相应的 Match 和 Next ，还会尝试更新 raftLog.committed ，
				// 因为有些 Entry 记录可能在此次复制中被保存到了半数以上的节点中。
				if r.maybeCommit() {
					// committed index has progressed for the term, so it is safe
					// to respond to pending read index requests
					//
					//
					releasePendingReadIndexMessages(r)
					// 集群提交：向所有节点发送 MsgApp 消息，注意，此次 MsgApp 消息的 Commit 字段与上次 MsgApp 消息已经不同
					r.bcastAppend()
				} else if oldPaused { // 之前是 pause 状态，现在可以任性地发消息了
					// If we were paused before, this node may be missing the
					// latest commit index, so send it.
					//
					// 之前 Leader 节点暂停向该 Follower 节点发送消息，收到 MsgAppResp 消息后，
					// 在上述代码中已经重立了相应状态，所以可以继续发送 MsgApp 消息
					r.sendAppend(m.From)
				}

				// We've updated flow control information above, which may
				// allow us to send multiple (size-limited) in-flight messages
				// at once (such as when transitioning from probe to
				// replicate, or when freeTo() covers multiple messages). If
				// we have more entries to send, send as many messages as we
				// can (without sending empty messages for the commit index)
				if r.id != m.From {
					for r.maybeSendAppend(m.From, false /* sendIfEmpty */) {
					}
				}
				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}

	case pb.MsgHeartbeatResp:
		pr.RecentActive = true // 对端仍旧活跃
		pr.MsgAppFlowPaused = false	//

		// NB: if the follower is paused (full Inflights), this will still send an
		// empty append, allowing it to recover from situations in which all the
		// messages that filled up Inflights in the first place were dropped. Note
		// also that the outgoing heartbeat already communicated the commit index.
		//
		// 比较该 follower 与 leader 日志的匹配位置 pr.Match 与 leader 日志的最新位置，
		// 如果两个位置不相等，说明还有日志需要发送给该 follower ，最终使得该 follower 的日志追上 leader 的日志。
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return nil
		}

		// 统计票数，如果没有达到大多数就直接返回，等待下一个心跳返回，直到满足大多数，便进行下一步
		if r.prs.Voters.VoteResult(r.readOnly.recvAck(m.From, m.Context)) != quorum.VoteWon {
			return nil
		}

		// 拿到当前 ctx 以及之前的 ctx 的对应的状态信息，返回给对应的节点
		// 由于是追加式加入 readIndex-->ctx[唯一radOnly ID]，所以这里可以采取当前 ctx 及之前的
		rss := r.readOnly.advance(m)
		for _, rs := range rss {
			if resp := r.responseToReadIndexReq(rs.req, rs.index); resp.To != None {
				r.send(resp)
			}
		}
	case pb.MsgSnapStatus:
		if pr.State != tracker.StateSnapshot {
			return nil
		}
		// TODO(tbg): this code is very similar to the snapshot handling in
		// MsgAppResp above. In fact, the code there is more correct than the
		// code here and should likely be updated to match (or even better, the
		// logic pulled into a newly created Progress state machine handler).
		if !m.Reject {
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			// NB: the order here matters or we'll be probing erroneously from
			// the snapshot index, but the snapshot never applied.
			pr.PendingSnapshot = 0
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the MsgAppResp from the remote node before sending
		// out the next MsgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		pr.MsgAppFlowPaused = true
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.State == tracker.StateReplicate {
			pr.BecomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:

		// 这类消息 follower 将转发给 leader 处理，因为 follower 并没有修改集群配置状态的权限。
		//
		// leader 在收到这类消息时，是以下的处理流程。
		//	(1)
		//	如果当前的 raft.leadTransferee 成员不为空，说明有正在进行的 leader 迁移流程。
		//	此时会判断是否与这次迁移是同样的新 leader ID，如果是则忽略该消息直接返回；否则将终止前面还没有完毕的迁移流程。
		//
		//	(2)
		//	如果这次迁移过去的新节点，就是当前的 leader ID，也直接返回不进行处理。
		//
		//	(3)
		//	到了这一步就是正式开始这一次的迁移 leader 流程了，一个节点能成为一个集群的 leader ，
		//	其必要条件是上面的日志与当前 leader 的一样多，所以这里会判断是否满足这个条件，
		//	如果满足那么发送 MsgTimeoutNow 消息给新的 leader 通知该节点进行 leader 迁移，
		//	否则就先进行日志同步操作让新的 leader 追上旧 leader 的日志数据。


		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return nil
		}

		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			// 判断是否已经有相同节点的 leader 转让流程在进行中
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				// 如果是，直接返回
				return nil
			}
			// 否则中断之前的转让流程
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}

		// 判断是否转让过来的 leader 是否本节点，如果是也直接返回，因为本节点已经是 leader 了
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			// 如果日志已经匹配了，那么就发送 timeout now 协议过去
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			// 否则继续追加日志
			r.sendAppend(leadTransferee)
		}
	}
	return nil
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
func stepCandidate(r *raft, m pb.Message) error {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}

	switch m.Type {
	case pb.MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MsgApp:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleSnapshot(m)

	case myVoteRespType:
		// 计算当前集群中有多少节点给自己投了票
		gr, rj, res := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.Type, rj)
		switch res {
		// 选举成功
		case quorum.VoteWon:
			if r.state == StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				// 变成 leader
				r.becomeLeader()
				r.bcastAppend()
			}
		// 选举失败
		case quorum.VoteLost:
			// pb.MsgPreVoteResp contains future term of pre-candidate
			// m.Term > r.Term; reuse r.Term
			r.becomeFollower(r.Term, None)
		}
	case pb.MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}

	return nil
}

func stepFollower(r *raft, m pb.Message) error {
	switch m.Type {
	case pb.MsgProp:
		//
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return ErrProposalDropped
		}

		// 转发给 leader
		m.To = r.lead
		r.send(m)

	case pb.MsgApp:
		// MsgApp 是 leader 节点向集群中其他节点同步数据的。

		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
		// Leadership transfers never use pre-vote even if r.preVote is true; we
		// know we are not recovering from a partition so there is no need for the
		// extra round trip.
		r.hup(campaignTransfer)
	case pb.MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return nil
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

func (r *raft) handleAppendEntries(m pb.Message) {

	// m.Index 表示 leader 发送给 follower 的上一条日志的索引位置，
	// 如果当前 follower 在 Index 位置的日志已经提交过了(有可能该 leader 是刚选举产生的，没有 follower 的日志信息，所以设置 m.Index=0 )，
	// 则不能进行追加操作，在前面在 Raft 协议时捉到过，己提交的记录不能被覆盖，
	// 所以 Follower 节点会将其 committed 位置通过 MsgAppResp 消息（ Index 字段）通知 Leader 节点，
	// 让 leader 从该 follower 的 committed 位置的下一条位置的日志开始发送给 follower 。
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}

	// 尝试将消息携带的 Entry 记录追加到 raftLog 中，可能存在冲突，因此需要先找到冲突的位置，然后用 leader 发送来的日志中从冲突位置开始覆盖 follower 的日志。
	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		// 如追加成功，则将最后一条记录的索引位(mlastIndex)通过 MsgAppResp 消息返回给 Leader 节点，
		// 这样 Leader 节点就可以根据此值更新其对应的 Next 和 Match 值。
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
		return
	}

	// 如采追加记录失败，则将失败信息返回给 Leader 节点（即 MsgAppResp 消息的 Reject 字段为 true )，
	// 同时返回的还有一些提示信息（ RejectHint 字段保存了当前节点 raftLog 中最后一条记录的索引）

	r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
		r.id, r.raftLog.zeroTermOnOutOfBounds(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)

	// Our log does not match the leader's at index m.Index. Return a hint to the
	// leader - a guess on the maximal (index, term) at which the logs match. Do
	// this by searching through the follower's log for the maximum (index, term)
	// pair with a term <= the MsgApp's LogTerm and an index <= the MsgApp's
	// Index. This can help skip all indexes in the follower's uncommitted tail
	// with terms greater than the MsgApp's LogTerm.
	//
	// See the other caller for findConflictByTerm (in stepLeader) for a much more
	// detailed explanation of this mechanism.

	// NB: m.Index >= raftLog.committed by now (see the early return above), and
	// raftLog.lastIndex() >= raftLog.committed by invariant, so min of the two is
	// also >= raftLog.committed. Hence, the findConflictByTerm argument is within
	// the valid interval, which then will return a valid (index, term) pair with
	// a non-zero term (unless the log is empty). However, it is safe to send a zero
	// LogTerm in this response in any case, so we don't verify it here.
	hintIndex := min(m.Index, r.raftLog.lastIndex())
	hintIndex, hintTerm := r.raftLog.findConflictByTerm(hintIndex, m.LogTerm)

	// 如果 leader 与 follower 的日志还没有匹配上，那么把 follower 的最新日志的索引位置告诉 leader ，
	// 以便 leader 下一次从该 follower 的最新日志位置之后开始尝试发送附加日志，直到 leader 与 follower 的日志匹配上了就能追加日志成功了。
	r.send(pb.Message{
		To:         m.From,
		Type:       pb.MsgAppResp,
		Index:      m.Index,
		Reject:     true,		// 拒绝
		RejectHint: hintIndex,	// follower 的最新日志的索引
		LogTerm:    hintTerm,
	})
}

func (r *raft) handleHeartbeat(m pb.Message) {
	r.raftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}



//
//
// 对于 Follower 节点：
//
// 如果本地日志已经包含了 snapshot 中的日志，那么就什么都不用做了，直接提交即可；
// 否则，将内存中的日志项清空，并将 Leader 发过来的 snapshot 存储在内存日志结构中（参见 raftLog.restore() ）。
// 同时，snapshot 记录了彼时刻的集群配置信息，既然要恢复成 snapshot 时的状态，也必须得按照该集群配置去重构本地的节点拓扑。
//
// 与节点自身主动进行的 snapshot 过程所不同的是，Follower 节点被动接受的 Leader 复制的 snapshot 后，需要将该 snapshot 更新至当前节点的应用状态机。
//
// Follower 节点接收并存储了 Leader 复制而来的 snapshot 后，更新应用状态的大致的过程是：
//	Follower 节点的 raft 内部状态机会将 unstable log 中的 snapshot 信息放在 Ready 结构中，
//	应用通过 Ready() 接口获取到 snapshot 信息，然后在内存中重放：

func (r *raft) handleSnapshot(m pb.Message) {
	// MsgSnap messages should always carry a non-nil Snapshot, but err on the
	// side of safety and treat a nil Snapshot as a zero-valued Snapshot.
	var s pb.Snapshot
	if m.Snapshot != nil {
		s = *m.Snapshot
	}

	sindex, sterm := s.Metadata.Index, s.Metadata.Term

	// 将快照存在 raftLog 的 unstable 结构中, 会由 node.run 方法基于 Ready 拿到当前的快照数据
	if r.restore(s) {
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		// 发送回复消息, 如果快照存在且没有错误, 这里的 Index 应该去取快照中的最后一个日志
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		// 这个快照存在问题，或者当前节点的 Index 大于快照的 index ，则返回当前已提交的 Index
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine. If this method returns false, the snapshot was
// ignored, either because it was obsolete or because of an error.
func (r *raft) restore(s pb.Snapshot) bool {

	// 如果当前快照的 Index 小于本 node 的 index ，就忽略这个快照消息
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}


	if r.state != StateFollower {
		// This is defense-in-depth: if the leader somehow ended up applying a
		// snapshot, it could move into a new term without moving into a
		// follower state. This should never fire, but if it did, we'd have
		// prevented damage by returning early, so log only a loud warning.
		//
		// At the time of writing, the instance is guaranteed to be in follower
		// state when this method is called.
		r.logger.Warningf("%x attempted to restore snapshot as leader; should never happen", r.id)
		r.becomeFollower(r.Term+1, None)
		return false
	}

	// More defense-in-depth: throw away snapshot if recipient is not in the
	// config. This shouldn't ever happen (at the time of writing) but lots of
	// code here and there assumes that r.id is in the progress tracker.
	found := false
	cs := s.Metadata.ConfState

	for _, set := range [][]uint64{
		cs.Voters,
		cs.Learners,
		cs.VotersOutgoing,
		// `LearnersNext` doesn't need to be checked. According to the rules, if a peer in
		// `LearnersNext`, it has to be in `VotersOutgoing`.
	} {
		for _, id := range set {
			if id == r.id {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		r.logger.Warningf(
			"%x attempted to restore snapshot but it is not in the ConfState %v; should never happen",
			r.id, cs,
		)
		return false
	}

	// Now go ahead and actually restore.
	// 如果本地的 raft log 已经包含了 Snapshot 的日志项，那直接提交即可，什么都不用做
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.raftLog.commitTo(s.Metadata.Index)
		return false
	}

	// 恢复内存日志项(其实主要是 unstable )为空，并记录下 snapshot 信息
	r.raftLog.restore(s)

	// Reset the configuration and add the (potentially updated) peers in anew.
	r.prs = tracker.MakeProgressTracker(r.prs.MaxInflight, r.prs.MaxInflightBytes)
	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prs,
		LastIndex: r.raftLog.lastIndex(),
	}, cs)

	if err != nil {
		// This should never happen. Either there's a bug in our config change
		// handling or the client corrupted the conf change.
		panic(fmt.Sprintf("unable to restore config %+v: %s", cs, err))
	}

	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))

	pr := r.prs.Progress[r.id]
	pr.MaybeUpdate(pr.Next - 1) // TODO(tbg): this is untested and likely unneeded

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] restored snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
	return true
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *raft) promotable() bool {
	pr := r.prs.Progress[r.id]
	return pr != nil && !pr.IsLearner && !r.raftLog.hasNextOrInProgressSnapshot()
}

func (r *raft) applyConfChange(cc pb.ConfChangeV2) pb.ConfState {
	cfg, prs, err := func() (tracker.Config, tracker.ProgressMap, error) {
		changer := confchange.Changer{
			Tracker:   r.prs,
			LastIndex: r.raftLog.lastIndex(),
		}
		if cc.LeaveJoint() {
			return changer.LeaveJoint()
		} else if autoLeave, ok := cc.EnterJoint(); ok {
			return changer.EnterJoint(autoLeave, cc.Changes...)
		}
		return changer.Simple(cc.Changes...)
	}()

	if err != nil {
		// TODO(tbg): return the error to the caller.
		panic(err)
	}

	return r.switchToConfig(cfg, prs)
}

// switchToConfig reconfigures this node to use the provided configuration. It
// updates the in-memory state and, when necessary, carries out additional
// actions such as reacting to the removal of nodes or changed quorum
// requirements.
//
// The inputs usually result from restoring a ConfState or applying a ConfChange.
func (r *raft) switchToConfig(cfg tracker.Config, prs tracker.ProgressMap) pb.ConfState {
	r.prs.Config = cfg
	r.prs.Progress = prs

	r.logger.Infof("%x switched to configuration %s", r.id, r.prs.Config)
	cs := r.prs.ConfState()
	pr, ok := r.prs.Progress[r.id]

	// Update whether the node itself is a learner, resetting to false when the
	// node is removed.
	r.isLearner = ok && pr.IsLearner

	if (!ok || r.isLearner) && r.state == StateLeader {
		// This node is leader and was removed or demoted. We prevent demotions
		// at the time writing but hypothetically we handle them the same way as
		// removing the leader: stepping down into the next Term.
		//
		// TODO(tbg): step down (for sanity) and ask follower with largest Match
		// to TimeoutNow (to avoid interruption). This might still drop some
		// proposals but it's better than nothing.
		//
		// TODO(tbg): test this branch. It is untested at the time of writing.
		return cs
	}

	// The remaining steps only make sense if this node is the leader and there
	// are other nodes.
	if r.state != StateLeader || len(cs.Voters) == 0 {
		return cs
	}

	if r.maybeCommit() {
		// If the configuration change means that more entries are committed now,
		// broadcast/append to everyone in the updated config.
		r.bcastAppend()
	} else {
		// Otherwise, still probe the newly added replicas; there's no reason to
		// let them wait out a heartbeat interval (or the next incoming
		// proposal).
		r.prs.Visit(func(id uint64, pr *tracker.Progress) {
			if id == r.id {
				return
			}
			r.maybeSendAppend(id, false /* sendIfEmpty */)
		})
	}
	// If the leadTransferee was removed or demoted, abort the leadership transfer.
	if _, tOK := r.prs.Config.Voters.IDs()[r.leadTransferee]; !tOK && r.leadTransferee != 0 {
		r.abortLeaderTransfer()
	}

	return cs
}

func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true if r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
}

func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// committedEntryInCurrentTerm return true if the peer has committed an entry in its term.
func (r *raft) committedEntryInCurrentTerm() bool {
	// NB: r.Term is never 0 on a leader, so if zeroTermOnOutOfBounds returns 0,
	// we won't see it as a match with r.Term.
	return r.raftLog.zeroTermOnOutOfBounds(r.raftLog.term(r.raftLog.committed)) == r.Term
}

// responseToReadIndexReq constructs a response for `req`. If `req` comes from the peer
// itself, a blank value will be returned.
func (r *raft) responseToReadIndexReq(req pb.Message, readIndex uint64) pb.Message {
	if req.From == None || req.From == r.id {
		r.readStates = append(r.readStates, ReadState{
			Index:      readIndex,
			RequestCtx: req.Entries[0].Data,
		})
		return pb.Message{}
	}
	return pb.Message{
		Type:    pb.MsgReadIndexResp,
		To:      req.From,
		Index:   readIndex,
		Entries: req.Entries,
	}
}

// increaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its maxUncommittedSize limit.
// If the new entries would exceed the limit, the method returns false. If not,
// the increase in uncommitted entry size is recorded and the method returns
// true.
//
// Empty payloads are never refused. This is used both for appending an empty
// entry at a new leader's term, as well as leaving a joint configuration.
func (r *raft) increaseUncommittedSize(ents []pb.Entry) bool {
	s := payloadsSize(ents)
	if r.uncommittedSize > 0 && s > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		// If the uncommitted tail of the Raft log is empty, allow any size
		// proposal. Otherwise, limit the size of the uncommitted tail of the
		// log and drop any proposal that would push the size over the limit.
		// Note the added requirement s>0 which is used to make sure that
		// appending single empty entries to the log always succeeds, used both
		// for replicating a new leader's initial empty entry, and for
		// auto-leaving joint configurations.
		return false
	}
	r.uncommittedSize += s
	return true
}

// reduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
func (r *raft) reduceUncommittedSize(s entryPayloadSize) {
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}

func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange || ents[i].Type == pb.EntryConfChangeV2 {
			n++
		}
	}
	return n
}

func releasePendingReadIndexMessages(r *raft) {
	if len(r.pendingReadIndexMessages) == 0 {
		// Fast path for the common case to avoid a call to storage.LastIndex()
		// via committedEntryInCurrentTerm.
		return
	}
	if !r.committedEntryInCurrentTerm() {
		r.logger.Error("pending MsgReadIndex should be released only after first commit in current term")
		return
	}

	msgs := r.pendingReadIndexMessages
	r.pendingReadIndexMessages = nil

	for _, m := range msgs {
		sendMsgReadIndexResponse(r, m)
	}
}

func sendMsgReadIndexResponse(r *raft, m pb.Message) {
	// thinking: use an internally defined context instead of the user given context.
	// We can express this in terms of the term and index instead of a user-supplied value.
	// This would allow multiple reads to piggyback on the same message.
	switch r.readOnly.option {
	// If more than the local vote is needed, go through a full broadcast.
	case ReadOnlySafe:
		r.readOnly.addRequest(r.raftLog.committed, m)
		// The local node automatically acks the request.
		r.readOnly.recvAck(r.id, m.Entries[0].Data)
		r.bcastHeartbeatWithCtx(m.Entries[0].Data)
	case ReadOnlyLeaseBased:
		if resp := r.responseToReadIndexReq(m, r.raftLog.committed); resp.To != None {
			r.send(resp)
		}
	}
}
