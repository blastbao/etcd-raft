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
	"context"
	"errors"

	pb "go.etcd.io/raft/v3/raftpb"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState = pb.HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
//
// SoftState 指易变的状态数据，记录了当前的 Leader 的 Node ID ，以及当前节点的角色。
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
//
//
// Ready 结构体：
//
// 它是让应用层感知 raft 节点状态变化的传递对象，是由 node 生成传递给上层应用的，
// 里面封装了 raft 节点状态的变化、待写入 WAL 和写入 MemoryStorage 的 Entry 日志、commit 了待应用的日志、快照数据和待发送的消息等信息，
// 当这些有一个有更改时 node 就会检测到并生成 Ready 实例写到一个 channel 中，上层应用会调用 node 的 api 方法读取出来进行处理，
// 每次只能进行一个 Ready 对象的处理，Ready 对象如果上层还没处理完，不会产生下一个 Ready 对象，当上层应用处理完了，
// 会通过调用 node 节点 Advance 方法来告知 Ready 实例已经应用完了。
//
// Ready 中成员变量：
//	SoftState：表示的是对应的各个节点的可变状态，与 raft 论文中相对应。
//	HardState：表示的是当前节点的 Term/Vote/Commit 信息，这些需要被持久化。
//	ReadStates：主要用于实现线性一致性读，这里先不去深究。
//	Entries：需要持久化保存的日志，主要是 raft 协议层中不稳定的日志，也就是没有提交的日志。
//	Snapshot：需要持久化保存的快照。
//	CommittedEntries：被提交的日志，但是还没有提交到状态机。
//	Messages：在 Entries 被持久化入日志后，需要被发送到 Follower 节点的消息。
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	//
	// 当前 node 的状态信息，主要记录了
	//	- Leader 是谁 ？
	//	- 当前 node 处于什么状态，是 Leader，还是 Follower ？
	// 用于更新 etcd server 的状态
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	//
	// HardState will be equal to empty state if there is no update.
	//
	// If async storage writes are enabled, this field does not need to be acted
	// on immediately. It will be reflected in a MsgStorageAppend message in the
	// Messages slice.
	//
	// 包含当前节点见过的最大的 term ，以及在这个 term 给谁投过票，以及当前节点知道的 commit index ，这部分数据会持久化
	pb.HardState

	// ReadStates can be used for node to serve linearizable read requests locally
	// when its applied index is greater than the index in ReadState.
	// Note that the readState will be returned when raft receives msgReadIndex.
	// The returned is only valid for the request that requested to read.
	//
	// 用于返回已经确认 Leader 身份的 read 请求的 commit index
	ReadStates []ReadState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	//
	// If async storage writes are enabled, this field does not need to be acted
	// on immediately. It will be reflected in a MsgStorageAppend message in the
	// Messages slice.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	//
	// If async storage writes are enabled, this field does not need to be acted
	// on immediately. It will be reflected in a MsgStorageAppend message in the
	// Messages slice.
	//
	// 需要持久化的快照
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been appended to stable
	// storage.
	//
	// If async storage writes are enabled, this field does not need to be acted
	// on immediately. It will be reflected in a MsgStorageApply message in the
	// Messages slice.
	//
	// 已经 commit 了，还没有 apply 到状态机的日志
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages.
	//
	// If async storage writes are not enabled, these messages must be sent
	// AFTER Entries are appended to stable storage.
	//
	// If async storage writes are enabled, these messages can be sent
	// immediately as the messages that have the completion of the async writes
	// as a precondition are attached to the individual MsgStorage{Append,Apply}
	// messages instead.
	//
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	//
	// 需要发送给 peers 的消息
	Messages []pb.Message

	// MustSync indicates whether the HardState and Entries must be durably
	// written to disk or if a non-durable write is permissible.
	MustSync bool
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

// Node represents a node in a raft cluster.
type Node interface {

	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	//
	// 逻辑时钟
	Tick()

	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log. Note that proposals can be lost without
	// notice, therefore it is user's job to ensure proposal retries.
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes a configuration change. Like any proposal, the
	// configuration change may be dropped with or without an error being
	// returned. In particular, configuration changes are dropped unless the
	// leader has certainty that there is no prior unapplied configuration
	// change in its log.
	//
	// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
	// message. The latter allows arbitrary configuration changes via joint
	// consensus, notably including replacing a voter. Passing a ConfChangeV2
	// message is only allowed if all Nodes participating in the cluster run a
	// version of this library aware of the V2 API. See pb.ConfChangeV2 for
	// usage details and semantics.
	ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error

	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	Step(ctx context.Context, msg pb.Message) error

	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by Ready (unless
	// async storage writes is enabled, in which case it should never be called).
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	//
	// NOTE: Advance must not be called when using AsyncStorageWrites. Response messages from the
	// local append and apply threads take its place.
	Advance()
	// ApplyConfChange applies a config change (previously passed to
	// ProposeConfChange) to the node. This must be called whenever a config
	// change is observed in Ready.CommittedEntries, except when the app decides
	// to reject the configuration change (i.e. treats it as a noop instead), in
	// which case it must not be called.
	//
	// Returns an opaque non-nil ConfState protobuf which must be recorded in
	// snapshots.
	ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState

	// TransferLeadership attempts to transfer leadership to the given transferee.
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	// ReadIndex request a read state. The read state will be set in the ready.
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	// Note that request can be lost without notice, therefore it is user's job
	// to ensure read index retries.
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot. The id is the raft ID of the follower
	// who is meant to receive the snapshot, and the status is SnapshotFinish or SnapshotFailure.
	// Calling ReportSnapshot with SnapshotFinish is a no-op. But, any failure in applying a
	// snapshot (for e.g., while streaming it from leader to follower), should be reported to the
	// leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
	// log probes until the follower can apply the snapshot and advance its state. If the follower
	// can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
	// updates from the leader. Therefore, it is crucial that the application ensures that any
	// failure in snapshot sending is caught and reported back to the leader; so it can resume raft
	// log probing in the follower.
	ReportSnapshot(id uint64, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.
	Stop()
}

type Peer struct {
	ID      uint64
	Context []byte
}

func setupNode(c *Config, peers []Peer) *node {
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}

	// 初始化一个实际节点，里面包含一个 raft ，实现具体的 raft 的协议
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}

	// 在启动前会先加载配置，如果 Storage 接口方法返回非空这个方法将会返回错误，即初始化启动时 Storage 接口返回应该为空
	err = rn.Bootstrap(peers)
	if err != nil {
		c.Logger.Warningf("error occurred during starting a new node: %v", err)
	}

	// 包含一个 rawNode ，同时包含一些 channel 用于接受传入的消息，通过这个 node 将消息传入 raft
	n := newNode(rn)
	return &n
}

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
//
// 直接调用 raft 包有两种方法：StartNode 和 RestartNode 。
//
// 方案一：
// 启动时会检查同辈节点，如果同辈节点为空就会建议采用第二种方案。
func StartNode(c *Config, peers []Peer) Node {
	n := setupNode(c, peers)
	go n.run()
	return n
}

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
//
// 方案二：
// RestartNode 是一种重启方案，不会去检查同辈节点，集群成员关系将会从 Storage 加载，
// Storage 是 raft 提供给用户自定义持久化数据的接口，换言之我们需要在 Storage 中保存集群的成员关系。
func RestartNode(c *Config) Node {
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	n := newNode(rn)
	go n.run()
	return &n
}

type msgWithResult struct {
	m      pb.Message
	result chan error
}

// node is the canonical implementation of the Node interface
type node struct {
	propc      chan msgWithResult	 // 向 raft StateMachine 提交一个 Propose（normal op/conf change）
	recvc      chan pb.Message		 // 向 raft StateMachine 提交 Peer 发送过来的一些 Message，例如一些 Response，或者对 Follower 来说各种 request message
	confc      chan pb.ConfChangeV2
	confstatec chan pb.ConfState
	readyc     chan Ready			 // 向上层应用 raftNode 输出 Ready 好的数据和状态
	advancec   chan struct{}		 // 用于 raftNode 通知 raft StateMachine 当前 Ready 处理完了，准备下一个
	tickc      chan struct{}		 // 用于 raftNode 通知 raft StateMachine，滴答逻辑时钟推进
	done       chan struct{}
	stop       chan struct{}
	status     chan chan Status      // 向上层应用输出 raft state machine 状态

	rn *RawNode
}

func newNode(rn *RawNode) node {
	return node{
		propc:      make(chan msgWithResult),
		recvc:      make(chan pb.Message),
		confc:      make(chan pb.ConfChangeV2),
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickc:  make(chan struct{}, 128),
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
		status: make(chan chan Status),
		rn:     rn,
	}
}

func (n *node) Stop() {
	// 触发关闭，若已关闭则返回
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}

	// Block until the stop has been acknowledged by run()
	// 等待关闭完成
	<-n.done
}

// run() 方法中，主要处理：
//	获取新产生的 Ready ，并塞入 readyc 通道供上层处理
//	探测 Leader 变化，仅当集群中存在已知 leader 时才会处理提案
//	监听各种通道，处理消息：
//	 - propc：处理 Propose 的请求消息
//	 - confc：处理 ProposeConfChange 的配置变更请求消息
//	 - recvc：处理响应消息
//	 - tickc, advancec：处理上层的 Tick 和 Advance 的信号
//	其他
func (n *node) run() {
	var propc chan msgWithResult
	var readyc chan Ready
	var advancec chan struct{}
	var rd Ready

	r := n.rn.raft

	// 初始状态不知道谁是 leader ，需要通过 Ready 获取
	lead := None

	for {

		// 1. 轮询时，首先将新产生的 Ready 取出来存入 rd 中，同时设置 readyc 管道
		if advancec == nil && n.rn.HasReady() {
			// Populate a Ready. Note that this Ready is not guaranteed to
			// actually be handled. We will arm readyc, but there's no guarantee
			// that we will actually send on it. It's possible that we will
			// service another channel instead, loop around, and then populate
			// the Ready again. We could instead force the previous Ready to be
			// handled first, but it's generally good to emit larger Readys plus
			// it simplifies testing (by emitting less frequently and more
			// predictably).
			//
			// 拿到 raft 层的 Ready 数据
			rd = n.rn.readyWithoutAccept()

			//
			readyc = n.readyc
		}

		// 2. 探测 Leader 变化并打日志，仅当集群中存在
		if lead != r.lead {
			// 知道 leader 是谁后就可以通过 propc 读取数据，否则不处理。
			if r.hasLeader() {
				if lead == None {
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else {
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			// 保存当前 leader
			lead = r.lead
		}

		// 3. 监听通道以进行处理
		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		//
		// 外部对 Node 调用 Propose 时，这里会收到消息并处理（如追加日志、投票等），然后把raft的返回值在返回给调用者
		case pm := <-propc:
			// 解析消息
			m := pm.m
			m.From = r.id
			// 处理消息
			err := r.Step(m)
			// 返回结果
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}
		//
		case m := <-n.recvc: // 这里会收到并处理响应消息
			// 相比于 propc 中的数据处理，多了一些判断
			if IsResponseMsg(m.Type) && !IsLocalMsgTarget(m.From) && r.prs.Progress[m.From] == nil {
				// Filter out response message from unknown From.
				break
			}
			r.Step(m)
		// 配置修改处理
		case cc := <-n.confc: // 外部对 Node 调用 ProposeConfChange 时，这里会收到，处理集群配置变更
			_, okBefore := r.prs.Progress[r.id]
			cs := r.applyConfChange(cc)
			// If the node was removed, block incoming proposals. Note that we
			// only do this if the node was in the config before. Nodes may be
			// a member of the group without knowing this (when they're catching
			// up on the log and don't have the latest config) and we don't want
			// to block the proposal channel in that case.
			//
			// NB: propc is reset when the leader changes, which, if we learn
			// about it, sort of implies that we got readded, maybe? This isn't
			// very sound and likely has bugs.
			if _, okAfter := r.prs.Progress[r.id]; okBefore && !okAfter {
				var found bool
				for _, sl := range [][]uint64{cs.Voters, cs.VotersOutgoing} {
					for _, id := range sl {
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
					propc = nil
				}
			}
			select {
			case n.confstatec <- cs:
			case <-n.done:
			}
		case <-n.tickc:// 外部对 Node 调用 Tick 时，这里会收到，并自增 tick 计数器
			n.rn.Tick()
		// 将新取出来的 Ready 放入 ready 通道中，供上层对 Node 调用 Ready() 获取并处理。
		case readyc <- rd:
			// 告诉 raft ，ready 数据已被接收
			n.rn.acceptReady(rd)
			if !n.rn.asyncStorageWrites {
				advancec = n.advancec // 赋值 Advance channel ，等待 Ready 处理完成的消息
			} else {
				rd = Ready{}
			}
			readyc = nil
		// 使用者处理完 Ready 数据后，调用了 Advance() ，这里会收到，并对 raft 调用 Advance 表示处理外之前弹出的 Ready
		case <-advancec:
			n.rn.Advance(rd)
			rd = Ready{}
			advancec = nil
		// 获取状态
		case c := <-n.status:
			c <- getStatus(r)
		// 停止 Raft 节点
		case <-n.stop:
			close(n.done)
			return
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
//
// 定时器触发一次
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.rn.raft.logger.Warningf("%x A tick missed to fire. Node blocks too long!", n.rn.raft.id)
	}
}

func (n *node) Campaign(ctx context.Context) error { return n.step(ctx, pb.Message{Type: pb.MsgHup}) }

// Propose 发起新的提案
func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.stepWait(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (n *node) Step(ctx context.Context, m pb.Message) error {
	// Ignore unexpected local messages receiving over network.
	if IsLocalMsg(m.Type) && !IsLocalMsgTarget(m.From) {
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

func confChangeToMsg(c pb.ConfChangeI) (pb.Message, error) {
	typ, data, err := pb.MarshalConfChange(c)
	if err != nil {
		return pb.Message{}, err
	}
	return pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: typ, Data: data}}}, nil
}

func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error {
	msg, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return n.Step(ctx, msg)
}

func (n *node) step(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, false)
}

func (n *node) stepWait(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, true)
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (n *node) stepWithWaitOption(ctx context.Context, m pb.Message, wait bool) error {
	// 如果消息非 Prop 类型，则写入到 n.recvc 管道，否则写入到 n.propc 管道
	if m.Type != pb.MsgProp {
		select {
		case n.recvc <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}

	// 提案管道
	ch := n.propc

	// 封装消息
	pm := msgWithResult{m: m}
	// 等待响应
	if wait {
		pm.result = make(chan error, 1)
	}

	// 发送消息
	select {
	case ch <- pm:// 将 Prop 消息写入提案管道
		if !wait { // 若不关心结果，直接返回即可
			return nil
		}
	case <-ctx.Done():// 超时
		return ctx.Err()
	case <-n.done:// 关闭
		return ErrStopped
	}

	// 等待响应
	select {
	case err := <-pm.result:// 处理结果
		if err != nil {
			return err
		}
	case <-ctx.Done():	// 超时
		return ctx.Err()
	case <-n.done:		// 关闭
		return ErrStopped
	}

	return nil
}

func (n *node) Ready() <-chan Ready { return n.readyc }

func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	var cs pb.ConfState
	select {
	case n.confc <- cc.AsV2():
	case <-n.done:
	}
	select {
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

func (n *node) Status() Status {
	c := make(chan Status)
	select {
	case n.status <- c:
		return <-c
	case <-n.done:
		return Status{}
	}
}

func (n *node) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-n.done:
	}
}

func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	select {
	case n.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	// manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
	case n.recvc <- pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead}:
	case <-n.done:
	case <-ctx.Done():
	}
}

func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}
