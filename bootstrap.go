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
	"errors"

	pb "go.etcd.io/raft/v3/raftpb"
)

// 在Raft集群中，引导操作（Bootstrap）是在集群启动时执行的一系列步骤，以确保节点能够正确地进行选举、复制日志和达成共识。
// 引导操作是为了建立一个稳定的集群状态，使所有节点能够相互通信和协作。
//
// 引导操作的主要目标是选择一个初始的领导者（Leader）节点，并确定集群中的各个角色（如投票者、非投票者等）。
// 这样一来，集群就能够开始进行日志复制和共识算法的运行。
//
// 引导操作的步骤通常如下：
//	- 选择一个初始的领导者节点：在初始阶段，可以通过配置文件或手动指定一个节点作为初始领导者。该节点将负责接收客户端请求并开始复制日志给其他节点。
//	- 配置集群成员：在引导过程中，需要确定集群中的成员节点，即投票者和非投票者。投票者节点参与选举和日志复制，而非投票者节点只参与日志复制。
//	- 进行选举：在引导过程中，节点会进行选举，以选择一个领导者节点。选举使用 Raft 协议中定义的选举算法，节点会相互投票，并根据规则选择出新的领导者。
//	- 同步日志：一旦选出领导者，它将负责接收客户端请求，并复制日志给其他节点。通过日志复制，集群中的所有节点都会达成一致的状态。
//
// 需要进行引导操作的原因是确保在集群启动时，所有节点都能达到一个一致的状态。
// 通过引导操作，可以初始化领导者和集群角色，并确保所有节点都能够正常工作。
//
// 具体的引导操作可以根据Raft库或框架的实现而有所不同，但通常涉及节点配置、选举过程和日志复制等步骤。
// 每个节点都会参与引导操作，并根据指定的规则和算法相互交互，以达到共识并建立稳定的集群状态。

// Bootstrap initializes the RawNode for first use by appending configuration
// changes for the supplied peers. This method returns an error if the Storage
// is nonempty.
//
// It is recommended that instead of calling this method, applications bootstrap
// their state manually by setting up a Storage that has a first index > 1 and
// which stores the desired ConfState as its InitialState.
func (rn *RawNode) Bootstrap(peers []Peer) error {

	if len(peers) == 0 {
		return errors.New("must provide at least one peer to Bootstrap")
	}

	lastIndex, err := rn.raft.raftLog.storage.LastIndex()
	if err != nil {
		return err
	}

	if lastIndex != 0 {
		return errors.New("can't bootstrap a nonempty Storage")
	}

	// We've faked out initial entries above, but nothing has been
	// persisted. Start with an empty HardState (thus the first Ready will
	// emit a HardState update for the app to persist).
	rn.prevHardSt = emptyState

	// TODO(tbg): remove StartNode and give the application the right tools to
	// bootstrap the initial membership in a cleaner way.
	rn.raft.becomeFollower(1, None)
	ents := make([]pb.Entry, len(peers))
	for i, peer := range peers {
		cc := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: peer.Context}
		data, err := cc.Marshal()
		if err != nil {
			return err
		}

		ents[i] = pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: uint64(i + 1), Data: data}
	}
	rn.raft.raftLog.append(ents...)

	// Now apply them, mainly so that the application can call Campaign
	// immediately after StartNode in tests. Note that these nodes will
	// be added to raft twice: here and when the application's Ready
	// loop calls ApplyConfChange. The calls to addNode must come after
	// all calls to raftLog.append so progress.next is set after these
	// bootstrapping entries (it is an error if we try to append these
	// entries since they have already been committed).
	// We do not set raftLog.applied so the application will be able
	// to observe all conf changes via Ready.CommittedEntries.
	//
	// TODO(bdarnell): These entries are still unstable; do we need to preserve
	// the invariant that committed < unstable?
	rn.raft.raftLog.committed = uint64(len(ents))
	for _, peer := range peers {
		rn.raft.applyConfChange(pb.ConfChange{NodeID: peer.ID, Type: pb.ConfChangeAddNode}.AsV2())
	}
	return nil
}
