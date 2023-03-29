// Copyright 2019 The etcd Authors
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

package tracker

// StateType is the state of a tracked follower.
type StateType uint64

const (
	// StateProbe indicates a follower whose last index isn't known. Such a
	// follower is "probed" (i.e. an append sent periodically) to narrow down
	// its last index. In the ideal (and common) case, only one round of probing
	// is necessary as the follower will react with a hint. Followers that are
	// probed over extended periods of time are often offline.
	//
	// 探测状态，当 follower 拒绝了最近的 append 消息时，那么就会进入探测状态；
	// 此时 leader 会试图继续往前追溯该 follower 的日志从哪里开始丢失的。
	//
	// 在 probe 状态时，leader 每次最多 append 一条日志，如果收到的回应中带有 RejectHint 信息，则回退 Next 索引，以便下次重试。
	//
	// 初始时，leader 会把所有 follower 的状态设为 probe ，因为它并不知道各个 follower 的同步状态，所以需要慢慢试探。
	StateProbe StateType = iota

	// StateReplicate is the state steady in which a follower eagerly receives
	// log entries to append to its log.
	//
	// 当 leader 确认某个 follower 的同步状态后，它就会把这个 follower 的 state 切换到这个状态，并且用 pipeline 的方式快速复制日志。
	// 在 leader 发送复制消息之后，就修改该节点的 Next 索引为发送消息的最大索引 +1 。
	StateReplicate

	// StateSnapshot indicates a follower that needs log entries not available
	// from the leader's Raft log. Such a follower needs a full snapshot to
	// return to StateReplicate.
	//
	// 接收快照状态。
	// 当 leader 向某个 follower 发送 append 消息，试图让该 follower 状态跟上 leader 时， 发现此时 leader 上保存的索引数据已经对不上了，
	// 比如 leader 在 index 为 10 之前的数据都已经写入快照中了，但是该 follower 需要的是10之前的数据，此时就会切换到该状态下，发送快照给该 follower 。
	// 当快照数据同步追上之后，并不是直接切换到 Replicate 状态，而是首先切换到 Probe 状态。
	StateSnapshot
)

var prstmap = [...]string{
	"StateProbe",
	"StateReplicate",
	"StateSnapshot",
}

func (st StateType) String() string { return prstmap[st] }
