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

import pb "go.etcd.io/raft/v3/raftpb"

// unstable contains "unstable" log entries and snapshot state that has
// not yet been written to Storage. The type serves two roles. First, it
// holds on to new log entries and an optional snapshot until they are
// handed to a Ready struct for persistence. Second, it continues to
// hold on to this state after it has been handed off to provide raftLog
// with a view of the in-progress log entries and snapshot until their
// writes have been stabilized and are guaranteed to be reflected in
// queries of Storage. After this point, the corresponding log entries
// and/or snapshot can be cleared from unstable.
//
// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
//
// 客户端发来的请求，会先存入 unstable 中（一个数组），然后上层模块会将这些记录保存到其他地方（storage)
// 持久化后会删除 unstable 中的记录，没持久化时不稳定，所以称为 unstable 。
//
// unstable 数据结构用于还没有被用户层持久化的数据，它维护了两部分内容 snapshot 和 entries 。
//
// entries 代表的是要进行操作的日志，但日志不可能无限增长，在特定的情况下，某些过期的日志会被清空。
// 那这就引入一个新问题了，如果此后一个新的 follower 加入，而 leader 只有一部分操作日志，那这个新 follower 不是没法同步了吗？
//
// 所以这个时候 snapshot 就登场了 - 我无法给你之前的日志，但我给你所有之前日志应用后的结果，
// 之后的日志你再以这个 snapshot 为基础进行应用，那我们的状态就可以同步了。
//
// 所以，unstable 的前半部分是快照数据，后半部分是日志条目组成的数组entries；
// 另外， unstable.offset 成员保存的是 entries 数组中的第一条数据在 raft 日志中的索引，
// 即第 i 条 entry 在 raft 日志中的索引为 i + unstable.offset。
//
// 这两个部分，并不同时存在，同一时间只有一个部分存在。
// 其中，快照数据仅当当前节点在接收从 leader 发送过来的快照数据时存在，在接收快照数据的时候，entries 数组中是没有数据的；
// 除了这种情况之外，就只会存在 entries 数组的数据了。
// 因此，当接收完毕快照数据进入正常的接收日志流程时，快照数据将被置空。
//
// 方法  描述
//	maybeFirstIndex() (uint64, bool)	获取相对整个 raftLog 的 first index，当 unstable 无法得知该值时，第二个返回值返回 false 。
//	maybeLastIndex() (uint64, bool)		获取相对整个 raftLog 的 last index，当 unstable 无法得知该值时，第二个返回值返回 false 。
//	maybeTerm(i uint64) (uint64, bool)	获取给定 index 的日志条目的 term ，当 unstable 无法得知该值时，第二个返回值返回 false 。
//	stableTo(i, t uint64)				通知 unstable 当前 index 为 i、term 为 t 及其之前的日志已经被保存到了稳定存储中，可以裁剪掉 unstable 中的这段日志了。裁剪后会根据空间利用率适当地对空间进行优化。
//	stableSnapTo(i uint64)				通知 unstable 当前 index 在 i 及其之前的快照已经保存到了稳定存储中，如果 unstable 中保存了该快照，那么可以释放该快照了。
//	restore(s pb.Snapshot)				根据快照恢复 unstable 的状态（设置 unstable 中的 offset、snapshot，并将 entries 置空）。
//	truncateAndAppend(ents []pb.Entry)	对给定的日志切片进行裁剪，并将其加入到 unstable 保存的日志中。
//	slice(lo uint64, hi uint64)			返回给定范围内的日志切片。首先会通过 mustCheckOutOfBounds(lo, hi uint64) 方法检查是否越界，如果越界会因此 panic 。
//
type unstable struct {
	// the incoming unstable snapshot, if any.
	// 快照数据，是 follower 从 leader 收到的最新的 Snapshot 。
	// 该快照数据也是未写入 Storage 中的 Entry 记录
	snapshot *pb.Snapshot

	// all entries that have not yet been written to storage.
	// 所有未被保存到 Storage 中的 Entry 记录
	//
	entries []pb.Entry

	// entries[i] has raft log position i+offset.
	// entries 中第一条记录的索引值
	//
	// entries[0].Index == offset + 0
	// entries[1].Index == offset + 1
	// ...
	offset uint64

	// if true, snapshot is being written to storage.
	snapshotInProgress bool
	// entries[:offsetInProgress-offset] are being written to storage.
	// Like offset, offsetInProgress is exclusive, meaning that it
	// contains the index following the largest in-progress entry.
	// Invariant: offset <= offsetInProgress
	offsetInProgress uint64

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
//
// 返回 unstable 数据的第一条数据索引。
// 因为只有快照数据在最前面，因此这个函数只有当快照数据存在的时候才能拿到第一条数据索引，其他的情况下已经拿不到了。
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
//
// 返回最后一条数据的索引。
// 因为是 entries 数据在后，而快照数据在前，所以取最后一条数据索引是从 entries 开始查，查不到的情况下才查快照数据。
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
//
// 这个函数根据传入的日志数据索引，得到这个日志对应的任期号。
// 前面已经提过，unstable.offset 是快照数据和 entries 数组的分界线，
// 因此在这个函数中，会区分传入的参数与 offset 的大小关系，小于 offset 的情况下在快照数据中查询，否则就在 entries 数组中查询了。
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	return u.entries[i-u.offset].Term, true
}

// nextEntries returns the unstable entries that are not already in the process
// of being written to storage.
func (u *unstable) nextEntries() []pb.Entry {
	inProgress := int(u.offsetInProgress - u.offset)
	if len(u.entries) == inProgress {
		return nil
	}
	return u.entries[inProgress:]
}

// nextSnapshot returns the unstable snapshot, if one exists that is not already
// in the process of being written to storage.
func (u *unstable) nextSnapshot() *pb.Snapshot {
	if u.snapshot == nil || u.snapshotInProgress {
		return nil
	}
	return u.snapshot
}

// acceptInProgress marks all entries and the snapshot, if any, in the unstable
// as having begun the process of being written to storage. The entries/snapshot
// will no longer be returned from nextEntries/nextSnapshot. However, new
// entries/snapshots added after a call to acceptInProgress will be returned
// from those methods, until the next call to acceptInProgress.
func (u *unstable) acceptInProgress() {
	if len(u.entries) > 0 {
		// NOTE: +1 because offsetInProgress is exclusive, like offset.
		u.offsetInProgress = u.entries[len(u.entries)-1].Index + 1
	}
	if u.snapshot != nil {
		u.snapshotInProgress = true
	}
}

// stableTo marks entries up to the entry with the specified (index, term) as
// being successfully written to stable storage.
//
// The method should only be called when the caller can attest that the entries
// can not be overwritten by an in-progress log append. See the related comment
// in newStorageAppendRespMsg.
//
// 该函数传入一个索引号 i 和任期号 t ，表示应用层已经将这个索引之前的数据进行持久化了，
// 此时 unstable 要做的事情就是在自己的数据中查询，只有在满足任期号相同以及 i 大于等于 offset 的情况下，
// 可以将 entries 中的数据进行缩容，将 i 之前的数据删除。
func (u *unstable) stableTo(i, t uint64) {

	// 尝试获取索引 i 的任期，如果不存在，直接返回
	gt, ok := u.maybeTerm(i)
	if !ok {
		// Unstable entry missing. Ignore.
		u.logger.Infof("entry at index %d missing from unstable log; ignoring", i)
		return
	}

	// 如果索引 i < offset, 则其已经持久化
	if i < u.offset {
		// Index matched unstable snapshot, not unstable entry. Ignore.
		u.logger.Infof("entry at index %d matched unstable snapshot; ignoring", i)
		return
	}

	if gt != t {
		// Term mismatch between unstable entry and specified entry. Ignore.
		// This is possible if part or all of the unstable log was replaced
		// between that time that a set of entries started to be written to
		// stable storage and when they finished.
		u.logger.Infof("entry at (index,term)=(%d,%d) mismatched with "+
			"entry at (%d,%d) in unstable log; ignoring", i, t, i, gt)
		return
	}


	num := int(i + 1 - u.offset)
	u.entries = u.entries[num:] // 缩减数组
	u.offset = i + 1			// 更新已 stable 的最大 index
	u.offsetInProgress = max(u.offsetInProgress, u.offset)
	u.shrinkEntriesArray()
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
		u.snapshotInProgress = false
	}
}

func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1	// 重置
	u.offsetInProgress = u.offset	// 重置
	u.entries = nil					// 重置
	u.snapshot = &s					// 保存快照
	u.snapshotInProgress = false	// 重置
}

//

func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	// 获取第一条待追加的 Entry 记录的索引值
	fromIndex := ents[0].Index

	// 在 u.entries 中的日志条目的索引值是基于 offset 的
	// 	entries[0].Index  == offset + 0
	// 	entries[1].Index  == offset + 1
	// 	...
	//  entries[-1].Index == offset+uint64(len(u.entries)-1)
	//
	// 所以，下一个待写入的索引值为 u.offset+uint64(len(u.entries)) ，
	// 如果
	//	(1) fromIndex == u.offset+uint64(len(u.entries)) ，意味着索引值正好衔接，可以直接合并。
	//  (2) fromIndex <= offset ，意味着 ???
	//  (3) formIndex > offset ，意味着部分重叠
	switch {
	case fromIndex == u.offset+uint64(len(u.entries)):
		// fromIndex is the next index in the u.entries, so append directly.
		// 如果待追加的记录与 u.entries 中的记录正好连续，则可以直接向 u.entries 中追加
		u.entries = append(u.entries, ents...)
	case fromIndex <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", fromIndex)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries.
		//
		// 用待追加的 Entry 记录替换当前的 entries 字段，并更新 offset
		u.entries = ents
		u.offset = fromIndex
		u.offsetInProgress = u.offset
	default:
		// Truncate to fromIndex (exclusive), and append the new entries.
		//
		// after 在 offset～last 之间，将 offset~after 之间的记录保留，抛弃 after 之后的记录

		u.logger.Infof("truncate the unstable entries before index %d", fromIndex)
		keep := u.slice(u.offset, fromIndex) // NB: appending to this slice is safe,
		u.entries = append(keep, ents...)    // and will reallocate/copy it
		// Only in-progress entries before fromIndex are still considered to be
		// in-progress.
		u.offsetInProgress = min(u.offsetInProgress, fromIndex)
	}
}

// slice returns the entries from the unstable log with indexes in the range
// [lo, hi). The entire range must be stored in the unstable log or the method
// will panic. The returned slice can be appended to, but the entries in it must
// not be changed because they are still shared with unstable.
//
// TODO(pavelkalinnikov): this, and similar []pb.Entry slices, may bubble up all
// the way to the application code through Ready struct. Protect other slices
// similarly, and document how the client can use them.
func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	// 判断 lo,hi 是否超过 u.entries 范围
	u.mustCheckOutOfBounds(lo, hi)
	// NB: use the full slice expression to limit what the caller can do with the
	// returned slice. For example, an append will reallocate and copy this slice
	// instead of corrupting the neighbouring u.entries.
	return u.entries[lo-u.offset : hi-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
