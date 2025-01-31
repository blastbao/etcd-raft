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

// inflight describes an in-flight MsgApp message.
type inflight struct {
	index uint64 // the index of the last entry inside the message
	bytes uint64 // the total byte size of the entries in the message
}

// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
type Inflights struct {
	// the starting index in the buffer
	// 记录第一条 MsgApp 消息的下标
	start int

	// 记录 MsgApp 消息的个数
	count int    // number of inflight messages in the buffer
	bytes uint64 // number of inflight bytes

	// 当前 inflights 实例中记录的 MsgApp 消息个数的上限（最大长度）。
	size     int    // the max number of inflight messages
	maxBytes uint64 // the max total byte size of inflight messages

	// buffer is a ring buffer containing info about all in-flight messages.
	// 用来记录 MsgApp 消息相关信息的数组，其中记录的是 MsgApp 消息中最后一条 Entry 记录的索引值，被当成环形数组使用
	buffer []inflight
}

// NewInflights sets up an Inflights that allows up to size inflight messages,
// with the total byte size up to maxBytes. If maxBytes is 0 then there is no
// byte size limit. The maxBytes limit is soft, i.e. we accept a single message
// that brings it from size < maxBytes to size >= maxBytes.
func NewInflights(size int, maxBytes uint64) *Inflights {
	return &Inflights{
		size:     size,
		maxBytes: maxBytes,
	}
}

// Clone returns an *Inflights that is identical to but shares no memory with
// the receiver.
func (in *Inflights) Clone() *Inflights {
	ins := *in
	ins.buffer = append([]inflight(nil), in.buffer...)
	return &ins
}

// Add notifies the Inflights that a new message with the given index and byte
// size is being dispatched. Full() must be called prior to Add() to verify that
// there is room for one more message, and consecutive calls to Add() must
// provide a monotonic sequence of indexes.
//
// 用来记录发送出去的 MsgApp 消息(一个一个的递增)
func (in *Inflights) Add(index, bytes uint64) {
	// 检测当前 buffer 是否已满
	if in.Full() {
		panic("cannot add into a Full inflights")
	}
	// 获取新增消息的下标
	next := in.start + in.count
	// 环形回绕
	size := in.size
	if next >= size {
		next -= size
	}
	// 进行扩容（2倍）
	if next >= len(in.buffer) {
		in.grow()
	}
	// 在 next 位置记录消息中最后一条 Entry 的索引值
	in.buffer[next] = inflight{index: index, bytes: bytes}
	// 消息数 +1
	in.count++
	// 字节数 +1
	in.bytes += bytes
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
func (in *Inflights) grow() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]inflight, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// FreeLE frees the inflights smaller or equal to the given `to` flight.
//
// 将指定消息及其之前的消息全部清空，释放 inflights 空间，让后面的消息继续发送
func (in *Inflights) FreeLE(to uint64) {
	if in.count == 0 || to < in.buffer[in.start].index {
		// out of the left side of the window
		return
	}

	idx := in.start
	var i int
	var bytes uint64
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx].index { // found the first large inflight
			break
		}
		bytes += in.buffer[idx].bytes

		// increase index and maybe rotate
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i inflights and set new start index
	in.count -= i
	in.bytes -= bytes
	in.start = idx
	if in.count == 0 {
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

// Full returns true if no more messages can be sent at the moment.
func (in *Inflights) Full() bool {
	return in.count == in.size || (in.maxBytes != 0 && in.bytes >= in.maxBytes)
}

// Count returns the number of inflight messages.
func (in *Inflights) Count() int { return in.count }

// reset frees all inflights.
func (in *Inflights) reset() {
	in.count = 0
	in.start = 0
}
