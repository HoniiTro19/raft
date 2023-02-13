// Copyright 2023 Huidong Zhang, OceanBase, AntGroup
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
	"go.uber.org/zap"
)

// RawNodeElect is a thread-unsafe NodeElect.
// The methods of this struct correspond to the methods of NodeElect and are described
// more fully there.
type RawNodeElect struct {
	election           *election
	asyncStorageWrites bool

	// Mutable fields.
	prevSoftSt     *SoftState
	prevHardSt     pb.HardState
	stepsOnAdvance []pb.Message
}

// NewRawNodeElect instantiates a RawNodeElect from the given configuration.
//
// See Bootstrap() for bootstrapping an initial state; this replaces the former
// 'peers' argument to this method (with identical behavior). However, It is
// recommended that instead of calling Bootstrap, applications bootstrap their
// state manually by setting up a Storage that has a first index > 1 and which
// stores the desired ConfState as its InitialState.
func NewRawNodeElect(config *Config, logger *zap.Logger) (*RawNodeElect, error) {
	e := newElection(config, logger)
	rne := &RawNodeElect{
		election: e,
	}
	rne.asyncStorageWrites = config.AsyncStorageWrites
	ss := e.softState()
	rne.prevSoftSt = &ss
	rne.prevHardSt = e.hardState()
	return rne, nil
}

// Tick advances the internal logical clock by a single tick.
func (rne *RawNodeElect) Tick() {
	rne.election.tick()
}

// Campaign causes this RawNodeElect to transition to candidate state.
func (rne *RawNodeElect) Campaign() error {
	return rne.election.Step(pb.Message{
		Type: pb.MsgHup,
	})
}

// ApplyConfChange applies a config change to the local node. The app must call
// this when it applies a configuration change, except when it decides to reject
// the configuration change, in which case no call must take place.
func (rne *RawNodeElect) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	cs := rne.election.applyConfChange(cc.AsV2())
	return &cs
}

// Step advances the state machine using the given message.
func (rne *RawNodeElect) Step(m pb.Message) error {
	// Ignore unexpected local messages receiving over network.
	if IsLocalMsg(m.Type) && !IsLocalMsgTarget(m.From) {
		return ErrStepLocalMsg
	}
	if IsResponseMsg(m.Type) && !IsLocalMsgTarget(m.From) && rne.election.prs.Progress[m.From] == nil {
		return ErrStepPeerNotFound
	}
	return rne.election.Step(m)
}

// readyWithoutAccept returns a Ready. This is a read-only operation, i.e. there
// is no obligation that the Ready must be handled.
func (rne *RawNodeElect) readyWithoutAccept() Ready {
	e := rne.election

	rd := Ready{
		Entries:          e.raftLog.nextUnstableEnts(),
		CommittedEntries: e.raftLog.nextCommittedEnts(rne.applyUnstableEntries()),
		Messages:         e.msgs,
	}
	if softSt := e.softState(); !softSt.equal(rne.prevSoftSt) {
		// Allocate only when SoftState changes.
		escapingSoftSt := softSt
		rd.SoftState = &escapingSoftSt
	}
	if hardSt := e.hardState(); !isHardStateEqual(hardSt, rne.prevHardSt) {
		rd.HardState = hardSt
	}
	if e.raftLog.hasNextUnstableSnapshot() {
		// Snapshot should not be generated in our election experiments
		// since raft log will not be compacted and entries can always be found
		// to send from leader to followers
		rd.Snapshot = *e.raftLog.nextUnstableSnapshot()
	}
	rd.MustSync = MustSync(e.hardState(), rne.prevHardSt, len(rd.Entries))

	if rne.asyncStorageWrites {
		// If async storage writes are enabled, enqueue messages to
		// local storage threads, where applicable.
		if e.needStorageAppendMsg(rd) {
			m := e.newStorageAppendMsg(rd)
			rd.Messages = append(rd.Messages, m)
		}
		if needStorageApplyMsg(rd) {
			m := e.newStorageApplyMsg(rd)
			rd.Messages = append(rd.Messages, m)
		}
	} else {
		// If async storage writes are disabled, immediately enqueue
		// msgsAfterAppend to be sent out. The Ready struct contract
		// mandates that Messages cannot be sent until after Entries
		// are written to stable storage.
		for _, m := range e.msgsAfterAppend {
			if m.To != e.id {
				rd.Messages = append(rd.Messages, m)
			}
		}
	}

	return rd
}

// needStorageAppendMsg here is wrapped as election method,
// whose content is the same as needStorageAppendMsg in rawnode.
func (e *election) needStorageAppendMsg(rd Ready) bool {
	// Return true if log entries, hard state, or a snapshot need to be written
	// to stable storage. Also return true if any messages are contingent on all
	// prior MsgStorageAppend being processed.
	return len(rd.Entries) > 0 ||
		!IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) ||
		len(e.msgsAfterAppend) > 0
}

// needStorageAppendRespMsg here is wrapped as election method,
// whose content is the same as needStorageAppendRespMsg in rawnode.
func (e *election) needStorageAppendRespMsg(rd Ready) bool {
	// Return true if raft needs to hear about stabilized entries or an applied
	// snapshot. See the comment in newStorageAppendRespMsg, which explains why
	// we check hasNextOrInProgressUnstableEnts instead of len(rd.Entries) > 0.
	return e.raftLog.hasNextOrInProgressUnstableEnts() ||
		!IsEmptySnap(rd.Snapshot)
}

// newStorageAppendMsg creates the message that should be sent to the local
// append thread to instruct it to append log entries, write an updated hard
// state, and apply a snapshot. The message also carries a set of responses
// that should be delivered after the rest of the message is processed. Used
// with AsyncStorageWrites.
// newStorageAppendMsg here is wrapped as election method,
// whose content is the same as newStorageAppendMsg in rawnode.
func (e *election) newStorageAppendMsg(rd Ready) pb.Message {
	m := pb.Message{
		Type:    pb.MsgStorageAppend,
		To:      LocalAppendThread,
		From:    e.id,
		Entries: rd.Entries,
	}
	if !IsEmptyHardState(rd.HardState) {
		// If the Ready includes a HardState update, assign each of its fields
		// to the corresponding fields in the Message. This allows clients to
		// reconstruct the HardState and save it to stable storage.
		//
		// If the Ready does not include a HardState update, make sure to not
		// assign a value to any of the fields so that a HardState reconstructed
		// from them will be empty (return true from raft.IsEmptyHardState).
		m.Term = rd.Term
		m.Vote = rd.Vote
		m.Commit = rd.Commit
	}
	if !IsEmptySnap(rd.Snapshot) {
		snap := rd.Snapshot
		m.Snapshot = &snap
	}
	// Attach all messages in msgsAfterAppend as responses to be delivered after
	// the message is processed, along with a self-directed MsgStorageAppendResp
	// to acknowledge the entry stability.
	//
	// NB: it is important for performance that MsgStorageAppendResp message be
	// handled after self-directed MsgAppResp messages on the leader (which will
	// be contained in msgsAfterAppend). This ordering allows the MsgAppResp
	// handling to use a fast-path in r.raftLog.term() before the newly appended
	// entries are removed from the unstable log.
	m.Responses = e.msgsAfterAppend
	if e.needStorageAppendRespMsg(rd) {
		m.Responses = append(m.Responses, e.newStorageAppendRespMsg(rd))
	}
	return m
}

// newStorageAppendRespMsg creates the message that should be returned to node
// after the unstable log entries, hard state, and snapshot in the current Ready
// (along with those in all prior Ready structs) have been saved to stable
// storage.
// newStorageAppendRespMsg here is wrapped as election method,
// whose content is the same as newStorageAppendRespMsg in rawnode.
func (e *election) newStorageAppendRespMsg(rd Ready) pb.Message {
	m := pb.Message{
		Type: pb.MsgStorageAppendResp,
		To:   e.id,
		From: LocalAppendThread,
		// Dropped after term change, see below.
		Term: e.Term,
	}
	if e.raftLog.hasNextOrInProgressUnstableEnts() {
		// If the raft log has unstable entries, attach the last index and term of the
		// append to the response message. This (index, term) tuple will be handed back
		// and consulted when the stability of those log entries is signaled to the
		// unstable. If the (index, term) match the unstable log by the time the
		// response is received (unstable.stableTo), the unstable log can be truncated.
		//
		// However, with just this logic, there would be an ABA problem[^1] that could
		// lead to the unstable log and the stable log getting out of sync temporarily
		// and leading to an inconsistent view. Consider the following example with 5
		// nodes, A B C D E:
		//
		//  1. A is the leader.
		//  2. A proposes some log entries but only B receives these entries.
		//  3. B gets the Ready and the entries are appended asynchronously.
		//  4. A crashes and C becomes leader after getting a vote from D and E.
		//  5. C proposes some log entries and B receives these entries, overwriting the
		//     previous unstable log entries that are in the process of being appended.
		//     The entries have a larger term than the previous entries but the same
		//     indexes. It begins appending these new entries asynchronously.
		//  6. C crashes and A restarts and becomes leader again after getting the vote
		//     from D and E.
		//  7. B receives the entries from A which are the same as the ones from step 2,
		//     overwriting the previous unstable log entries that are in the process of
		//     being appended from step 5. The entries have the original terms and
		//     indexes from step 2. Recall that log entries retain their original term
		//     numbers when a leader replicates entries from previous terms. It begins
		//     appending these new entries asynchronously.
		//  8. The asynchronous log appends from the first Ready complete and stableTo
		//     is called.
		//  9. However, the log entries from the second Ready are still in the
		//     asynchronous append pipeline and will overwrite (in stable storage) the
		//     entries from the first Ready at some future point. We can't truncate the
		//     unstable log yet or a future read from Storage might see the entries from
		//     step 5 before they have been replaced by the entries from step 7.
		//     Instead, we must wait until we are sure that the entries are stable and
		//     that no in-progress appends might overwrite them before removing entries
		//     from the unstable log.
		//
		// To prevent these kinds of problems, we also attach the current term to the
		// MsgStorageAppendResp (above). If the term has changed by the time the
		// MsgStorageAppendResp if returned, the response is ignored and the unstable
		// log is not truncated. The unstable log is only truncated when the term has
		// remained unchanged from the time that the MsgStorageAppend was sent to the
		// time that the MsgStorageAppendResp is received, indicating that no-one else
		// is in the process of truncating the stable log.
		//
		// However, this replaces a correctness problem with a liveness problem. If we
		// only attempted to truncate the unstable log when appending new entries but
		// also occasionally dropped these responses, then quiescence of new log entries
		// could lead to the unstable log never being truncated.
		//
		// To combat this, we attempt to truncate the log on all MsgStorageAppendResp
		// messages where the unstable log is not empty, not just those associated with
		// entry appends. This includes MsgStorageAppendResp messages associated with an
		// updated HardState, which occur after a term change.
		//
		// In other words, we set Index and LogTerm in a block that looks like:
		//
		//  if r.raftLog.hasNextOrInProgressUnstableEnts() { ... }
		//
		// not like:
		//
		//  if len(rd.Entries) > 0 { ... }
		//
		// To do so, we attach r.raftLog.lastIndex() and r.raftLog.lastTerm(), not the
		// (index, term) of the last entry in rd.Entries. If rd.Entries is not empty,
		// these will be the same. However, if rd.Entries is empty, we still want to
		// attest that this (index, term) is correct at the current term, in case the
		// MsgStorageAppend that contained the last entry in the unstable slice carried
		// an earlier term and was dropped.
		//
		// A MsgStorageAppend with a new term is emitted on each term change. This is
		// the same condition that causes MsgStorageAppendResp messages with earlier
		// terms to be ignored. As a result, we are guaranteed that, assuming a bounded
		// number of term changes, there will eventually be a MsgStorageAppendResp
		// message that is not ignored. This means that entries in the unstable log
		// which have been appended to stable storage will eventually be truncated and
		// dropped from memory.
		//
		// [^1]: https://en.wikipedia.org/wiki/ABA_problem
		m.Index = e.raftLog.lastIndex()
		m.LogTerm = e.raftLog.lastTerm()
	}
	if !IsEmptySnap(rd.Snapshot) {
		snap := rd.Snapshot
		m.Snapshot = &snap
	}
	return m
}

// newStorageApplyMsg creates the message that should be sent to the local
// apply thread to instruct it to apply committed log entries. The message
// also carries a response that should be delivered after the rest of the
// message is processed. Used with AsyncStorageWrites.
// newStorageApplyMsg here is wrapped as election method,
// whose content is the same as newStorageApplyMsg in rawnode.
func (e *election) newStorageApplyMsg(rd Ready) pb.Message {
	ents := rd.CommittedEntries
	return pb.Message{
		Type:    pb.MsgStorageApply,
		To:      LocalApplyThread,
		From:    e.id,
		Term:    0, // committed entries don't apply under a specific term
		Entries: ents,
		Responses: []pb.Message{
			e.newStorageApplyRespMsg(ents),
		},
	}
}

// newStorageApplyRespMsg creates the message that should be returned to node
// after the committed entries in the current Ready (along with those in all
// prior Ready structs) have been applied to the local state machine.
// newStorageApplyRespMsg here is wrapped as election method,
// whose content is the same as newStorageApplyRespMsg in rawnode.
func (e *election) newStorageApplyRespMsg(ents []pb.Entry) pb.Message {
	return pb.Message{
		Type:    pb.MsgStorageApplyResp,
		To:      e.id,
		From:    LocalApplyThread,
		Term:    0, // committed entries don't apply under a specific term
		Entries: ents,
	}
}

// acceptReady is called when the consumer of the RawNodeElect has decided to go
// ahead and handle a Ready. Nothing must alter the state of the RawNodeElect between
// this call and the prior call to Ready().
func (rne *RawNodeElect) acceptReady(rd Ready) {
	e := rne.election
	if rd.SoftState != nil {
		rne.prevSoftSt = rd.SoftState
	}
	if !IsEmptyHardState(rd.HardState) {
		rne.prevHardSt = rd.HardState
	}
	if !rne.asyncStorageWrites {
		if len(rne.stepsOnAdvance) != 0 {
			e.logger.Panic("two accepted Ready structs without call to Advance", zap.Uint64("member", e.id))
		}
		for _, m := range e.msgsAfterAppend {
			if m.To == e.id {
				rne.stepsOnAdvance = append(rne.stepsOnAdvance, m)
			}
		}
		if e.needStorageAppendRespMsg(rd) {
			m := e.newStorageAppendRespMsg(rd)
			rne.stepsOnAdvance = append(rne.stepsOnAdvance, m)
		}
		if needStorageApplyRespMsg(rd) {
			m := e.newStorageApplyRespMsg(rd.CommittedEntries)
			rne.stepsOnAdvance = append(rne.stepsOnAdvance, m)
		}
	}
	e.msgs = nil
	e.msgsAfterAppend = nil
	e.raftLog.acceptUnstable()
	if len(rd.CommittedEntries) > 0 {
		ents := rd.CommittedEntries
		index := ents[len(ents)-1].Index
		e.raftLog.acceptApplying(index, entsSize(ents), rne.applyUnstableEntries())
	}
}

// applyUnstableEntries returns whether entries are allowed to be applied once
// they are known to be committed but before they have been written locally to
// stable storage.
func (rne *RawNodeElect) applyUnstableEntries() bool {
	return !rne.asyncStorageWrites
}

// HasReady called when RawNodeElect user need to check if any Ready pending.
func (rne *RawNodeElect) HasReady() bool {
	// TODO(nvanbenschoten): order these cases in terms of cost and frequency.
	e := rne.election
	if softSt := e.softState(); !softSt.equal(rne.prevSoftSt) {
		return true
	}
	if hardSt := e.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rne.prevHardSt) {
		return true
	}
	if e.raftLog.hasNextUnstableSnapshot() {
		return true
	}
	if len(e.msgs) > 0 || len(e.msgsAfterAppend) > 0 {
		return true
	}
	if e.raftLog.hasNextUnstableEnts() || e.raftLog.hasNextCommittedEnts(rne.applyUnstableEntries()) {
		return true
	}
	return false
}

// Advance notifies the RawNodeElect that the application has applied and saved progress in the
// last Ready results.
//
// NOTE: Advance must not be called when using AsyncStorageWrites. Response messages from
// the local append and apply threads take its place.
func (rne *RawNodeElect) Advance(_ Ready) {
	// The actions performed by this function are encoded into stepsOnAdvance in
	// acceptReady. In earlier versions of this library, they were computed from
	// the provided Ready struct. Retain the unused parameter for compatibility.
	e := rne.election
	if rne.asyncStorageWrites {
		e.logger.Panic("Advance must not be called when using AsyncStorageWrites", zap.Uint64("member", e.id))
	}
	for i, m := range rne.stepsOnAdvance {
		_ = e.Step(m)
		rne.stepsOnAdvance[i] = pb.Message{}
	}
	rne.stepsOnAdvance = rne.stepsOnAdvance[:0]
}

func (rne *RawNodeElect) Bootstrap(peers []Peer) error {
	if len(peers) == 0 {
		return errors.New("must provide at least one peer to Bootstrap")
	}
	e := rne.election
	lastIndex, err := e.raftLog.storage.LastIndex()
	if err != nil {
		return err
	}

	if lastIndex != 0 {
		return errors.New("can't bootstrap a nonempty Storage")
	}

	// We've faked out initial entries above, but nothing has been
	// persisted. Start with an empty HardState (thus the first Ready will
	// emit a HardState update for the app to persist).
	rne.prevHardSt = emptyState

	// TODO(tbg): remove StartNode and give the application the right tools to
	// bootstrap the initial membership in a cleaner way.
	e.becomeFollower(1, None)
	ents := make([]pb.Entry, len(peers))
	for i, peer := range peers {
		cc := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: peer.Context}
		data, err := cc.Marshal()
		if err != nil {
			return err
		}

		ents[i] = pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: uint64(i + 1), Data: data}
	}
	e.raftLog.append(ents...)

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
	e.raftLog.committed = uint64(len(ents))
	for _, peer := range peers {
		e.applyConfChange(pb.ConfChange{NodeID: peer.ID, Type: pb.ConfChangeAddNode}.AsV2())
	}
	return nil
}
