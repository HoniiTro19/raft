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
	"fmt"
	"sort"
	"strings"

	"go.etcd.io/raft/v3/confchange"
	"go.etcd.io/raft/v3/quorum"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"go.uber.org/zap"
)

type election struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	raftLog *raftLog

	maxMsgSize         entryEncodingSize
	maxUncommittedSize entryPayloadSize
	// TODO(tbg): rename to trk.
	prs tracker.ProgressTracker

	state StateType

	// isLearner is true if the local raft node is a learner.
	isLearner bool

	// msgs contains the list of messages that should be sent out immediately to
	// other nodes.
	//
	// Messages in this list must target other nodes.
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
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	pendingConfIndex uint64
	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
	uncommittedSize entryPayloadSize

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	checkQuorum bool
	preVote     bool

	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

	step func(pb.Message) error
	tick func()

	logger *zap.Logger
	config *Config
}

func newElection(c *Config, logger *zap.Logger) *election {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLogWithSize(c.Storage, c.Logger, entryEncodingSize(c.MaxCommittedSizePerReady))
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}

	e := &election{
		id:                 c.ID,
		lead:               None,
		isLearner:          false,
		raftLog:            raftlog,
		maxMsgSize:         entryEncodingSize(c.MaxSizePerMsg),
		maxUncommittedSize: entryPayloadSize(c.MaxUncommittedEntriesSize),
		prs:                tracker.MakeProgressTracker(c.MaxInflightMsgs, c.MaxInflightBytes),
		electionTimeout:    c.ElectionTick,
		heartbeatTimeout:   c.HeartbeatTick,
		checkQuorum:        c.CheckQuorum,
		preVote:            c.PreVote,
		logger:             logger,
		config:             c,
	}

	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   e.prs,
		LastIndex: raftlog.lastIndex(),
	}, cs)
	if err != nil {
		panic(err)
	}
	assertConfStatesEquivalent(c.Logger, cs, e.switchToConfig(cfg, prs))

	if !IsEmptyHardState(hs) {
		e.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied, 0 /* size */)
	}
	e.becomeFollower(e.Term, None)

	var nodesStrs []string
	for _, n := range e.prs.VoterNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}
	e.logger.Info("new raft", zap.Uint64("member", e.id),
		zap.String("peers", strings.Join(nodesStrs, ",")),
		zap.Uint64("term", e.Term),
		zap.Uint64("commit", e.raftLog.committed),
		zap.Uint64("applied", e.raftLog.applied),
		zap.Uint64("logterm", e.raftLog.lastTerm()),
		zap.Uint64("index", e.raftLog.lastIndex()))
	return e
}

func (e *election) softState() SoftState { return SoftState{Lead: e.lead, RaftState: e.state} }

func (e *election) hardState() pb.HardState {
	return pb.HardState{
		Term:   e.Term,
		Vote:   e.Vote,
		Commit: e.raftLog.committed,
	}
}

// send schedules persisting state to a stable storage and AFTER that
// sending the message (as part of next Ready message processing).
func (e *election) send(m pb.Message) {
	if m.From == None {
		m.From = e.id
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
			e.logger.Panic("term should be set in the message", zap.Uint64("member", e.id), zap.String("msg", m.String()))
		}
	} else {
		if m.Term != 0 {
			e.logger.Panic("term should not be set in the message",
				zap.Uint64("member", e.id),
				zap.String("msg", m.String()),
				zap.Uint64("term", m.Term))
		}
		m.Term = e.Term
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
		// > persists new log entries before they are counted towards the entries'
		// > commitment; this prevents committed entries from being lost or
		// > "uncommitted" when servers restart
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
		e.msgsAfterAppend = append(e.msgsAfterAppend, m)
	} else {
		if m.To == e.id {
			e.logger.Panic("message should not be self-addressed", zap.Uint64("member", e.id), zap.String("msg", m.String()))
		}
		e.msgs = append(e.msgs, m)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer.
func (e *election) sendAppend(to uint64) {
	e.maybeSendAppend(to, true)
}

// maybeSendAppend sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The sendIfEmpty
// argument controls whether messages with no entries will be sent
// ("empty" messages are useful to convey updated Commit indexes, but
// are undesirable when we're sending multiple messages in a batch).
func (e *election) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := e.prs.Progress[to]
	if pr.IsPaused() {
		return false
	}

	term, errt := e.raftLog.term(pr.Next - 1)
	var ents []pb.Entry
	var erre error
	// In a throttled StateReplicate only send empty MsgApp, to ensure progress.
	// Otherwise, if we had a full Inflights and all inflight messages were in
	// fact dropped, replication to that follower would stall. Instead, an empty
	// MsgApp will eventually reach the follower (heartbeats responses prompt the
	// leader to send an append), allowing it to be acked or rejected, both of
	// which will clear out Inflights.
	if pr.State != tracker.StateReplicate || !pr.Inflights.Full() {
		ents, erre = e.raftLog.entries(pr.Next, e.maxMsgSize)
	}

	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		if !pr.RecentActive {
			e.logger.Debug("ignore sending snapshot to member not recently active",
				zap.Uint64("member", e.id),
				zap.Uint64("to", to))
			return false
		}

		snapshot, err := e.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				e.logger.Debug("fail to send snapshot because snapshot is temporarily unavailable",
					zap.Uint64("member", e.id),
					zap.Uint64("to", to))
				return false
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		sindex, _ := snapshot.Metadata.Index, snapshot.Metadata.Term
		e.logger.Debug("send snapshot", zap.Uint64("member", e.id),
			zap.Uint64("index", e.raftLog.firstIndex()),
			zap.Uint64("commit", e.raftLog.committed),
			zap.Uint64("to", to),
			zap.String("snap", snapshot.Metadata.String()),
			zap.String("pr", pr.String()))

		pr.BecomeSnapshot(sindex)
		e.logger.Debug("pause sending replication messages", zap.Uint64("member", e.id),
			zap.Uint64("to", to),
			zap.String("pr", pr.String()))
		e.send(pb.Message{To: to, Type: pb.MsgSnap, Snapshot: &snapshot})
		return true
	}

	// Send the actual MsgApp otherwise, and update the progress accordingly.
	next := pr.Next // save Next for later, as the progress update can change it
	if err := pr.UpdateOnEntriesSend(len(ents), uint64(payloadsSize(ents)), next); err != nil {
		e.logger.Panic("error on update progress entries", zap.Uint64("member", e.id), zap.Error(err))
	}
	e.send(pb.Message{
		To:      to,
		Type:    pb.MsgApp,
		Index:   next - 1,
		LogTerm: term,
		Entries: ents,
		Commit:  e.raftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (e *election) sendHeartbeat(to uint64) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(e.prs.Progress[to].Match, e.raftLog.committed)
	m := pb.Message{
		To:     to,
		Type:   pb.MsgHeartbeat,
		Commit: commit,
	}

	e.send(m)
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (e *election) bcastAppend() {
	e.prs.Visit(func(id uint64, _ *tracker.Progress) {
		if id == e.id {
			return
		}
		e.sendAppend(id)
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (e *election) bcastHeartbeat() {
	e.prs.Visit(func(id uint64, _ *tracker.Progress) {
		if id == e.id {
			return
		}
		e.sendHeartbeat(id)
	})
}

func (e *election) appliedTo(index uint64, size entryEncodingSize) {
	oldApplied := e.raftLog.applied
	newApplied := max(index, oldApplied)
	e.raftLog.appliedTo(newApplied, size)

	if e.prs.Config.AutoLeave && newApplied >= e.pendingConfIndex && e.state == StateLeader {
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
		if err := e.Step(m); err != nil {
			e.logger.Debug("not initiate automatic transition out of joint configuration",
				zap.Uint64("member", e.id),
				zap.String("config", e.prs.Config.String()),
				zap.Error(err))
		} else {
			e.logger.Info("initiate automatic transition out of joint configuration",
				zap.Uint64("member", e.id),
				zap.String("config", e.prs.Config.String()))
		}
	}
}

func (e *election) appliedSnap(snap *pb.Snapshot) {
	index := snap.Metadata.Index
	e.raftLog.stableSnapTo(index)
	e.appliedTo(index, 0 /* size */)
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (e *election) maybeCommit() bool {
	mci := e.prs.Committed()
	return e.raftLog.maybeCommit(mci, e.Term)
}

func (e *election) reset(term uint64) {
	if e.Term != term {
		e.Term = term
		e.Vote = None
	}
	e.lead = None

	e.electionElapsed = 0
	e.heartbeatElapsed = 0
	e.resetRandomizedElectionTimeout()

	e.prs.ResetVotes()
	e.prs.Visit(func(id uint64, pr *tracker.Progress) {
		*pr = tracker.Progress{
			Match:     0,
			Next:      e.raftLog.lastIndex() + 1,
			Inflights: tracker.NewInflights(e.prs.MaxInflight, e.prs.MaxInflightBytes),
			IsLearner: pr.IsLearner,
		}
		if id == e.id {
			pr.Match = e.raftLog.lastIndex()
		}
	})

	e.pendingConfIndex = 0
	e.uncommittedSize = 0
}

func (e *election) appendEntry(es ...pb.Entry) (accepted bool) {
	li := e.raftLog.lastIndex()
	for i := range es {
		es[i].Term = e.Term
		es[i].Index = li + 1 + uint64(i)
	}
	// Track the size of this uncommitted proposal.
	if !e.increaseUncommittedSize(es) {
		e.logger.Warn(
			"append new entries to log would exceed uncommitted entry size limit; dropping proposal",
			zap.Uint64("member", e.id),
		)
		// Drop the proposal.
		return false
	}
	// use latest "last" index after truncate/append
	li = e.raftLog.append(es...)
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
	e.send(pb.Message{To: e.id, Type: pb.MsgAppResp, Index: li})
	return true
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (e *election) tickElection() {
	e.electionElapsed++

	if e.promotable() && e.pastElectionTimeout() {
		e.electionElapsed = 0
		if err := e.Step(pb.Message{From: e.id, Type: pb.MsgHup}); err != nil {
			e.logger.Debug("error occurred during election", zap.Uint64("member", e.id), zap.Error(err))
		}
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (e *election) tickHeartbeat() {
	e.heartbeatElapsed++
	e.electionElapsed++

	if e.electionElapsed >= e.electionTimeout {
		e.electionElapsed = 0
		if e.checkQuorum {
			if err := e.Step(pb.Message{From: e.id, Type: pb.MsgCheckQuorum}); err != nil {
				e.logger.Debug("error occurred during checking sending heartbeat", zap.Uint64("member", e.id), zap.Error(err))
			}
		}
	}

	if e.state != StateLeader {
		return
	}

	if e.heartbeatElapsed >= e.heartbeatTimeout {
		e.heartbeatElapsed = 0
		if err := e.Step(pb.Message{From: e.id, Type: pb.MsgBeat}); err != nil {
			e.logger.Debug("error occurred during checking sending heartbeat", zap.Uint64("member", e.id), zap.Error(err))
		}
	}
}

func (e *election) becomeFollower(term uint64, lead uint64) {
	e.step = e.stepFollower
	e.reset(term)
	e.tick = e.tickElection
	e.lead = lead
	e.state = StateFollower
	e.logger.Info("become follower", zap.Uint64("member", e.id), zap.Uint64("term", e.Term))
}

func (e *election) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if e.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	e.step = e.stepCandidate
	e.reset(e.Term + 1)
	e.tick = e.tickElection
	e.Vote = e.id
	e.state = StateCandidate
	e.logger.Info("become candidate", zap.Uint64("member", e.id), zap.Uint64("term", e.Term))
}

func (e *election) becomePreCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if e.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	e.step = e.stepCandidate
	e.prs.ResetVotes()
	e.tick = e.tickElection
	e.lead = None
	e.state = StatePreCandidate
	e.logger.Info("become pre-candidate", zap.Uint64("member", e.id), zap.Uint64("term", e.Term))
}

func (e *election) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if e.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	e.step = e.stepLeader
	e.reset(e.Term)
	e.tick = e.tickHeartbeat
	e.lead = e.id
	e.state = StateLeader
	// Followers enter replicate mode when they've been successfully probed
	// (perhaps after having received a snapshot as a result). The leader is
	// trivially in this state. Note that r.reset() has initialized this
	// progress with the last index already.
	pr := e.prs.Progress[e.id]
	pr.BecomeReplicate()
	// The leader always has RecentActive == true; MsgCheckQuorum makes sure to
	// preserve this.
	pr.RecentActive = true

	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	e.pendingConfIndex = e.raftLog.lastIndex()

	emptyEnt := pb.Entry{Data: nil}
	if !e.appendEntry(emptyEnt) {
		// This won't happen because we just called reset() above.
		e.logger.Panic("empty entry is dropped", zap.Uint64("member", e.id))
	}
	// The payloadSize of an empty entry is 0 (see TestPayloadSizeOfEmptyEntry),
	// so the preceding log append does not count against the uncommitted log
	// quota of the new leader. In other words, after the call to appendEntry,
	// r.uncommittedSize is still 0.
	e.logger.Info("become leader", zap.Uint64("member", e.id), zap.Uint64("term", e.Term))
}

func (e *election) hup(t CampaignType) {
	if e.state == StateLeader {
		e.logger.Debug("ignore MsgHup because already leader", zap.Uint64("member", e.id))
		return
	}

	if !e.promotable() {
		e.logger.Warn("unpromotable and can not campaign", zap.Uint64("member", e.id))
		return
	}
	ents, err := e.raftLog.slice(e.raftLog.applied+1, e.raftLog.committed+1, noLimit)
	if err != nil {
		e.logger.Panic("unexpected error on getting unapplied entries", zap.Uint64("member", e.id))
	}
	if n := numOfPendingConf(ents); n != 0 && e.raftLog.committed > e.raftLog.applied {
		e.logger.Warn("cannot campaign since there are still pending configuration changes to apply",
			zap.Uint64("member", e.id),
			zap.Uint64("term", e.Term))
		return
	}

	e.logger.Info("start a new election", zap.Uint64("member", e.id), zap.Uint64("term", e.Term))
	e.campaign(t)
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
func (e *election) campaign(t CampaignType) {
	if !e.promotable() {
		// This path should not be hit (callers are supposed to check), but
		// better safe than sorry.
		e.logger.Warn("campaign() should have been called since unpromotable", zap.Uint64("member", e.id))
	}
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection {
		e.becomePreCandidate()
		voteMsg = pb.MsgPreVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		term = e.Term + 1
	} else {
		e.becomeCandidate()
		voteMsg = pb.MsgVote
		term = e.Term
	}
	var ids []uint64
	{
		idMap := e.prs.Voters.IDs()
		ids = make([]uint64, 0, len(idMap))
		for id := range idMap {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	for _, id := range ids {
		if id == e.id {
			// The candidate votes for itself and should account for this self
			// vote once the vote has been durably persisted (since it doesn't
			// send a MsgVote to itself). This response message will be added to
			// msgsAfterAppend and delivered back to this node after the vote
			// has been written to stable storage.
			e.send(pb.Message{To: id, Term: term, Type: voteRespMsgType(voteMsg)})
			continue
		}
		e.logger.Info("send vote request",
			zap.Uint64("member", e.id),
			zap.Uint64("term", e.Term),
			zap.Uint64("logterm", e.raftLog.lastTerm()),
			zap.Uint64("index", e.raftLog.lastIndex()),
			zap.String("msg", voteMsg.String()),
			zap.Uint64("to", id))

		e.send(pb.Message{To: id, Term: term, Type: voteMsg, Index: e.raftLog.lastIndex(), LogTerm: e.raftLog.lastTerm()})
	}
}

func (e *election) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result quorum.VoteResult) {
	if v {
		e.logger.Info("receive vote grant",
			zap.Uint64("member", e.id),
			zap.Uint64("term", e.Term),
			zap.String("msg", t.String()),
			zap.Uint64("from", id))
	} else {
		e.logger.Info("receive vote reject",
			zap.Uint64("member", e.id),
			zap.Uint64("term", e.Term),
			zap.String("msg", t.String()),
			zap.Uint64("from", id))
	}
	e.prs.RecordVote(id, v)
	return e.prs.TallyVotes()
}

func (e *election) Step(m pb.Message) error {
	// Handle the message term, which may result in our stepping down to a follower.
	switch {
	case m.Term == 0:
		// local message
	case m.Term > e.Term:
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			inLease := e.checkQuorum && e.lead != None && e.electionElapsed < e.electionTimeout
			if inLease {
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				e.logger.Info("ignore the message because lease is not expired",
					zap.Uint64("member", e.id),
					zap.Uint64("term", e.Term),
					zap.Uint64("logterm", e.raftLog.lastTerm()),
					zap.Uint64("index", e.raftLog.lastIndex()),
					zap.Uint64("vote", e.Vote),
					zap.String("msg", m.String()),
					zap.Int("tick", e.electionTimeout-e.electionElapsed))
				return nil
			}
		}
		switch {
		case m.Type == pb.MsgPreVote:
			// Never change our term in response to a PreVote
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			e.logger.Info("receive a message with higher term",
				zap.Uint64("member", e.id),
				zap.Uint64("term", e.Term),
				zap.String("msg", m.String()))
			if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
				e.becomeFollower(m.Term, m.From)
			} else {
				e.becomeFollower(m.Term, None)
			}
		}

	case m.Term < e.Term:
		if (e.checkQuorum || e.preVote) && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
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
			e.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else if m.Type == pb.MsgPreVote {
			// Before Pre-Vote enable, there may have candidate with higher term,
			// but less log. After update to Pre-Vote, the cluster may deadlock if
			// we drop messages with a lower term.
			e.logger.Info("reject vote",
				zap.Uint64("member", e.id),
				zap.Uint64("term", e.Term),
				zap.Uint64("logterm", e.raftLog.lastTerm()),
				zap.Uint64("index", e.raftLog.lastIndex()),
				zap.Uint64("vote", e.Vote),
				zap.String("msg", m.String()))
			e.send(pb.Message{To: m.From, Term: e.Term, Type: pb.MsgPreVoteResp, Reject: true})
		} else if m.Type == pb.MsgStorageAppendResp {
			if m.Index != 0 {
				// Don't consider the appended log entries to be stable because
				// they may have been overwritten in the unstable log during a
				// later term. See the comment in newStorageAppendResp for more
				// about this race.
				e.logger.Info("ignore entry appends from a message with lower term",
					zap.Uint64("member", e.id),
					zap.Uint64("term", e.Term),
					zap.String("msg", m.String()))
			}
			if m.Snapshot != nil {
				// Even if the snapshot applied under a different term, its
				// application is still valid. Snapshots carry committed
				// (term-independent) state.
				e.appliedSnap(m.Snapshot)
			}
		} else {
			// ignore other cases
			e.logger.Info("ignore a message with lower term",
				zap.Uint64("member", e.id),
				zap.Uint64("term", e.Term),
				zap.String("msg", m.String()))
		}
		return nil
	}

	switch m.Type {
	case pb.MsgHup:
		if e.preVote {
			e.hup(campaignPreElection)
		} else {
			e.hup(campaignElection)
		}

	case pb.MsgStorageAppendResp:
		if m.Index != 0 {
			e.raftLog.stableTo(m.Index, m.LogTerm)
		}
		if m.Snapshot != nil {
			e.appliedSnap(m.Snapshot)
		}

	case pb.MsgStorageApplyResp:
		if len(m.Entries) > 0 {
			index := m.Entries[len(m.Entries)-1].Index
			e.appliedTo(index, entsSize(m.Entries))
			e.reduceUncommittedSize(payloadsSize(m.Entries))
		}

	case pb.MsgVote, pb.MsgPreVote:
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := e.Vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(e.Vote == None && e.lead == None) ||
			// ...or this is a PreVote for a future term...
			(m.Type == pb.MsgPreVote && m.Term > e.Term)
		// ...and we believe the candidate is up to date.
		if canVote && e.raftLog.isUpToDate(m.Index, m.LogTerm) {
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
			e.logger.Info("grant vote",
				zap.Uint64("member", e.id),
				zap.Uint64("term", e.Term),
				zap.Uint64("logterm", e.raftLog.lastTerm()),
				zap.Uint64("index", e.raftLog.lastIndex()),
				zap.Uint64("vote", e.Vote),
				zap.String("msg", m.String()))
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why, consider the
			// case where a single node was previously partitioned away and
			// it's local term is now out of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			e.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// Only record real votes.
				e.electionElapsed = 0
				e.Vote = m.From
			}
		} else {
			e.logger.Info("reject vote",
				zap.Uint64("member", e.id),
				zap.Uint64("term", e.Term),
				zap.Uint64("mlogterm", e.raftLog.lastTerm()),
				zap.Uint64("mindex", e.raftLog.lastIndex()),
				zap.Uint64("mvote", e.Vote),
				zap.String("msg", m.String()))

			e.send(pb.Message{To: m.From, Term: e.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		err := e.step(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *election) stepLeader(m pb.Message) error {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat:
		e.bcastHeartbeat()
		return nil
	case pb.MsgCheckQuorum:
		if !e.prs.QuorumActive() {
			e.logger.Warn("step down to follower since quorum is not active", zap.Uint64("member", e.id))
			e.becomeFollower(e.Term, None)
		}
		// Mark everyone (but ourselves) as inactive in preparation for the next
		// CheckQuorum.
		e.prs.Visit(func(id uint64, pr *tracker.Progress) {
			if id != e.id {
				pr.RecentActive = false
			}
		})
		return nil
	}

	// All other message types require a progress for m.From (pr).
	pr := e.prs.Progress[m.From]
	if pr == nil {
		e.logger.Debug("no progress available for the message", zap.Uint64("member", e.id), zap.Uint64("from", m.From))
		return nil
	}
	switch m.Type {
	case pb.MsgAppResp:
		// NB: this code path is also hit from (*raft).advance, where the leader steps
		// an MsgAppResp to acknowledge the appended entries in the last Ready.

		pr.RecentActive = true

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
			e.logger.Debug("receive MsgAppResp rejected",
				zap.Uint64("member", e.id),
				zap.String("msg", m.String()))
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
				nextProbeIdx = e.raftLog.findConflictByTerm(m.RejectHint, m.LogTerm)
			}
			if pr.MaybeDecrTo(m.Index, nextProbeIdx) {
				e.logger.Debug("decrease progress", zap.Uint64("member", e.id),
					zap.Uint64("from", m.From),
					zap.String("pr", pr.String()))
				if pr.State == tracker.StateReplicate {
					pr.BecomeProbe()
				}
				e.sendAppend(m.From)
			}
		} else {
			oldPaused := pr.IsPaused()
			if pr.MaybeUpdate(m.Index) {
				switch {
				case pr.State == tracker.StateProbe:
					pr.BecomeReplicate()
				case pr.State == tracker.StateSnapshot && pr.Match >= pr.PendingSnapshot:
					// TODO(tbg): we should also enter this branch if a snapshot is
					// received that is below pr.PendingSnapshot but which makes it
					// possible to use the log again.
					e.logger.Debug("recover from needing snapshot, resume sending replication messages",
						zap.Uint64("member", e.id),
						zap.Uint64("from", m.From),
						zap.String("pr", pr.String()))
					// Transition back to replicating state via probing state
					// (which takes the snapshot into account). If we didn't
					// move to replicating state, that would only happen with
					// the next round of appends (but there may not be a next
					// round for a while, exposing an inconsistent RaftStatus).
					pr.BecomeProbe()
					pr.BecomeReplicate()
				case pr.State == tracker.StateReplicate:
					pr.Inflights.FreeLE(m.Index)
				}

				if oldPaused {
					// If we were paused before, this node may be missing the
					// latest commit index, so send it.
					e.sendAppend(m.From)
				}
				// We've updated flow control information above, which may
				// allow us to send multiple (size-limited) in-flight messages
				// at once (such as when transitioning from probe to
				// replicate, or when freeTo() covers multiple messages). If
				// we have more entries to send, send as many messages as we
				// can (without sending empty messages for the commit index)
				if e.id != m.From {
					for e.maybeSendAppend(m.From, false /* sendIfEmpty */) {
					}
				}
			}
		}
	case pb.MsgHeartbeatResp:
		pr.RecentActive = true
		pr.MsgAppFlowPaused = false

		// NB: if the follower is paused (full Inflights), this will still send an
		// empty append, allowing it to recover from situations in which all the
		// messages that filled up Inflights in the first place were dropped. Note
		// also that the outgoing heartbeat already communicated the commit index.
		if pr.Match < e.raftLog.lastIndex() {
			e.sendAppend(m.From)
		}
		return nil
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
			e.logger.Debug("snapshot succeeds, resume sending replication messages",
				zap.Uint64("member", e.id),
				zap.Uint64("from", m.From),
				zap.String("pr", pr.String()))
		} else {
			// NB: the order here matters or we'll be probing erroneously from
			// the snapshot index, but the snapshot never applied.
			pr.PendingSnapshot = 0
			pr.BecomeProbe()
			e.logger.Debug("snapshot fails, resume sending replication messages",
				zap.Uint64("member", e.id),
				zap.Uint64("from", m.From),
				zap.String("pr", pr.String()))
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
		e.logger.Debug("fail to send message because unreachable",
			zap.Uint64("member", e.id),
			zap.Uint64("from", m.From),
			zap.String("pr", pr.String()))
	}
	return nil
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
func (e *election) stepCandidate(m pb.Message) error {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if e.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgApp:
		e.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		e.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		e.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		e.handleHeartbeat(m)
	case pb.MsgSnap:
		e.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		e.handleSnapshot(m)
	case myVoteRespType:
		gr, rj, res := e.poll(m.From, m.Type, !m.Reject)
		e.logger.Info("get vote response, generate poll result", zap.Uint64("member", e.id),
			zap.String("msg", m.String()),
			zap.Int("grants", gr),
			zap.Int("rejects", rj))
		switch res {
		case quorum.VoteWon:
			if e.state == StatePreCandidate {
				e.campaign(campaignElection)
			} else {
				e.becomeLeader()
				e.bcastAppend()
			}
		case quorum.VoteLost:
			// pb.MsgPreVoteResp contains future term of pre-candidate
			// m.Term > r.Term; reuse r.Term
			e.becomeFollower(e.Term, None)
		}
	}
	return nil
}

func (e *election) stepFollower(m pb.Message) error {
	switch m.Type {
	case pb.MsgApp:
		e.electionElapsed = 0
		e.lead = m.From
		e.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		e.electionElapsed = 0
		e.lead = m.From
		e.handleHeartbeat(m)
	case pb.MsgSnap:
		e.electionElapsed = 0
		e.lead = m.From
		e.handleSnapshot(m)
	}
	return nil
}

func (e *election) handleAppendEntries(m pb.Message) {
	if m.Index < e.raftLog.committed {
		e.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: e.raftLog.committed})
		return
	}

	if mlastIndex, ok := e.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		e.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		e.logger.Debug("reject MsgAPP",
			zap.Uint64("member", e.id),
			zap.Uint64("logterm", e.raftLog.zeroTermOnErrCompacted(e.raftLog.term(m.Index))),
			zap.Uint64("index", m.Index),
			zap.String("msg", m.String()))

		// Return a hint to the leader about the maximum index and term that the
		// two logs could be divergent at. Do this by searching through the
		// follower's log for the maximum (index, term) pair with a term <= the
		// MsgApp's LogTerm and an index <= the MsgApp's Index. This can help
		// skip all indexes in the follower's uncommitted tail with terms
		// greater than the MsgApp's LogTerm.
		//
		// See the other caller for findConflictByTerm (in stepLeader) for a much
		// more detailed explanation of this mechanism.
		hintIndex := min(m.Index, e.raftLog.lastIndex())
		hintIndex = e.raftLog.findConflictByTerm(hintIndex, m.LogTerm)
		hintTerm, err := e.raftLog.term(hintIndex)
		if err != nil {
			panic(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
		}
		e.send(pb.Message{
			To:         m.From,
			Type:       pb.MsgAppResp,
			Index:      m.Index,
			Reject:     true,
			RejectHint: hintIndex,
			LogTerm:    hintTerm,
		})
	}
}

func (e *election) handleHeartbeat(m pb.Message) {
	e.raftLog.commitTo(m.Commit)
	e.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}

func (e *election) handleSnapshot(m pb.Message) {
	// MsgSnap messages should always carry a non-nil Snapshot, but err on the
	// side of safety and treat a nil Snapshot as a zero-valued Snapshot.
	var s pb.Snapshot
	if m.Snapshot != nil {
		s = *m.Snapshot
	}
	if e.restore(s) {
		e.logger.Info("restore snapshot", zap.Uint64("member", e.id),
			zap.Uint64("commit", e.raftLog.committed),
			zap.String("snap", s.Metadata.String()))
		e.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: e.raftLog.lastIndex()})
	} else {
		e.logger.Info("ignore snapshot", zap.Uint64("member", e.id),
			zap.Uint64("commit", e.raftLog.committed),
			zap.String("snap", s.Metadata.String()))
		e.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: e.raftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine. If this method returns false, the snapshot was
// ignored, either because it was obsolete or because of an error.
func (e *election) restore(s pb.Snapshot) bool {
	if s.Metadata.Index <= e.raftLog.committed {
		return false
	}
	if e.state != StateFollower {
		// This is defense-in-depth: if the leader somehow ended up applying a
		// snapshot, it could move into a new term without moving into a
		// follower state. This should never fire, but if it did, we'd have
		// prevented damage by returning early, so log only a loud warning.
		//
		// At the time of writing, the instance is guaranteed to be in follower
		// state when this method is called.
		e.logger.Warn("attempt to restore snapshot as leader; should never happen", zap.Uint64("member", e.id))
		e.becomeFollower(e.Term+1, None)
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
			if id == e.id {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		e.logger.Warn(
			"attempt to restore snapshot but it is not in the ConfState; should never happen",
			zap.Uint64("member", e.id),
			zap.String("cs", cs.String()))
		return false
	}

	// Now go ahead and actually restore.
	if e.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		e.logger.Info("fast-forwarded commit to snapshot",
			zap.Uint64("member", e.id),
			zap.Uint64("commit", e.raftLog.committed),
			zap.Uint64("logterm", e.raftLog.lastTerm()),
			zap.Uint64("index", e.raftLog.lastIndex()),
			zap.String("snap", s.Metadata.String()))
		e.raftLog.commitTo(s.Metadata.Index)
		return false
	}

	e.raftLog.restore(s)

	// Reset the configuration and add the (potentially updated) peers in anew.
	e.prs = tracker.MakeProgressTracker(e.prs.MaxInflight, e.prs.MaxInflightBytes)
	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   e.prs,
		LastIndex: e.raftLog.lastIndex(),
	}, cs)

	if err != nil {
		// This should never happen. Either there's a bug in our config change
		// handling or the client corrupted the conf change.
		panic(fmt.Sprintf("unable to restore config %+v: %s", cs, err))
	}

	assertConfStatesEquivalent(e.config.Logger, cs, e.switchToConfig(cfg, prs))

	pr := e.prs.Progress[e.id]
	pr.MaybeUpdate(pr.Next - 1) // TODO(tbg): this is untested and likely unneeded

	e.logger.Info("restore snapshot", zap.Uint64("member", e.id),
		zap.Uint64("commit", e.raftLog.committed),
		zap.Uint64("logterm", e.raftLog.lastTerm()),
		zap.Uint64("index", e.raftLog.lastIndex()),
		zap.String("snap", s.Metadata.String()))
	return true
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (e *election) promotable() bool {
	pr := e.prs.Progress[e.id]
	return pr != nil && !pr.IsLearner && !e.raftLog.hasNextOrInProgressSnapshot()
}

func (e *election) applyConfChange(cc pb.ConfChangeV2) pb.ConfState {
	cfg, prs, err := func() (tracker.Config, tracker.ProgressMap, error) {
		changer := confchange.Changer{
			Tracker:   e.prs,
			LastIndex: e.raftLog.lastIndex(),
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

	return e.switchToConfig(cfg, prs)
}

// switchToConfig reconfigures this node to use the provided configuration. It
// updates the in-memory state and, when necessary, carries out additional
// actions such as reacting to the removal of nodes or changed quorum
// requirements.
//
// The inputs usually result from restoring a ConfState or applying a ConfChange.
func (e *election) switchToConfig(cfg tracker.Config, prs tracker.ProgressMap) pb.ConfState {
	e.prs.Config = cfg
	e.prs.Progress = prs

	e.logger.Info("switch configuration", zap.Uint64("member", e.id), zap.String("cfg", e.prs.Config.String()))
	cs := e.prs.ConfState()
	pr, ok := e.prs.Progress[e.id]

	// Update whether the node itself is a learner, resetting to false when the
	// node is removed.
	e.isLearner = ok && pr.IsLearner

	if (!ok || e.isLearner) && e.state == StateLeader {
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
	if e.state != StateLeader || len(cs.Voters) == 0 {
		return cs
	}

	if e.maybeCommit() {
		// If the configuration change means that more entries are committed now,
		// broadcast/append to everyone in the updated config.
		e.bcastAppend()
	} else {
		// Otherwise, still probe the newly added replicas; there's no reason to
		// let them wait out a heartbeat interval (or the next incoming
		// proposal).
		e.prs.Visit(func(id uint64, pr *tracker.Progress) {
			if id == e.id {
				return
			}
			e.maybeSendAppend(id, false /* sendIfEmpty */)
		})
	}

	return cs
}

func (e *election) loadState(state pb.HardState) {
	if state.Commit < e.raftLog.committed || state.Commit > e.raftLog.lastIndex() {
		e.logger.Panic("commit of state is out of range", zap.Uint64("member", e.id),
			zap.Uint64("commit", state.Commit))
	}
	e.raftLog.committed = state.Commit
	e.Term = state.Term
	e.Vote = state.Vote
}

// pastElectionTimeout returns true if r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (e *election) pastElectionTimeout() bool {
	return e.electionElapsed >= e.randomizedElectionTimeout
}

func (e *election) resetRandomizedElectionTimeout() {
	e.randomizedElectionTimeout = e.electionTimeout + globalRand.Intn(e.electionTimeout)
}

// increaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its maxUncommittedSize limit.
// If the new entries would exceed the limit, the method returns false. If not,
// the increase in uncommitted entry size is recorded and the method returns
// true.
//
// Empty payloads are never refused. This is used both for appending an empty
// entry at a new leader's term, as well as leaving a joint configuration.
func (e *election) increaseUncommittedSize(ents []pb.Entry) bool {
	s := payloadsSize(ents)
	if e.uncommittedSize > 0 && s > 0 && e.uncommittedSize+s > e.maxUncommittedSize {
		// If the uncommitted tail of the Raft log is empty, allow any size
		// proposal. Otherwise, limit the size of the uncommitted tail of the
		// log and drop any proposal that would push the size over the limit.
		// Note the added requirement s>0 which is used to make sure that
		// appending single empty entries to the log always succeeds, used both
		// for replicating a new leader's initial empty entry, and for
		// auto-leaving joint configurations.
		return false
	}
	e.uncommittedSize += s
	return true
}

// reduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
func (e *election) reduceUncommittedSize(s entryPayloadSize) {
	if s > e.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		e.uncommittedSize = 0
	} else {
		e.uncommittedSize -= s
	}
}

func getProgressCopyElect(e *election) map[uint64]tracker.Progress {
	m := make(map[uint64]tracker.Progress)
	e.prs.Visit(func(id uint64, pr *tracker.Progress) {
		p := *pr
		p.Inflights = pr.Inflights.Clone()
		pr = nil

		m[id] = p
	})
	return m
}

// getStatus gets a copy of the current raft status.
func getStatusElect(e *election) Status {
	var s Status
	s.BasicStatus = getBasicStatusElect(e)
	if s.RaftState == StateLeader {
		s.Progress = getProgressCopyElect(e)
	}
	s.Config = e.prs.Config.Clone()
	return s
}

func getBasicStatusElect(e *election) BasicStatus {
	s := BasicStatus{
		ID:             e.id,
		LeadTransferee: e.leadTransferee,
	}
	s.HardState = e.hardState()
	s.SoftState = e.softState()
	s.Applied = e.raftLog.applied
	return s
}
