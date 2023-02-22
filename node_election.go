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
	"context"
	"errors"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

var (
	// the invocations of some methods are not supported in election experiment and relevant messages should not exist.
	ErrPropose        = errors.New("method Propose in NodeElect is not supported in election experiment")
	ErrTransferLeader = errors.New("method TransferLeadership in NodeElect is not supported in election experiment")
	ErrReadIndex      = errors.New("method ReadIndex in NodeElect is not supported in election experiment")
)

// StartNodeElect returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
func StartNodeElect(c *Config, peers []Peer, logger *zap.Logger) Node {
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	rne, err := NewRawNodeElect(c, logger)
	if err != nil {
		panic(err)
	}
	err = rne.Bootstrap(peers)
	if err != nil {
		logger.Warn("error occurred during starting a new node", zap.Uint64("member", c.ID), zap.Error(err))
	}

	ne := newNodeElect(rne)
	go ne.run()
	return &ne
}

// RestartNodeElect is similar to StartNodeElect but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
func RestartNodeElect(c *Config, logger *zap.Logger) Node {
	rne, err := NewRawNodeElect(c, logger)
	if err != nil {
		panic(err)
	}
	ne := newNodeElect(rne)
	go ne.run()
	return &ne
}

// NodeElect is the canonical implementation of the Node interface
type NodeElect struct {
	recvc      chan pb.Message
	confc      chan pb.ConfChangeV2
	confstatec chan pb.ConfState
	readyc     chan Ready
	advancec   chan struct{}
	tickc      chan struct{}
	done       chan struct{}
	stop       chan struct{}
	status     chan chan Status

	rne *RawNodeElect
}

func newNodeElect(rne *RawNodeElect) NodeElect {
	return NodeElect{
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
		rne:    rne,
	}
}

func (ne *NodeElect) Stop() {
	select {
	case ne.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-ne.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-ne.done
}

func (ne *NodeElect) run() {
	var readyc chan Ready
	var advancec chan struct{}
	var rd Ready

	e := ne.rne.election
	for {
		if advancec == nil && ne.rne.HasReady() {
			// Populate a Ready. Note that this Ready is not guaranteed to
			// actually be handled. We will arm readyc, but there's no guarantee
			// that we will actually send on it. It's possible that we will
			// service another channel instead, loop around, and then populate
			// the Ready again. We could instead force the previous Ready to be
			// handled first, but it's generally good to emit larger Readys plus
			// it simplifies testing (by emitting less frequently and more
			// predictably).
			rd = ne.rne.readyWithoutAccept()
			readyc = ne.readyc
		}

		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		case m := <-ne.recvc:
			if IsResponseMsg(m.Type) && !IsLocalMsgTarget(m.From) && e.prs.Progress[m.From] == nil {
				// Filter out response message from unknown From.
				break
			}
			e.Step(m)
		case cc := <-ne.confc:
			cs := e.applyConfChange(cc)
			select {
			case ne.confstatec <- cs:
			case <-ne.done:
			}
		case <-ne.tickc:
			ne.rne.Tick()
		case readyc <- rd:
			ne.rne.acceptReady(rd)
			if !ne.rne.asyncStorageWrites {
				advancec = ne.advancec
			} else {
				rd = Ready{}
			}
			readyc = nil
		case <-advancec:
			ne.rne.Advance(rd)
			rd = Ready{}
			advancec = nil
		case c := <-ne.status:
			c <- getStatusElect(e)
		case <-ne.stop:
			close(ne.done)
			return
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (ne *NodeElect) Tick() {
	select {
	case ne.tickc <- struct{}{}:
	case <-ne.done:
	default:
		e := ne.rne.election
		e.logger.Warn("A tick missed to fire. Node blocks too long!", zap.Uint64("member", e.id))
	}
}

func (ne *NodeElect) Campaign(ctx context.Context) error {
	return ne.step(ctx, pb.Message{Type: pb.MsgHup})
}

func (ne *NodeElect) Propose(ctx context.Context, data []byte) error {
	return ErrPropose
}

func (ne *NodeElect) Step(ctx context.Context, m pb.Message) error {
	// Ignore unexpected local messages receiving over network.
	if IsLocalMsg(m.Type) && !IsLocalMsgTarget(m.From) {
		// TODO: return an error?
		return nil
	}
	return ne.step(ctx, m)
}

func (ne *NodeElect) ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error {
	return ErrPropose
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (ne *NodeElect) step(ctx context.Context, m pb.Message) error {
	if m.Type != pb.MsgProp {
		select {
		case ne.recvc <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ne.done:
			return ErrStopped
		}
	}
	return ErrPropose
}

func (ne *NodeElect) Ready() <-chan Ready { return ne.readyc }

func (ne *NodeElect) Advance() {
	select {
	case ne.advancec <- struct{}{}:
	case <-ne.done:
	}
}

func (ne *NodeElect) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	var cs pb.ConfState
	select {
	case ne.confc <- cc.AsV2():
	case <-ne.done:
	}
	select {
	case cs = <-ne.confstatec:
	case <-ne.done:
	}
	return &cs
}

func (ne *NodeElect) Status() Status {
	c := make(chan Status)
	select {
	case ne.status <- c:
		return <-c
	case <-ne.done:
		return Status{}
	}
}

func (ne *NodeElect) ReportUnreachable(id uint64) {
	select {
	case ne.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-ne.done:
	}
}

func (ne *NodeElect) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	select {
	case ne.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-ne.done:
	}
}

func (ne *NodeElect) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	panic(ErrTransferLeader)
}

func (ne *NodeElect) ReadIndex(ctx context.Context, rctx []byte) error {
	return ErrReadIndex
}

func (ne *NodeElect) IsLeader() bool {
	e := ne.rne.election
	return e.lead == e.id
}
