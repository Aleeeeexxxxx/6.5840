package raft

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"6.5840/labrpc"

	"go.uber.org/zap"
)

type Leader struct {
	worker *Raft
	logger *zap.Logger
	term   int

	stopCh chan struct{}
	peers  []*replicator
	// shared StateManager for replicators
	replicatorStateMngr *atomic.Value
}

func NewLeader(worker *Raft) *Leader {
	leader := &Leader{
		worker: worker,
		logger: GetLoggerOrPanic("leader").
			With(zap.Int(Term, worker.state.GetCurrentTerm())).
			With(zap.Int(Index, worker.me)),
		stopCh:              make(chan struct{}),
		term:                worker.state.GetCurrentTerm(),
		replicatorStateMngr: new(atomic.Value),
	}

	leader.replicatorStateMngr.Store(worker.state.New())

	for index, p := range worker.peers {
		if index == worker.me {
			continue
		}
		leader.peers = append(
			leader.peers,
			NewReplicator(index, p, worker, leader.stopCh, leader.replicatorStateMngr),
		)
	}
	return leader
}

func (l *Leader) Type() RoleType {
	return RoleLeader
}

func (l *Leader) StartDaemon() {
	for _, peer := range l.peers {
		go peer.daemon()
	}
}

func (l *Leader) StopDaemon() {
	close(l.stopCh)
}

func (l *Leader) HandleNotify() {}

func (l *Leader) HandleRequestVotesTask(task *RequestVotesTask) {
	currentTerm := l.term
	peerTerm := task.args.Term
	logger := l.logger.With(
		zap.Int("peer index", task.args.Me),
		zap.Int("peer term", task.args.Term),
		zap.Int(Term, currentTerm),
	)

	if currentTerm > peerTerm {
		logger.Debug("RequestVote reject, term ahead")
		task.reply.VoteFor = false
	} else if currentTerm == peerTerm {
		l.logger.Debug("RequestVote reject, I'm leader")
		task.reply.VoteFor = false
	} else {
		l.worker.state.UpdateTerm(peerTerm)
		l.worker.context.MarkStateChanged()

		if l.worker.state.IsLogAheadPeer(
			task.args.LeaderLastLogIndex, task.args.LeaderLastLogTerm,
		) {
			logger.Debug("RequestVote reject, log ahead peer",
				zap.Int("lastLogIndex", l.worker.state.logMngr.GetLastLogIndex()),
				zap.Int("lastLogTerm", l.worker.state.logMngr.GetLastLogTerm()),
				zap.Int("peerLastLogIndex", task.args.LeaderLastLogIndex),
				zap.Int("peerLastLogIndex", task.args.LeaderLastLogTerm),
			)
			l.worker.become(RoleCandidate)
			task.reply.VoteFor = false
		} else {
			logger.Debug("RequestVote granted")
			l.worker.become(RoleFollower)
			task.reply.VoteFor = true
		}
	}
}

func (l *Leader) HandleAppendEntriesTask(task *AppendEntriesTask) {
	currentTerm := l.term
	peerTerm := task.args.Term
	logger := l.logger.With(
		zap.Int("peer index", task.args.Me),
		zap.Int("peer term", task.args.Term),
		zap.Int(Term, currentTerm),
	)

	if currentTerm > peerTerm {
		handleTermBehindRequest(l.worker, task.reply, logger)
	} else if currentTerm == peerTerm {
		panic("two leaders in one term")
	} else {
		logger.Info("found new leader")
		l.worker.become(RoleFollower)
		l.worker.state.SyncStateFromAppendEntriesTask(task)
		l.worker.context.MarkStateChanged()
	}
}

func (l *Leader) UpdateReplicatorState() {
	l.replicatorStateMngr.Store(l.worker.state.New())
}

func (l *Leader) UpdateCommittedIndex() {
	committed := make([]int32, 0, len(l.peers))

	for _, peer := range l.peers {
		committed = append(committed, atomic.LoadInt32(&peer.replicated))
	}
	sort.Slice(committed, func(i, j int) bool {
		return committed[i] < committed[j]
	})

	if tryToCommit := int(committed[len(committed)/2]); tryToCommit >= IndexStartFrom {
		term, err := l.worker.state.logMngr.GetLogTermByIndex(tryToCommit)
		if err != nil {
			panic(err)
		}

		// only commit logs in this term
		if term == l.term && l.worker.state.UpdateCommitted(tryToCommit) {
			l.UpdateReplicatorState()
		}
	}
}

type replicator struct {
	term      int
	me        int
	peerIndex int
	peer      *labrpc.ClientEnd

	logger     *zap.Logger
	state      *atomic.Value
	nextIndex  int
	replicated int32

	stopCh  chan struct{}
	timeout time.Duration
}

func NewReplicator(
	peerIndex int,
	peer *labrpc.ClientEnd,
	worker *Raft,
	stopCh chan struct{},
	stateMngr *atomic.Value,
) *replicator {
	return &replicator{
		term:      worker.state.GetCurrentTerm(),
		me:        worker.me,
		peerIndex: peerIndex,
		peer:      peer,
		state:     stateMngr,
		logger: GetLoggerOrPanic("replicator").
			With(zap.Int(Peer, peerIndex)).
			With(zap.Int(Term, worker.state.GetCurrentTerm())).
			With(zap.Int(Index, worker.me)),
		stopCh:     stopCh,
		timeout:    worker.heartBeatInterval / 5,
		replicated: EmptyLogIndex,
	}
}

func (rp *replicator) initNextIndex() {
	// replicator has to make sure that DO NOT send empty log when matching
	// if not, wrong logs could be committed, see src/raft/state.go:79
	rp.nextIndex = rp.state.Load().(*StateManager).logMngr.GetLastLogIndex()
	if rp.nextIndex == EmptyLogIndex { // no log in the state mngr
		rp.nextIndex = IndexStartFrom
	}
}

func (rp *replicator) daemon() {
	rp.initNextIndex()
	rp.syncLogsWithPeer()
	timer := time.NewTimer(rp.timeout)

	for {
		select {
		case <-rp.stopCh:
			rp.logger.Info("replicator stopped")
			return
		case <-timer.C:
			rp.syncLogsWithPeer()
			timer.Reset(rp.timeout)
		}
	}
}

func (rp *replicator) syncLogsWithPeer() {
	rp.logger.Info("replicator start sync")
	continueSending := true

	for continueSending {
		var (
			args  AppendEntryArgs
			reply AppendEntryReply
		)
		args, continueSending = rp.fillAppendEntriesArgs()
		rp.logger.Debug(
			"AppendEntries args",
			zap.String("v", fmt.Sprintf("%#v", args)),
			zap.String("log", fmt.Sprintf("%#v", args.Logs)),
		)

		if err := rp.withTimeout(func() error {
			//rp.logger.Debug("call RPC for sync logs")
			if ok := rp.peer.Call("Raft.AppendEntries", args, &reply); !ok {
				return errorSendReqToPeer
			}
			return nil
		}); err != nil {
			rp.logger.Warn(
				"an error happened when sending RPC request to peer",
				zap.Error(err),
			)
			return
		}

		rp.logger.Debug(
			"AppendEntries reply",
			zap.String("v", fmt.Sprintf("%#v", reply)),
		)
		rp.nextIndex = reply.ExpectedNextIndex
		if reply.Success {
			if args.Logs != nil {
				atomic.StoreInt32(&rp.replicated, int32(args.Logs.Index))
			} else {
				atomic.StoreInt32(&rp.replicated, int32(args.Snapshot.LastLogIndex))
			}
		}
		if reply.Term > rp.term {
			continueSending = false
		}
	}
}

func (rp *replicator) fillAppendEntriesArgs() (AppendEntryArgs, bool) {
	stateMngr := rp.state.Load().(*StateManager)
	args := AppendEntryArgs{
		Term:                rp.term,
		Me:                  rp.me,
		LeaderLastCommitted: stateMngr.GetCommitted(),
		LeaderLastLogIndex:  stateMngr.logMngr.GetLastLogIndex(),
		LeaderLastLogTerm:   stateMngr.logMngr.GetLastLogTerm(),
	}

	expectedLog, err := stateMngr.logMngr.GetLogEntryByIndex(rp.nextIndex)
	if err == errorLogIndexExceedUpperLimit {
		rp.logger.Info("all logs replicated to peer, heartbeat only")
		return args, false
	}
	if err == errorRetrieveEntryInSnapshot {
		rp.logger.Info("sync snapshot to peer")
		args.Snapshot = stateMngr.logMngr.GetSnapshot()
		return args, true
	}

	args.Logs = expectedLog
	if previous := rp.nextIndex - 1; previous >= IndexStartFrom {
		term, err := stateMngr.logMngr.GetLogTermByIndex(previous)
		if err != nil {
			panic(fmt.Sprintf("previous log index should not out of range, index=%d", previous))
		}

		args.LastLogIndex = previous
		args.LastLogTerm = term
	} else {
		args.LastLogIndex = EmptyLogIndex
		args.LastLogTerm = -1
	}
	return args, true
}

func (rp *replicator) withTimeout(fn func() error) error {
	timer := time.NewTimer(rp.timeout)
	done := make(chan struct{})
	var err error

	go func() {
		err = fn()
		select {
		case <-timer.C:
		default:
			done <- struct{}{}
		}
		close(done)
	}()

	select {
	case <-timer.C:
		return errorTimeout
	case <-done:
		return err
	}
}
