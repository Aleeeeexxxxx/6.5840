package raft

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeader_UpdateCommittedIndex(t *testing.T) {
	rq := require.New(t)

	l := &Leader{}
	l.term = 1
	l.replicatorStateMngr = new(atomic.Value)
	l.worker = &Raft{
		state: &StateManager{
			logMngr: &LogService{
				Storage: Storage{
					Logs: Logs{
						{Term: 1, Index: 1},
						{Term: 1, Index: 2},
						{Term: 1, Index: 3},
						{Term: 1, Index: 4},
						{Term: 1, Index: 5},
					},
					Snapshot: nil,
				},
			},
		},
	}

	t.Run("should update committed index, 5 total", func(t *testing.T) {
		l.peers = []*replicator{
			{replicated: 1},
			{replicated: 2},
			{replicated: 3},
			{replicated: 4},
		}
		l.worker.state.committed = 0

		l.UpdateCommittedIndex()
		rq.Equal(3, l.worker.state.GetCommitted())
	})

	t.Run("should update committed index, 3 total", func(t *testing.T) {
		l.peers = []*replicator{
			{replicated: 1},
			{replicated: 2},
		}
		l.worker.state.committed = 0

		l.UpdateCommittedIndex()
		rq.Equal(2, l.worker.state.GetCommitted())
	})
}
