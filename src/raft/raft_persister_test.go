package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaft_Persist(t *testing.T) {
	p := MakePersister()
	rf := Make(nil, 1, p, nil)
	rq := require.New(t)

	rf.persist()
	rf.readPersist(p.ReadRaftState(), nil)
	rq.Nil(rf.state.logMngr.Snapshot)
}
