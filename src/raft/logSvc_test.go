package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogSvc(t *testing.T) {
	// ls := NewLogService(0)
}

func TestLogStorage_Snapshot(t *testing.T) {
	rq := require.New(t)

	t.Run("no snapshot, should save", func(t *testing.T) {
		st := new(Storage)

		err := st.SaveSnapshot(&Snapshot{
			LastLogIndex: 2,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("data"),
		})

		rq.NoError(err)
	})

	t.Run("snapshot exists, should skip", func(t *testing.T) {
		st := new(Storage)
		st.Snapshot = &Snapshot{
			LastLogIndex: 2,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("data"),
		}

		err := st.SaveSnapshot(&Snapshot{
			LastLogIndex: 1,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("skip"),
		})

		rq.Equal(err, errorSnapshotExists)
		rq.Equal(string(st.Snapshot.Data), "data")
	})

	t.Run("snapshot exists, should overwrite", func(t *testing.T) {
		st := new(Storage)
		st.Snapshot = &Snapshot{
			LastLogIndex: 2,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("data"),
		}

		err := st.SaveSnapshot(&Snapshot{
			LastLogIndex: 4,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("overwrite"),
		})

		rq.NoError(err)
		rq.Equal(string(st.Snapshot.Data), "overwrite")
	})

	t.Run("log index smaller than snapshot, trim all", func(t *testing.T) {
		st := new(Storage)
		st.Logs = Logs{
			{Index: 1},
			{Index: 2},
			{Index: 3},
		}

		err := st.SaveSnapshot(&Snapshot{
			LastLogIndex: 4,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("snapshot"),
		})

		rq.NoError(err)
		rq.Equal(string(st.Snapshot.Data), "snapshot")
		rq.Equal(0, len(st.Logs))
	})

	t.Run("log index equals to snapshot, trim all", func(t *testing.T) {
		st := new(Storage)
		st.Logs = Logs{
			{Index: 1},
			{Index: 2},
			{Index: 3},
			{Index: 4},
		}

		err := st.SaveSnapshot(&Snapshot{
			LastLogIndex: 4,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("snapshot"),
		})

		rq.NoError(err)
		rq.Equal(string(st.Snapshot.Data), "snapshot")
		rq.Equal(0, len(st.Logs))
	})

	t.Run("log index bigger than snapshot, trim some", func(t *testing.T) {
		st := new(Storage)
		st.Logs = Logs{
			{Index: 1},
			{Index: 2},
			{Index: 3},
			{Index: 4},
			{Index: 5},
		}

		err := st.SaveSnapshot(&Snapshot{
			LastLogIndex: 4,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("snapshot"),
		})

		rq.NoError(err)
		rq.Equal(string(st.Snapshot.Data), "snapshot")
		rq.Equal(1, len(st.Logs))
	})

	t.Run("log index bigger than snapshot, trim logs and discard old snapshot", func(t *testing.T) {
		st := new(Storage)
		st.Logs = Logs{
			{Index: 4},
			{Index: 5},
		}
		st.Snapshot = &Snapshot{
			LastLogIndex: 3,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("data"),
		}

		err := st.SaveSnapshot(&Snapshot{
			LastLogIndex: 4,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("snapshot"),
		})

		rq.NoError(err)
		rq.Equal(string(st.Snapshot.Data), "snapshot")
		rq.Equal(1, len(st.Logs))
	})

	t.Run("snapshot index smaller than old snapshot, skip", func(t *testing.T) {
		st := new(Storage)
		st.Logs = Logs{
			{Index: 4},
			{Index: 5},
		}
		st.Snapshot = &Snapshot{
			LastLogIndex: 3,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("data"),
		}

		err := st.SaveSnapshot(&Snapshot{
			LastLogIndex: 2,
			LastLogTerm:  3,
			BuildTerm:    3,
			Data:         []byte("snapshot"),
		})

		rq.Equal(err, errorSnapshotExists)
		rq.Equal(string(st.Snapshot.Data), "data")
		rq.Equal(2, len(st.Logs))
	})
}
