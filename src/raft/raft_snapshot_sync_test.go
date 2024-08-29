package raft

import (
	"sync"
	"testing"
)

func TestRaft_SnapshotSync(t *testing.T) {
	rf := &Raft{
		buildSnapshotCh: make(chan *BuildSnapshotTask),
		stopCh:          make(chan struct{}),
		logger:          GetLoggerOrPanic("TestRaft_SnapshotSync"),
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-rf.stopCh:
				return
			case task := <-rf.buildSnapshotCh:
				task.wg.Done()
			}
		}
	}()

	// async
	rf.Snapshot(1, []byte("snapshot"))

	// sync
	task := rf.Snapshot(1, []byte("snapshot"))
	task.wg.Wait()

	// rf stopped
	close(rf.stopCh)
	task = rf.Snapshot(1, []byte("snapshot"))
	task.wg.Wait()

	wg.Wait()
}
