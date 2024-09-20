package shardkv

import "6.5840/raft"

type Watcher struct {
	stopCh chan struct{}
	rf     *raft.Raft

	jobs map[int]struct{} // shard id -> job
}

func NewWatcher(rf *raft.Raft) *Watcher {
	return &Watcher{
		stopCh: make(chan struct{}, 1),
		rf:     rf,
		jobs:   make(map[int]struct{}),
	}
}

func (w *Watcher) Stop() {
	w.stopCh <- struct{}{}
	close(w.stopCh)
}
