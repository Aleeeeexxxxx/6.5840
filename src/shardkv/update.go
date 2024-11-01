package shardkv

import (
	"context"
	"fmt"
	"sync"
	"time"

	"6.5840/kvraft"
	"6.5840/shardctrler"
)

func (sm *ShardsManager) runLeaderDaemon(ctx context.Context) {
	sm.loopUpdateCfg(ctx)
	sm.loopSyncShards(ctx)
}

func shardConfigToOp(cfg *shardctrler.Config) *kvraft.Op {
	return &kvraft.Op{
		Op:    "Put",
		Key:   ShardKvUpdateConfig,
		Value: shardctrler.JsonStringfyOrPanic(cfg),
	}
}

func (sm *ShardsManager) loopUpdateCfg(ctx context.Context) {
	next := 1

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if sm.appliedCfg != nil {
		next = sm.appliedCfg.Num + 1
	}

	go loop(ctx, sm.cfgQueryInterval, func() bool {
		cfg := sm.cfgCilent.Query(next)
		if cfg.Num == next {
			op := shardConfigToOp(&cfg)
			if _, _, ok := sm.raft.Start(op); ok {
				next++
				return true
			}
		}

		return false
	})
}

func (sm *ShardsManager) loopSyncShards(ctx context.Context) {
	var inflights sync.Map

	go loop(ctx, sm.shardSyncInterval, func() bool {
		sm.mutex.Lock()
		defer sm.mutex.Unlock()

		for n, shard := range sm.shards {
			n := n
			if _, ok := inflights.Load(n); !ok {
				op := shard.GetCurrentOp()

				if op == nil || op.Op == OpWaitForPull {
					continue
				}

				inflights.Store(n, struct{}{})

				go func(op ShardOp) {
					defer inflights.Delete(n)

					var raftOp *kvraft.Op
					if op.Op == OpPulling {
						raftOp = sm.syncPull(n, op)
					} else {
						raftOp = sm.syncCommit(n, op)
					}

					sm.raft.Start(raftOp)
				}(*op)
			}
		}

		return false
	})
}

func (sm *ShardsManager) syncPull(shardId int, op ShardOp) *kvraft.Op {
	ck := kvraft.MakeClerk(sm.make_ends(op.Servers))

	raw := ck.Get(fmt.Sprintf("%d", shardId))

	opValue := ShardOpValue{
		CfgNum:  op.CfgNum,
		ShardID: shardId,
		Data:    raw,
	}

	return &kvraft.Op{
		Op:    "Put",
		Key:   ShardKvAddShard,
		Value: shardctrler.JsonStringfyOrPanic(opValue),
	}
}

func (sm *ShardsManager) syncCommit(shardId int, op ShardOp) *kvraft.Op {
	ck := kvraft.MakeClerk(sm.make_ends(op.Servers))

	remoteOpValue := ShardOpValue{
		CfgNum:  op.CfgNum,
		ShardID: shardId,
	}
	ck.Put(ShardKvRemoveShard, shardctrler.JsonStringfyOrPanic(remoteOpValue))

	commitOpValue := ShardOpValue{
		CfgNum:  op.CfgNum,
		ShardID: shardId,
	}

	return &kvraft.Op{
		Op:    "Put",
		Key:   ShardKvCommitPulling,
		Value: shardctrler.JsonStringfyOrPanic(commitOpValue),
	}
}

func loop(ctx context.Context, interval time.Duration, step func() bool) {
	timer := time.NewTicker(interval)

LOOP:
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			defer timer.Reset(interval)

			if shouldContinue := step(); !shouldContinue {
				break LOOP
			}
		}
	}
}

type ShardKVCtrlClerk struct {
}
