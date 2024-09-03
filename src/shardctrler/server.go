package shardctrler

import (
	"fmt"
	"strconv"

	"6.5840/kvraft"
	"6.5840/labrpc"
	"6.5840/raft"

	"go.uber.org/zap"
)

type ShardCtrler struct {
	rf       *raft.Raft
	kvServer *kvraft.KVServer
	logger   *zap.Logger

	cfg *Config
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.kvServer.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)

	sc.kvServer = kvraft.MakeKvServer(servers, me, persister, 3000, sc.rewriteOp)
	sc.rf = sc.kvServer.Raft()
	sc.logger = GetShardCtrlerLoggerOrPanic("server")
	sc.cfg = GetDefaultConfig()

	return sc
}

func (sc *ShardCtrler) rewriteCommandOp(op *kvraft.Op) *kvraft.Op {
	if op.Op == "Get" {
		if op.Key == QueryLatestConfigNum {
			op.Key = sc.cfg.NumString()
			sc.logger.Info(
				"rewrite to query latest config num",
				zap.String("new key", op.Key),
				zap.Int32(kvraft.LogClerkID, op.Metadata.ClerkID),
				zap.Int64(kvraft.LogMessageID, op.Metadata.MessageID),
			)
		}
		return op
	}

	sc.cfg.Num++
	sc.handleKVRaftOp(op)

	op.Key = sc.cfg.NumString()
	op.Value = JsonStringfyOrPanic(sc.cfg)
	return op
}

func (sc *ShardCtrler) rewriteSnapshotOp(st *kvraft.DataStorage) *kvraft.Op {
	max := 0
	var cfgJsonString string

	st.Map(func(k, v string) {
		num, _ := strconv.Atoi(k)
		if max < num {
			max = num
			cfgJsonString = v
		}
	})

	MustJsonUnmarshal(cfgJsonString, sc.cfg)
	return nil
}

func (sc *ShardCtrler) rewriteOp(op *kvraft.Op, st *kvraft.DataStorage) *kvraft.Op {
	if op != nil {
		return sc.rewriteCommandOp(op)
	}
	return sc.rewriteSnapshotOp(st)
}

func (sc *ShardCtrler) handleKVRaftOp(op *kvraft.Op) {
	sc.logger.Info(
		"handle config change",
		zap.String("type", op.Key),
		zap.Int32(kvraft.LogClerkID, op.Metadata.ClerkID),
		zap.Int64(kvraft.LogMessageID, op.Metadata.MessageID),
	)

	switch op.Key {
	case OpJoin:
		var args JoinArgs
		MustJsonUnmarshal(op.Value, &args)

		for gid, shards := range args.Servers {
			sc.cfg.Groups[gid] = shards
		}

	case OpLeave:
		var args LeaveArgs
		MustJsonUnmarshal(op.Value, &args)

		for _, gid := range args.GIDs {
			delete(sc.cfg.Groups, gid)

			for shard, assignTo := range sc.cfg.Shards {
				if assignTo == gid {
					sc.cfg.Shards[shard] = unassign
				}
			}
		}

	case OpMove:
		var args MoveArgs
		MustJsonUnmarshal(op.Value, &args)

		sc.cfg.Shards[args.Shard] = args.GID
		return
	}

	sc.logger.Debug(
		fmt.Sprintf("config shards before balance, shards=%#v", sc.cfg.Shards),
		zap.Int32(kvraft.LogClerkID, op.Metadata.ClerkID),
		zap.Int64(kvraft.LogMessageID, op.Metadata.MessageID),
	)

	cb := NewConfigBalancer(sc.cfg)
	cb.Balance()

	sc.logger.Debug(
		fmt.Sprintf("config shards after balance, shards=%#v", sc.cfg.Shards),
		zap.Int32(kvraft.LogClerkID, op.Metadata.ClerkID),
		zap.Int64(kvraft.LogMessageID, op.Metadata.MessageID),
	)
}

func GetDefaultConfig() *Config {
	defaultCfg := &Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}

	for i := 0; i < NShards; i++ {
		defaultCfg.Shards[i] = unassign
	}
	return defaultCfg
}
