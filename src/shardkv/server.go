package shardkv

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"6.5840/kvraft"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"go.uber.org/zap"
)

type ShardKV struct {
	me       int
	make_end func(string) *labrpc.ClientEnd

	shardMngr *ShardsManager
	kvServer  *kvraft.KVServer
	logger    *zap.Logger

	leaderDaemonCancelFn context.CancelFunc
}

func (kv *ShardKV) handleShardKVCtrlOp(op *kvraft.Op, st *kvraft.DataStorage) *kvraft.Op {
	if op.Op == "Get" {
		panic("ShardKV ctrl op should not be Get op")
	}

	switch op.Key {
	case ShardKvUpdateConfig:
		kv.handleUpdateConfigOp(op, st)

	case ShardKvAddShard:
		kv.handleShardKvCtrlOp(op, st,
			kv.shardMngr.HandleAddShard,
			func(val ShardOpValue, st *kvraft.DataStorage) {
				st.PutNoLock(fmt.Sprintf("%s%d", ShardKVShardDataPrefix, val.ShardID), val.Data)
			},
		)
	case ShardKvRemoveShard:
		kv.handleShardKvCtrlOp(op, st,
			kv.shardMngr.HandleAddShard,
			func(val ShardOpValue, st *kvraft.DataStorage) {
				st.DeleteNoLock(fmt.Sprintf("%s%d", ShardKVShardDataPrefix, val.ShardID))
			},
		)
	case ShardKvCommitPulling:
		kv.handleShardKvCtrlOp(op, st, kv.shardMngr.HandleCommitShard, nil)
	default:
		panic("unknown shard kv ctrl op")
	}

	return op
}

func (kv *ShardKV) handleShardKvCtrlOp(
	op *kvraft.Op,
	st *kvraft.DataStorage,
	shareMngrHandler func(shardId int, cfgNum int) bool,
	storageHandler func(val ShardOpValue, st *kvraft.DataStorage),
) {
	var val ShardOpValue
	shardctrler.MustJsonUnmarshal(op.Value, &val)

	if shareMngrHandler(val.ShardID, val.CfgNum) {
		storageHandler(val, st)

		op.Key = ShardKVShardStatus
		op.Value = kv.shardMngr.Serialize()
	} else {
		op.Key = dummy
		op.Value = dummy
	}
}

func (kv *ShardKV) handleUpdateConfigOp(op *kvraft.Op, _ *kvraft.DataStorage) {
	var cfg shardctrler.Config
	if err := json.NewDecoder(strings.NewReader(op.Value)).Decode(&cfg); err != nil {
		panic(err)
	}
	if ok := kv.shardMngr.HandleUpdateConfig(&cfg); !ok {
		op.Key = dummy
		op.Value = dummy
		return
	}

	op.Key = ShardKVShardStatus
	op.Value = kv.shardMngr.Serialize()
}

func (kv *ShardKV) handleCustomerOp(op *kvraft.Op, st *kvraft.DataStorage) *kvraft.Op {
	key := op.Key
	shard := key2shard(key)

	if !kv.shardMngr.IsSharedOK(shard) {
		op.Key = ShardKVShardUnavailable
		op.Value = ShardKVShardUnavailable
		return op
	}

	op.Key = ShardKVShardDataPrefix + fmt.Sprintf("%d", shard)

	if op.Op == "Get" {
		return op
	}

	data := ShardData{}
	raw, ok := st.GetNoLock(op.Key)
	if ok {
		if err := json.Unmarshal([]byte(raw), &data); err != nil {
			panic(err)
		}
	}

	switch op.SubOp {
	case "Put":
		data[key] = op.Value
	case "Append":
		old := data[key]
		data[key] = old + op.Value
	}

	b, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	op.Value = string(b)

	return op
}

func (kv *ShardKV) Kill() {
	kv.kvServer.Kill()
	if kv.leaderDaemonCancelFn != nil {
		kv.leaderDaemonCancelFn()
	}
}

func (kv *ShardKV) KVServerHook(op *kvraft.Op, st *kvraft.DataStorage) *kvraft.Op {
	// apply snapshot
	if st != nil {
		if val, ok := st.GetNoLock(ShardKVShardStatus); ok {
			kv.shardMngr.Deserialize(val)
		}
		return nil
	}

	// apply command
	if strings.HasPrefix(op.Key, ShardKVCtrlPrefix) {
		return kv.handleShardKVCtrlOp(op, st)
	}
	return kv.handleCustomerOp(op, st)
}

func StartServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int,
	gid int,
	ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd,
) *ShardKV {

	kv := new(ShardKV)
	kv.me = me
	kv.make_end = make_end

	kv.logger = GetShardKVLoggerOrPanic("server").
		With(zap.Int(LogMe, me)).
		With(zap.Int(LogShardKVGid, gid))

	kv.kvServer = kvraft.MakeKvServer(servers, me, persister, maxraftstate, kv.KVServerHook)
	kv.shardMngr = MakeShardsManager(gid, make_end, shardctrler.MakeClerk(ctrlers), kv.kvServer.Raft())

	kv.kvServer.Raft().SetRoleChangeHook(func(role raft.RoleType) {
		if kv.leaderDaemonCancelFn != nil {
			kv.leaderDaemonCancelFn()
			kv.leaderDaemonCancelFn = nil
		}

		if role == raft.RoleLeader {
			kv.logger.Info("start leader hook")
			ctx, cancel := context.WithCancel(context.Background())
			kv.leaderDaemonCancelFn = cancel

			kv.shardMngr.runLeaderDaemon(ctx)
		}
	})

	return kv
}
