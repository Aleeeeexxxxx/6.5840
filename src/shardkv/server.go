package shardkv

import (
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
}

func (kv *ShardKV) handleShardKVCtrlOp(op *kvraft.Op, st *kvraft.DataStorage) *kvraft.Op {
	if op.Op == "Get" {
		panic("ShardKV ctrl op should not be Get op")
	}

	switch op.Key {
	case ShardKvUpdateConfig:
		kv.handleUpdateConfigOp(op, st)
	case ShardKvAddShard:
		kv.handleShardKvAddShard(op, st)
	case ShardKvRemoveShard:
		kv.handleShardKvRemoveShard(op, st)
	}

	return op
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

	op.Key = ShardKVConfigPrefix + cfg.NumString()
}

func (kv *ShardKV) handleShardKvAddShard(op *kvraft.Op, st *kvraft.DataStorage) {
	var val ShardOpValue
	if err := json.NewDecoder(strings.NewReader(op.Value)).Decode(&val); err != nil {
		panic(err)
	}

	if ok := kv.shardMngr.HandleAddShard(val.ShardID, val.CfgNum); !ok {
		op.Key = dummy
		op.Value = dummy
		return
	}

	st.PutNoLock(fmt.Sprintf("%s%d", ShardKVShardDataPrefix, val.ShardID), val.Data)

	op.Key = fmt.Sprintf("%s%d", ShardKVShardStatusPrefix, val.ShardID)
	op.Value = fmt.Sprintf("%d", val.CfgNum)
}

func (kv *ShardKV) handleShardKvRemoveShard(op *kvraft.Op, st *kvraft.DataStorage) {
	var val ShardOpValue
	if err := json.NewDecoder(strings.NewReader(op.Value)).Decode(&val); err != nil {
		panic(err)
	}

	if ok := kv.shardMngr.HandleAddShard(val.ShardID, val.CfgNum); !ok {
		op.Key = dummy
		op.Value = dummy
		return
	}

	st.DeleteNoLock(fmt.Sprintf("%s%d", ShardKVShardDataPrefix, val.ShardID))

	op.Key = fmt.Sprintf("%s%d", ShardKVShardStatusPrefix, val.ShardID)
	op.Value = fmt.Sprintf("%d", val.CfgNum)
}

func (kv *ShardKV) handleCustomerOp(op *kvraft.Op, _ *kvraft.DataStorage) *kvraft.Op {
	key := op.Key
	shard := key2shard(key)

	if !kv.shardMngr.IsSharedOK(shard) {
		op.Key = ShardKVShardUnavailable
		op.Value = ShardKVShardUnavailable
	}

	return op
}

func (kv *ShardKV) Kill() {
	kv.kvServer.Kill()
}

func (kv *ShardKV) KVServerHook(op *kvraft.Op, st *kvraft.DataStorage) *kvraft.Op {
	// apply snapshot
	if st != nil {
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

	kv.shardMngr = MakeShardsManager(ctrlers, gid)
	kv.kvServer = kvraft.MakeKvServer(servers, me, persister, maxraftstate, kv.KVServerHook)

	return kv
}
