package shardkv

import (
	"testing"

	"6.5840/kvraft"
	"github.com/stretchr/testify/require"
)

func TestShardKV_CustomOp(t *testing.T) {
	rq := require.New(t)

	sm := MakeShardsManager(1)
	sm.shards = map[int]*Shard{
		7: {
			ShardId:       7,
			AppliedCfgNum: 1,
		},
	}

	kv := new(ShardKV)
	kv.shardMngr = sm

	st := kvraft.NewDataStorage(1, nil)

	t.Run("shard unavaiable", func(t *testing.T) {
		op := kv.handleCustomerOp(&kvraft.Op{
			Op:    "Put",
			Key:   "bey",
			SubOp: "Append",
			Value: "value",
		}, st)

		rq.Equal("@shardkv//unavailable", op.Key)
		rq.Equal("@shardkv//unavailable", op.Value)
	})

	t.Run("put, shard ok", func(t *testing.T) {
		op := kv.handleCustomerOp(&kvraft.Op{
			Op:    "Put",
			Key:   "key",
			SubOp: "Append",
			Value: "value",
		}, st)

		rq.Equal("@shardkv//data..7", op.Key)
		rq.Equal("{\"key\":\"value\"}", op.Value)
	})

	t.Run("get, shard ok", func(t *testing.T) {
		op := kv.handleCustomerOp(&kvraft.Op{
			Op:  "Get",
			Key: "aey",
		}, st)

		rq.Equal("@shardkv//data..7", op.Key)
	})
}
