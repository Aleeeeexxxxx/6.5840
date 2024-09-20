package shardkv

import (
	"testing"

	"6.5840/shardctrler"
	"github.com/stretchr/testify/require"
)

func TestShardsManager_UpdateCfg(t *testing.T) {
	rq := require.New(t)
	sm := MakeShardsManager(1)

	// join
	ok := sm.HandleUpdateConfig(&shardctrler.Config{
		Num: 1,
		Shards: [shardctrler.NShards]int{
			1, 2, 2, 2, 2, 2, 2, 2, 2, 2,
		},
	})
	rq.True(ok)

	rq.NotNil(sm.appliedCfg)
	rq.Equal(1, sm.appliedCfg.Num)

	rq.Equal(1, len(sm.shards))
	rq.Equal(0, sm.shards[0].ShardId)
	rq.Equal(0, len(sm.shards[0].Seq))

	// assigned to a new shard
	ok = sm.HandleUpdateConfig(&shardctrler.Config{
		Num: 2,
		Shards: [shardctrler.NShards]int{
			1, 1, 2, 2, 2, 2, 2, 2, 2, 2,
		},
	})
	rq.True(ok)

	rq.NotNil(sm.appliedCfg)
	rq.Equal(2, sm.appliedCfg.Num)

	rq.Equal(2, len(sm.shards))

	rq.Equal(0, sm.shards[0].ShardId)
	rq.Equal(0, len(sm.shards[0].Seq))

	rq.Equal(1, sm.shards[1].ShardId)
	rq.Equal(2, len(sm.shards[1].Seq))
	rq.Equal(2, sm.shards[1].Seq[0].CfgNum)
	rq.Equal(2, sm.shards[1].Seq[0].Peer)
	rq.Equal(OpPulling, sm.shards[1].Seq[0].Op)
	rq.Equal(OpCommitting, sm.shards[1].Seq[1].Op)

	// removed assignment
	ok = sm.HandleUpdateConfig(&shardctrler.Config{
		Num: 3,
		Shards: [shardctrler.NShards]int{
			1, 3, 2, 2, 2, 2, 2, 2, 2, 2,
		},
	})
	rq.True(ok)

	rq.NotNil(sm.appliedCfg)
	rq.Equal(3, sm.appliedCfg.Num)

	rq.Equal(2, len(sm.shards))

	rq.Equal(0, sm.shards[0].ShardId)
	rq.Equal(0, len(sm.shards[0].Seq))

	rq.Equal(1, sm.shards[1].ShardId)
	rq.Equal(3, len(sm.shards[1].Seq))
	// seq 1
	rq.Equal(2, sm.shards[1].Seq[0].CfgNum)
	rq.Equal(2, sm.shards[1].Seq[0].Peer)
	rq.Equal(OpPulling, sm.shards[1].Seq[0].Op)
	// seq 2
	rq.Equal(2, sm.shards[1].Seq[1].CfgNum)
	rq.Equal(2, sm.shards[1].Seq[1].Peer)
	rq.Equal(OpCommitting, sm.shards[1].Seq[1].Op)
	// seq 3
	rq.Equal(3, sm.shards[1].Seq[2].CfgNum)
	rq.Equal(3, sm.shards[1].Seq[2].Peer)
	rq.Equal(OpWaitForPull, sm.shards[1].Seq[2].Op)

	// no related change
	ok = sm.HandleUpdateConfig(&shardctrler.Config{
		Num: 4,
		Shards: [shardctrler.NShards]int{
			1, 3, 2, 2, 2, 3, 2, 2, 2, 2,
		},
	})
	rq.True(ok)

	rq.NotNil(sm.appliedCfg)
	rq.Equal(4, sm.appliedCfg.Num)

	rq.Equal(2, len(sm.shards))
	rq.Equal(0, len(sm.shards[0].Seq))
	rq.Equal(3, len(sm.shards[1].Seq))
}

func TestShardsManager_ShardOp(t *testing.T) {
	rq := require.New(t)
	sm := MakeShardsManager(1)

	// join
	ok := sm.HandleUpdateConfig(&shardctrler.Config{
		Num: 1,
		Shards: [shardctrler.NShards]int{
			1, 2, 2, 2, 2, 2, 2, 2, 2, 2,
		},
	})
	rq.True(ok)

	// assigned to a new shard
	ok = sm.HandleUpdateConfig(&shardctrler.Config{
		Num: 2,
		Shards: [shardctrler.NShards]int{
			1, 1, 2, 2, 2, 2, 2, 2, 2, 2,
		},
	})
	rq.True(ok)

	// add a shard
	ok = sm.HandleAddShard(1, 2)
	rq.True(ok)

	ok = sm.HandleCommitShard(1, 2)
	rq.True(ok)

	rq.Equal(0, len(sm.shards[1].Seq))
	rq.Equal(2, sm.shards[1].AppliedCfgNum)

	// removed assignment
	ok = sm.HandleUpdateConfig(&shardctrler.Config{
		Num: 3,
		Shards: [shardctrler.NShards]int{
			1, 3, 2, 2, 2, 2, 2, 2, 2, 2,
		},
	})
	rq.True(ok)

	// remove a shard
	ok = sm.HandleRemoveShard(1, 3)
	rq.True(ok)

	rq.Nil(sm.shards[1])
}

func TestShardsManager_Serialize(t *testing.T) {
	rq := require.New(t)
	sm := MakeShardsManager(1)

	sm.appliedCfg = &shardctrler.Config{
		Num: 2,
	}
	sm.shards = map[int]*Shard{
		1: {
			ShardId:       1,
			AppliedCfgNum: 1,
			Seq: []*ShardOp{
				{
					CfgNum: 1,
					Op:     "string",
					Peer:   1,
				},
			},
		},
	}

	data := sm.Serialize()

	sm.appliedCfg = nil
	sm.shards = nil

	sm.Deserialize(data)

	rq.NotNil(sm.appliedCfg)
	rq.Equal(2, sm.appliedCfg.Num)
	rq.Equal(1, len(sm.shards))
	rq.Equal(1, sm.shards[1].ShardId)
	rq.Equal(1, sm.shards[1].AppliedCfgNum)
	rq.Equal(1, len(sm.shards[1].Seq))
	rq.Equal(1, sm.shards[1].Seq[0].CfgNum)
}
