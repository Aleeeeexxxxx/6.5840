package shardctrler

import (
	"6.5840/kvraft"
	"6.5840/labrpc"
	"6.5840/raft"

	"go.uber.org/zap"
)

const unassign = 0

type ConfigBalancer struct {
	cfg *Config

	assignments map[int][]int // gid -> shards
	unassigned  []int
}

func NewConfigBalancer(cfg *Config) *ConfigBalancer {
	cb := &ConfigBalancer{
		cfg:         cfg,
		assignments: map[int][]int{},
	}
	return cb
}

func (cb *ConfigBalancer) parseAssignments() {
	for gid := range cb.cfg.Groups {
		cb.assignments[gid] = []int{}
	}

	for shard, gid := range cb.cfg.Shards {
		if gid == unassign {
			cb.unassigned = append(cb.unassigned, shard)
		} else {
			cb.assignments[gid] = append(cb.assignments[gid], shard)
		}
	}
}

func (cb *ConfigBalancer) assignUnassigned() {
	if len(cb.assignments) == 0 {
		return
	}

	for len(cb.unassigned) > 0 {
		min := -1
		minGids := []int{}

		for gid, shards := range cb.assignments {
			if min == -1 || len(shards) < min {
				min = len(shards)
				minGids = []int{gid}
			} else if len(shards) == min {
				minGids = append(minGids, gid)
			}
		}

		for _, gid := range minGids {
			if len(cb.unassigned) == 0 {
				break
			}

			shard := cb.unassigned[0]
			cb.unassigned = cb.unassigned[1:]

			cb.assignments[gid] = append(cb.assignments[gid], shard)
		}

	}
}

func (cb *ConfigBalancer) rebalanceAssignments() {
	if len(cb.assignments) == 0 {
		return
	}

	for {
		var maxGid, minGid int
		max := -1
		min := -1

		for gid, shards := range cb.assignments {
			if len(shards) > max {
				max = len(shards)
				maxGid = gid
			}
			if min == -1 || len(shards) < min {
				min = len(shards)
				minGid = gid
			}
		}

		if max-min <= 1 {
			return
		}

		move := cb.assignments[maxGid][len(cb.assignments[maxGid])-1]

		cb.assignments[maxGid] = cb.assignments[maxGid][:len(cb.assignments[maxGid])-1]
		cb.assignments[minGid] = append(cb.assignments[minGid], move)
	}

}

func (cb *ConfigBalancer) Balance() {
	cb.parseAssignments()

	cb.assignUnassigned()
	cb.rebalanceAssignments()

	for gid, shards := range cb.assignments {
		for _, shard := range shards {
			cb.cfg.Shards[shard] = gid
		}
	}
}

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
	sc.cfg = sc.defaultConfig()

	return sc
}

func (sc *ShardCtrler) rewriteOp(op *kvraft.Op) *kvraft.Op {
	if op.Op == "Get" {
		if op.Key == QueryLatestConfigNum {
			op.Key = sc.cfg.NumString()
		}
		return op
	}

	sc.cfg.Num++
	sc.handleKVRaftOp(op)

	op.Key = sc.cfg.NumString()
	op.Value = JsonStringfyOrPanic(sc.cfg)
	return op
}

func (sc *ShardCtrler) handleKVRaftOp(op *kvraft.Op) {
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

	cb := NewConfigBalancer(sc.cfg)
	cb.Balance()
}

func (sc *ShardCtrler) defaultConfig() *Config {
	cfg := &Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}

	for i := 0; i < NShards; i++ {
		cfg.Shards[i] = unassign
	}
	return cfg
}
