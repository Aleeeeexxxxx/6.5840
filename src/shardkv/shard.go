package shardkv

import (
	"encoding/json"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	OpPulling     = "Pulling"
	OpCommitting  = "Committing" // commit to peer, let them delete the shard
	OpWaitForPull = "Pushing"
)

type ShardOp struct {
	CfgNum  int
	Op      string
	Peer    int      // gid
	Servers []string // gid -> servers
}

type Shard struct {
	ShardId       int
	AppliedCfgNum int
	Seq           []*ShardOp
}

func (s Shard) GetCurrentOp() *ShardOp {
	if len(s.Seq) == 0 {
		return nil
	}
	return s.Seq[0]
}

type ShardsManager struct {
	gid int

	mutex      sync.Mutex
	appliedCfg *shardctrler.Config
	shards     map[int]*Shard // shardId -> Shard

	raft *raft.Raft

	cfgQueryInterval time.Duration
	cfgCilent        *shardctrler.Clerk

	shardSyncInterval time.Duration
	make_ends         func([]string) []*labrpc.ClientEnd
}

func MakeShardsManager(
	gid int,
	make_end func(string) *labrpc.ClientEnd,
	cfgCilent *shardctrler.Clerk,
	raft *raft.Raft,
) *ShardsManager {

	sm := &ShardsManager{
		shards:            make(map[int]*Shard),
		gid:               gid,
		cfgCilent:         cfgCilent,
		cfgQueryInterval:  100 * time.Millisecond,
		make_ends:         make_ends(make_end),
		shardSyncInterval: 100 * time.Millisecond,
		raft:              raft,
	}
	return sm
}

func (sm *ShardsManager) IsSharedOK(id int) bool {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	shard, ok := sm.shards[id]
	if !ok {
		return false
	}
	cur := shard.GetCurrentOp()
	if cur == nil {
		return true
	}
	if len(shard.Seq) == 1 && cur.Op == OpCommitting {
		return true
	}
	return false
}

func (sm *ShardsManager) HandleUpdateConfig(cfg *shardctrler.Config) bool {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if sm.appliedCfg != nil && cfg.Num <= sm.appliedCfg.Num {
		return false
	}

	if sm.appliedCfg != nil && cfg.Num != sm.appliedCfg.Num+1 {
		panic("config number should be continuous")
	}

	defer func() { sm.appliedCfg = cfg }()

	for shardId, curAssignedGid := range cfg.Shards {
		if sm.appliedCfg == nil {
			if curAssignedGid == sm.gid {
				sm.shards[shardId] = &Shard{
					ShardId:       shardId,
					AppliedCfgNum: cfg.Num,
				}
			}
			continue
		}

		lastAssignedGid := sm.appliedCfg.Shards[shardId]

		if lastAssignedGid != curAssignedGid {
			if lastAssignedGid == sm.gid {
				shard, ok := sm.shards[shardId]
				if !ok {
					panic("shard should exist")
				}
				shard.Seq = append(shard.Seq, &ShardOp{
					CfgNum:  cfg.Num,
					Op:      OpWaitForPull,
					Peer:    curAssignedGid,
					Servers: cfg.Groups[curAssignedGid],
				})
			}

			if curAssignedGid == sm.gid {
				shard, ok := sm.shards[shardId]
				if !ok {
					shard = &Shard{
						ShardId:       shardId,
						AppliedCfgNum: cfg.Num - 1,
					}
					sm.shards[shardId] = shard
				}
				shard.Seq = append(
					shard.Seq,
					&ShardOp{
						CfgNum:  cfg.Num,
						Op:      OpPulling,
						Peer:    lastAssignedGid,
						Servers: cfg.Groups[lastAssignedGid],
					},
					&ShardOp{
						CfgNum:  cfg.Num,
						Op:      OpCommitting,
						Peer:    lastAssignedGid,
						Servers: cfg.Groups[lastAssignedGid],
					},
				)
			}
		}
	}

	return true
}

func (sm *ShardsManager) precheckShardOp(id, cfgNum int, targetOp string) (*Shard, bool) {
	shard, ok := sm.shards[id]
	if !ok {
		return nil, false
	}

	if shard.AppliedCfgNum > cfgNum {
		return nil, false
	} else if shard.AppliedCfgNum == cfgNum {
		if targetOp == OpCommitting {

		} else {
			return nil, false
		}
	}

	op := shard.GetCurrentOp()
	if op == nil {
		return nil, false
	}

	if op.CfgNum != cfgNum {
		panic("should wait for the same config")
	}

	if op.Op != targetOp {
		panic("incorrect op")
	}

	shard.AppliedCfgNum = cfgNum
	shard.Seq = shard.Seq[1:]
	return shard, true
}

func (sm *ShardsManager) HandleCommitShard(id, cfgNum int) bool {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	_, ok := sm.precheckShardOp(id, cfgNum, OpCommitting)
	return ok
}

func (sm *ShardsManager) HandleAddShard(id, cfgNum int) bool {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	_, ok := sm.precheckShardOp(id, cfgNum, OpPulling)
	return ok
}

func (sm *ShardsManager) HandleRemoveShard(id, cfgNum int) bool {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	shard, ok := sm.precheckShardOp(id, cfgNum, OpWaitForPull)

	if !ok {
		return false
	}

	if shard.GetCurrentOp() == nil {
		delete(sm.shards, id)
	}
	return true
}

type ShardsManagerState struct {
	AppliedCfg *shardctrler.Config
	Shards     map[int]*Shard // shardId -> Shard
}

func (sm *ShardsManager) Serialize() string {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	state := ShardsManagerState{
		AppliedCfg: sm.appliedCfg,
		Shards:     sm.shards,
	}

	data, _ := json.Marshal(state)
	return string(data)
}

func (sm *ShardsManager) Deserialize(data string) {
	var state ShardsManagerState

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	json.Unmarshal([]byte(data), &state)

	sm.appliedCfg = state.AppliedCfg
	sm.shards = state.Shards
}
