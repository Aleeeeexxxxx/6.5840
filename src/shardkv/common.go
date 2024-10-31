package shardkv

import "strings"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	ShardKVCtrlPrefix = "@shardkv//"

	/* key for kvraft.Op */
	ShardKvUpdateConfig = ShardKVCtrlPrefix + "cfg"      // -> config json
	ShardKvAddShard     = ShardKVCtrlPrefix + "addshard" // -> fully data of shard
	ShardKvRemoveShard  = ShardKVCtrlPrefix + "rmshard"

	/* k/v in storage */
	dummy     = ShardKVCtrlPrefix + "dummy"
	Seperator = ".."

	// shard status, data
	ShardKVShardDataPrefix = ShardKVCtrlPrefix + "data" + Seperator //
	ShardKVShardStatus     = ShardKVCtrlPrefix + "status"

	// shard status, for rpc response
	ShardKVShardUnavailable = ShardKVCtrlPrefix + "unavailable"
)

func IsShardKVCtrlOp(op string) bool {
	return strings.HasPrefix(op, ShardKVCtrlPrefix)
}

type ShardOpValue struct {
	CfgNum  int
	ShardID int
	Data    string
}

type ShardData map[string]string
