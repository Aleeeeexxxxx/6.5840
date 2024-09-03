package shardctrler

import (
	"encoding/json"
	"fmt"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

const unassign = 0

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (cfg Config) NumString() string {
	return fmt.Sprintf("%d", cfg.Num)
}

const QueryLatestConfigNum = "-1"

const (
	OpMove  = "move"
	OpJoin  = "join"
	OpLeave = "leave"
)

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}

type LeaveArgs struct {
	GIDs []int
}

type MoveArgs struct {
	Shard int
	GID   int
}

func MustJsonUnmarshal(raw string, v interface{}) {
	if err := json.Unmarshal([]byte(raw), v); err != nil {
		panic(fmt.Errorf("failed to unmarshal [%s]: %w", raw, err))
	}
}

func JsonStringfyOrPanic(v interface{}) string {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(raw)
}
