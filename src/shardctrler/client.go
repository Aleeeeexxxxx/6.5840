package shardctrler

//
// Shardctrler clerk.
//

import (
	"fmt"

	"6.5840/kvraft"
	"6.5840/labrpc"
)

type Clerk struct {
	kvclerk *kvraft.Clerk
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.kvclerk = kvraft.MakeClerk(servers)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	val := ck.kvclerk.Get(fmt.Sprintf("%d", num))

	if len(val) == 0 {
		return *GetDefaultConfig()
	}

	var cfg Config
	MustJsonUnmarshal(val, &cfg)

	return cfg
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{
		Servers: servers,
	}
	ck.kvclerk.Put(OpJoin, JsonStringfyOrPanic(args))
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{
		GIDs: gids,
	}
	ck.kvclerk.Put(OpLeave, JsonStringfyOrPanic(args))
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{
		Shard: shard,
		GID:   gid,
	}
	ck.kvclerk.Put(OpMove, JsonStringfyOrPanic(args))
}
