package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/kvraft"
	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   *shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	make_ends func([]string) []*labrpc.ClientEnd
	// You will have to modify this struct.
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	return makeClerk(shardctrler.MakeClerk(ctrlers), make_end)
}

func makeClerk(cfgClient *shardctrler.Clerk, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = cfgClient
	ck.config = nil

	ck.make_end = make_end
	ck.make_ends = make_ends(make_end)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	shard := key2shard(key)
	first := true

	for {
		if first && ck.config != nil {
			first = false
		} else {
			cfg := ck.sm.Query(-1)
			ck.config = &cfg
		}

		gid := ck.config.Shards[shard]
		servers := ck.config.Groups[gid]

		value := kvraft.MakeClerk(ck.make_ends(servers)).Get(key)

		if value != ShardKVShardUnavailable {
			var data ShardData
			shardctrler.MustJsonUnmarshal(value, &data)
			return data[key]
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	shard := key2shard(key)
	first := true

	for {
		if first && ck.config != nil {
			first = false
		} else {
			cfg := ck.sm.Query(-1)
			ck.config = &cfg
		}

		gid := ck.config.Shards[shard]
		servers := ck.config.Groups[gid]

		value := kvraft.MakeClerk(ck.make_ends(servers)).PutAppend(key, value, op)

		if value != ShardKVShardUnavailable {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func make_ends(make_end func(string) *labrpc.ClientEnd) func([]string) []*labrpc.ClientEnd {
	return func(servers []string) []*labrpc.ClientEnd {
		var ret []*labrpc.ClientEnd
		for _, server := range servers {
			ret = append(ret, make_end(server))
		}
		return ret
	}
}
