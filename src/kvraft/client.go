package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"go.uber.org/zap"
)

var globalClerkInstanceID int32 = 0

const retryInterval = time.Second

type Clerk struct {
	id       int32
	leaderID int
	servers  []*labrpc.ClientEnd
	mutext   sync.Mutex
	logger   *zap.Logger
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = atomic.AddInt32(&globalClerkInstanceID, 1)
	ck.leaderID = int(nrand()) % len(servers)
	ck.logger = GetLoggerOrPanic("follower").With(zap.Int32("clerk", ck.id))

	ck.logger.Info("create new client")

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mutext.Lock()
	defer ck.mutext.Unlock()

	ck.logger.Info("client op: Get", zap.String("key", key))

	args := &GetArgs{
		Key:      key,
		Metadata: ck.metadata(),
	}
	reply := &GetReply{}

	ck.call("KVServer.Get", args, reply)

	ck.logger.Info("client op: Get", zap.String("result", reply.Value))
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mutext.Lock()
	defer ck.mutext.Unlock()

	ck.logger.Info(
		"client op: PutAppend",
		zap.String(LogKey, key),
		zap.String(LogValue, value),
		zap.String("subOp", op),
	)

	args := &PutAppendArgs{
		Key:      key,
		Metadata: ck.metadata(),
	}
	reply := &PutAppendReply{}

	ck.call("KVServer.PutAppend", args, reply)
	ck.logger.Info("client op: PutAppend succeed", zap.String("op", op))
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) call(method string, args Args, reply Reply) {
	count := 0

	for {
		count++
		ck.logger.Info(
			"call rpc",
			zap.String("method", method),
			zap.Int("count", count),
			zap.Int("leaderID", ck.leaderID),
			zap.String("args", args.String()),
		)

		ch := make(chan bool, 1)
		timer := time.NewTimer(retryInterval)

		go func() {
			c := ck.servers[ck.leaderID]
			select {
			case ch <- c.Call(method, args, reply):
			case <-timer.C:
			}
			close(ch)
		}()

		select {
		case <-timer.C:
			ck.logger.Warn("timeout waiting for rpc call, will retry")
		case ok := <-ch:
			ck.logger.Debug("got rpc response", zap.String("reply", reply.String()))
			if ok && reply.Accept() {
				ck.logger.Info("call rpc successfully")
				return
			} else {
				ck.logger.Info("wrong leader, will retry")
			}
			ck.logger.Warn("network issue, will retry")
		}

		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
	}
}

func (ck *Clerk) metadata() Metadata {
	return Metadata{
		ClerkID:   ck.id,
		MessageID: time.Now().Unix(),
	}
}
