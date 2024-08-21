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
	id                int32
	leaderID          int
	currentMessageID  int64
	servers           []*labrpc.ClientEnd
	realLeaderIDs     []int
	singleRequestLock sync.Mutex
	logger            *zap.Logger
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	svrs, realLeaderIDs := random_handles(servers)
	servers = svrs

	ck := new(Clerk)
	ck.servers = servers
	ck.id = atomic.AddInt32(&globalClerkInstanceID, 1)
	ck.leaderID = int(nrand()) % len(servers)
	ck.logger = GetKVClientLoggerOrPanic("client").With(zap.Int32(LogClerkID, ck.id))
	ck.currentMessageID = 0
	ck.realLeaderIDs = realLeaderIDs

	ck.logger.Info("create new client", zap.Int32(LogClerkID, ck.id))
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
	ck.singleRequestLock.Lock()
	defer ck.singleRequestLock.Unlock()

	metadata := ck.metadata()
	logger := ck.logger.
		With(zap.Int64(LogMessageID, metadata.MessageID))
	defer logger.Sync()

	logger.Info(
		"submit client op: Get",
		zap.String(LogKey, key),
		zap.Int(LogLeaderID, ck.getCurrentLeaderID()),
	)

	args := GetArgs{
		Key:      key,
		Metadata: metadata,
	}
	reply := &GetReply{}

	ck.call("KVServer.Get", args, reply, logger)

	logger.Info(
		"client op: Get succeed",
		zap.String("result", reply.Value),
		zap.Int(LogLeaderID, ck.getCurrentLeaderID()),
	)
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
	ck.singleRequestLock.Lock()
	defer ck.singleRequestLock.Unlock()

	metadata := ck.metadata()
	logger := ck.logger.
		With(zap.Int64(LogMessageID, metadata.MessageID))
	defer logger.Sync()

	logger.Info(
		"submit client op, PutAppend",
		zap.String(LogKey, key),
		zap.String(LogValue, value),
		zap.String("op", "PutAppend"),
		zap.String("subOp", op),
		zap.Int(LogLeaderID, ck.getCurrentLeaderID()),
	)

	args := PutAppendArgs{
		Key:      key,
		Metadata: metadata,
		Op:       op,
		Value:    value,
	}
	reply := &PutAppendReply{}

	ck.call("KVServer.PutAppend", args, reply, logger)
	logger.Info(
		"client op: PutAppend succeed",
		zap.Int(LogLeaderID, ck.getCurrentLeaderID()),
	)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) call(method string, args Args, reply Reply, logger *zap.Logger) {
	count := 0

	for {
		count++
		subLogger := logger.
			With(zap.Int("count", count)).
			With(zap.Int(LogLeaderID, ck.getCurrentLeaderID()))

		subLogger.Info(
			"call rpc",
			zap.String("method", method),
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
			subLogger.Warn("timeout waiting for rpc call, will retry")
		case ok := <-ch:
			subLogger.Debug("got rpc response", zap.String("reply", reply.String()))
			if ok {
				if reply.Accept() {
					subLogger.Info("call rpc successfully")
					return
				} else {
					subLogger.Info("wrong leader, will retry")
				}
			} else {
				subLogger.Warn("network issue, will retry")
			}
		}

		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		subLogger.Debug("choose new leader")
	}
}

func (ck *Clerk) metadata() Metadata {
	return Metadata{
		ClerkID:   ck.id,
		MessageID: atomic.AddInt64(&ck.currentMessageID, 1),
	}
}

func (ck *Clerk) getCurrentLeaderID() int {
	return ck.realLeaderIDs[ck.leaderID]
}
