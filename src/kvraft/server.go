package kvraft

import (
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"go.uber.org/zap"
)

type Op struct {
	Op       string
	SubOp    string
	Key      string
	Value    string
	metadata Metadata
}

type KVServer struct {
	me     int
	logger *zap.Logger

	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big

	dead     int32 // set by Kill()
	requests *RequestMngr

	clients *ClerkStorage
	storage *DataStorage
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.logger = GetLoggerOrPanic("kv server").With(zap.Int("me", me))

	kv.clients = NewClerkStorage(me)
	kv.requests = NewRequestMngr(me)
	kv.storage = NewDataStorage(me)

	go kv.listen()
	return kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		metadata: args.Metadata,
		Op:       "Get",
		SubOp:    "",
		Key:      args.Key,
		Value:    "",
	}
	reply.Value, reply.Err = kv.handleOp(&op)
	reply.Metadata = args.Metadata
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		metadata: args.Metadata,
		Op:       "Put",
		SubOp:    args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	_, reply.Err = kv.handleOp(&op)
	reply.Metadata = args.Metadata
}

func (kv *KVServer) handleOp(op *Op) (string, Err) {
	logger := kv.logger.
		With(zap.Int32(LogClerkID, op.metadata.ClerkID)).
		With(zap.Int64(LogMessageID, op.metadata.MessageID)).
		With(zap.String("op", op.Op)).
		With(zap.String("subop", op.SubOp))
	logger.Info(
		"handle op",
		zap.String(LogKey, op.Key),
		zap.String(LogValue, op.Value),
	)

	if kv.killed() {
		logger.Info("rejected, server stopped")
		return "", ErrWrongLeader
	}

	if value, ok := kv.clients.GetOpValue(op.metadata); ok {
		logger.Info(
			"value matched from client storage",
			zap.String("value", value),
		)
		return value, OK
	}

	logger.Info("submit op to raft")
	if _, _, success := kv.rf.Start(*op); !success {
		logger.Info("raft reject the op")
		return "", ErrWrongLeader
	} else {
		logger.Info("op submitted, waiting for apply")
		return kv.requests.Wait(op.metadata)
	}
}

func (kv *KVServer) Kill() {
	kv.logger.Info("kill server")
	atomic.StoreInt32(&kv.dead, 1)

	kv.rf.Kill()
	kv.requests.Release()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) listen() {
	kv.logger.Info("start to listen to raft msg")

	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.listenCommand(msg.CommandIndex, msg.Command.(Op))
		}
	}
}

func (kv *KVServer) listenCommand(index int, op Op) {
	logger := kv.logger.
		With(zap.Int32(LogClerkID, op.metadata.ClerkID)).
		With(zap.Int64(LogMessageID, op.metadata.MessageID)).
		With(zap.Int(LogCMDIndex, index))
	logger.Info("handle raft command msg")

	kv.clients.AppendNewOp(&op)
	value, err := kv.storage.ApplyCommand(index, &op)
	kv.requests.Complete(op.metadata, value, err)
}
