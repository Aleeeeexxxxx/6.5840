package kvraft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"go.uber.org/zap"
)

type Op struct {
	Op    string
	SubOp string
	Key   string
	Value string
	Metadata
}

type KVServer struct {
	me     int
	logger *zap.Logger

	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big

	dead     int32 // set by Kill()
	requests *RequestMngr

	clients   *ClerkStorage
	storage   *DataStorage
	persister *raft.Persister
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.logger = GetKVServerLoggerOrPanic("kv server").With(zap.Int("me", me))

	kv.clients = NewClerkStorage(me)
	kv.requests = NewRequestMngr(me)
	kv.storage = NewDataStorage(me)

	go kv.listen()
	return kv
}

func (kv *KVServer) Get(args GetArgs, reply *GetReply) error {
	op := Op{
		Metadata: args.Metadata,
		Op:       "Get",
		SubOp:    "",
		Key:      args.Key,
		Value:    "",
	}
	reply.Value, reply.Err = kv.handleOp(&op)
	reply.Metadata = args.Metadata
	return nil
}

func (kv *KVServer) PutAppend(args PutAppendArgs, reply *PutAppendReply) error {
	op := Op{
		Metadata: args.Metadata,
		Op:       "Put",
		SubOp:    args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	_, reply.Err = kv.handleOp(&op)
	reply.Metadata = args.Metadata
	return nil
}

func (kv *KVServer) handleOp(op *Op) (string, Err) {
	logger := kv.logger.
		With(zap.Int32(LogClerkID, op.Metadata.ClerkID)).
		With(zap.Int64(LogMessageID, op.Metadata.MessageID)).
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

	if value, ok := kv.clients.GetOpValue(op.Metadata); ok {
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
		return kv.requests.Wait(op.Metadata)
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

			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.logger.Info(
					"RaftStateSize exceed the limit, build snapshot",
					zap.Int("RaftStateSize", kv.persister.RaftStateSize()),
				)
				index, snapshot := kv.buildSnapshot()
				go func() { kv.rf.Snapshot(index, snapshot) }()
			}
		}
		if msg.SnapshotValid {
			kv.installSnapshot(msg.SnapshotIndex, msg.Snapshot)
			kv.handleRequests()
		}
	}
}

func (kv *KVServer) listenCommand(index int, op Op) {
	logger := kv.logger.
		With(zap.Int32(LogClerkID, op.Metadata.ClerkID)).
		With(zap.Int64(LogMessageID, op.Metadata.MessageID)).
		With(zap.Int(LogCMDIndex, index))
	logger.Info("handle raft command msg")

	// prevent duplicate requests
	if val, ok := kv.clients.GetOpValue(op.Metadata); ok {
		logger.Info("msg id is older than current, reply from client storage, skip updating")
		kv.requests.Complete(op.Metadata, val, OK)
		return
	}

	// update data storage first, then clerk storage
	value, err := kv.storage.ApplyCommand(index, &op)
	op.Value = value

	kv.clients.AppendNewOp(&op)
	kv.requests.Complete(op.Metadata, value, err)
}

func (kv *KVServer) buildSnapshot() (int, []byte) {
	data, index := kv.storage.Serialize()
	clients := kv.clients.Serialize()

	buf := make([]byte, len(data)+len(clients)+2*4)

	binary.BigEndian.PutUint32(buf[:4], uint32(len(clients)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(data)))

	copy(buf[8:], clients)
	copy(buf[8+len(clients):], data)

	return index, buf
}

func (kv *KVServer) installSnapshot(index int, data []byte) {
	kv.logger.Info(fmt.Sprintf("install snapshot, index=%d", index))

	reader := bytes.NewReader(data)

	var clientsLen, dataLen uint32
	binary.Read(reader, binary.BigEndian, &clientsLen)
	binary.Read(reader, binary.BigEndian, &dataLen)

	buf := make([]byte, max(clientsLen, dataLen))

	reader.Read(buf[:clientsLen])
	kv.clients.Deserialize(buf[:clientsLen])

	reader.Read(buf[:dataLen])
	kv.storage.Deserialize(index, buf[:dataLen])
}

func (kv *KVServer) handleRequests() {
	kv.clients.ForEach(func(id int32, c *Client) {
		kv.requests.Complete(Metadata{ClerkID: id, MessageID: c.messageID}, c.value, OK)
	})
}

func max(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
