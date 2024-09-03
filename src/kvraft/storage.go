package kvraft

import (
	"encoding/json"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

type Client struct {
	MessageID int64
	Value     string
}

type ClerkStorage struct {
	mutex  sync.RWMutex
	data   map[int32]*Client // ClerkID ->
	logger *zap.Logger
}

func NewClerkStorage(me int) *ClerkStorage {
	return &ClerkStorage{
		data:   make(map[int32]*Client),
		logger: GetKVServerLoggerOrPanic("clerk storage").With(zap.Int("me", me)),
	}
}

func (cm *ClerkStorage) GetOpValue(metadata Metadata) (string, bool) {
	logger := cm.logger.With(
		zap.Int32(LogClerkID, metadata.ClerkID),
		zap.Int64(LogMessageID, metadata.MessageID),
	)
	logger.Info("try to get last commit value")

	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	key := metadata.ClerkID
	c, ok := cm.data[key]
	if ok {
		LastCommittedMsgID := c.MessageID
		logger = logger.With(zap.Int64("LastCommittedMsgID", LastCommittedMsgID))

		if metadata.MessageID == LastCommittedMsgID {
			logger.Info("msgID equals to committed")
			return c.Value, true
		} else if metadata.MessageID < LastCommittedMsgID {
			logger.Info("msgID smaller than committed")
			// reply the value is safe
			// because the request should be discard now
			return c.Value, true
		} else {
			logger.Info("msgID bigger than committed")
		}
	} else {
		logger.Info("miss message")
	}
	return "", false
}

func (cm *ClerkStorage) AppendNewOp(op *Op) {
	logger := cm.logger.With(
		zap.Int32(LogClerkID, op.Metadata.ClerkID),
		zap.Int64(LogMessageID, op.Metadata.MessageID),
	)
	logger.Debug("append new op")

	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	key := op.Metadata.ClerkID
	c, ok := cm.data[key]
	if ok {
		if c.MessageID >= op.Metadata.MessageID {
			logger.Info(
				"op message id is smaller than current, skip append",
				zap.Int64(LogMessageID, c.MessageID),
			)
			return
		}
	} else {
		c = &Client{}
		cm.data[op.Metadata.ClerkID] = c
	}

	c.MessageID = op.Metadata.MessageID
	c.Value = op.Value
	logger.Info("new op appended")
}

func (cm *ClerkStorage) Serialize() []byte {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	data, _ := json.Marshal(cm.data)
	return data
}

func (cm *ClerkStorage) Deserialize(p []byte) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	json.Unmarshal(p, &cm.data)
}

func (cm *ClerkStorage) ForEach(f func(id int32, c *Client)) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	for id, c := range cm.data {
		f(id, c)
	}
}

type Hook func(*Op, *DataStorage) *Op
type DataStorage struct {
	mutex                sync.Mutex
	data                 map[string]string
	lastAppliedIndex     int
	afterAccessCheckHook Hook
	logger               *zap.Logger
}

func NewDataStorage(me int, hook Hook) *DataStorage {
	return &DataStorage{
		data:                 make(map[string]string),
		lastAppliedIndex:     -1,
		logger:               GetKVServerLoggerOrPanic("data storage").With(zap.Int("me", me)),
		afterAccessCheckHook: hook,
	}
}

func (st *DataStorage) ApplyCommand(index int, command *Op) (string, Err) {
	logger := st.logger.
		With(zap.Int("cmd index", index)).
		With(zap.Int32("clerk", command.Metadata.ClerkID)).
		With(zap.Int64("msgID", command.Metadata.MessageID))
	logger.Info(
		"apply command",
		zap.String("op", command.Op),
		zap.String("sub op", command.SubOp),
		zap.String("key", command.Key),
		zap.String("value", command.Value),
	)

	st.mutex.Lock()
	defer st.mutex.Unlock()

	if index <= st.lastAppliedIndex {
		logger.Warn(
			"command ignored, has been applied",
			zap.Int("op index", st.lastAppliedIndex),
		)
		return "", ErrDuplicateReq
	}

	st.lastAppliedIndex = index

	if st.afterAccessCheckHook != nil {
		command = st.afterAccessCheckHook(command, st)
	}

	if command.Op == "Put" {
		switch command.SubOp {
		case "Append":
			logger.Debug(fmt.Sprintf("Append [%s] to [%s]", command.Value, command.Key))
			st.putAppend(command.Key, command.Value)
		case "Put":
			logger.Debug(fmt.Sprintf("Put [%s] to [%s]", command.Value, command.Key))
			st.put(command.Key, command.Value)
		}
	}

	logger.Info("command applied", zap.String("value", st.data[command.Key]))
	// st.LogStatusOfStorage(logger)

	val, ok := st.data[command.Key]
	if !ok {
		return "", ErrNoKey
	}
	return val, OK
}

func (st *DataStorage) put(key, val string) {
	st.data[key] = val
}

func (st *DataStorage) putAppend(key, val string) {
	v, ok := st.data[key]
	if !ok {
		st.data[key] = val
	} else {
		st.data[key] = fmt.Sprintf("%s%s", v, val)
	}
}

func (st *DataStorage) LogStatusOfStorage(logger *zap.Logger) {
	data, _ := json.Marshal(st.data)
	logger.Info(
		"storage status",
		zap.String("data", string(data)),
		zap.Int("last applied index", st.lastAppliedIndex),
	)
}

func (st *DataStorage) Serialize() ([]byte, int) {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	st.logger.Debug(
		"serialize data storage",
		zap.Int("last applied index", st.lastAppliedIndex),
	)

	data, _ := json.Marshal(st.data)
	return data, st.lastAppliedIndex
}

func (st *DataStorage) Map(f func(string, string)) {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	for k, v := range st.data {
		f(k, v)
	}
}

func (st *DataStorage) Deserialize(index int, p []byte) {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	st.logger.Debug(
		"serialize data storage",
		zap.Int("last applied index", index),
	)

	json.Unmarshal(p, &st.data)
	st.lastAppliedIndex = index

	if st.afterAccessCheckHook != nil {
		st.afterAccessCheckHook(nil, st)
	}
}
