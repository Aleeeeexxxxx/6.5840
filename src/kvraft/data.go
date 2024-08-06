package kvraft

import (
	"fmt"
	"sync"

	"go.uber.org/zap"
)

type DataStorage struct {
	mutex            sync.Mutex
	data             map[string]string
	lastAppliedIndex int

	logger *zap.Logger
}

func NewDataStorage(me int) *DataStorage {
	return &DataStorage{
		data:             make(map[string]string),
		lastAppliedIndex: -1,
		logger:           GetLoggerOrPanic("storage").With(zap.Int("me", me)),
	}
}

func (st *DataStorage) ApplyCommand(index int, command *Op) (string, Err) {
	logger := st.logger.
		With(zap.Int("cmd index", index)).
		With(zap.Int32("clerk", command.metadata.ClerkID)).
		With(zap.Int64("msgID", command.metadata.MessageID))
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

	if command.Op == "Put" {
		switch command.SubOp {
		case "Append":
			st.putAppend(command.Key, command.Value)
		case "Put":
			st.put(command.Key, command.Value)
		}
	}

	logger.Info("command applied", zap.String("value", st.data[command.Key]))
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
		st.data[key] = fmt.Sprintf("%s%s", val, v)
	}
}
