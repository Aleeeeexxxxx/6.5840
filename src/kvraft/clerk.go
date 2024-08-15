package kvraft

import (
	"sync"

	"go.uber.org/zap"
)

type Client struct {
	messageID int64
	value     string
}

type ClerkStorage struct {
	mutex  sync.RWMutex
	data   map[int32]*Client
	logger *zap.Logger
}

func NewClerkStorage(me int) *ClerkStorage {
	return &ClerkStorage{
		data:   make(map[int32]*Client),
		logger: GetLoggerOrPanic("clerk storage").With(zap.Int("me", me)),
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
		LastCommittedMsgID := c.messageID
		logger = logger.With(zap.Int64("LastCommittedMsgID", LastCommittedMsgID))

		if metadata.MessageID == LastCommittedMsgID {
			logger.Info("msgID equals to committed")
			return c.value, true
		} else if metadata.MessageID < LastCommittedMsgID {
			logger.Info("msgID smaller than committed")
			return c.value, true
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
		if c.messageID >= op.Metadata.MessageID {
			logger.Info(
				"op message id is smaller than current, skip append",
				zap.Int64(LogMessageID, c.messageID),
			)
			return
		}
	} else {
		c = &Client{}
		cm.data[op.Metadata.ClerkID] = c
	}

	c.messageID = op.Metadata.MessageID
	c.value = op.Value
	logger.Info("new op appended")
}
