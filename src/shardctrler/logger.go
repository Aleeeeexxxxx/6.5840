package shardctrler

import (
	"6.5840/kvraft"

	"go.uber.org/zap"
)

func GetShardCtrlerLoggerOrPanic(component string) *zap.Logger {
	return kvraft.GetLoggerOrPanic("shardCtrler-" + component)
}
