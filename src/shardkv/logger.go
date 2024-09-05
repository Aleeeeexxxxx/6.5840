package shardkv

import (
	"6.5840/kvraft"

	"go.uber.org/zap"
)

func GetShardKVLoggerOrPanic(component string) *zap.Logger {
	return kvraft.GetLoggerOrPanic("shardKV-" + component)
}

const (
	LogShardKVGid = "gid"
	LogMe         = "me"
)
