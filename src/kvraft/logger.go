package kvraft

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

func GetBaseLogger() (*zap.Logger, error) {
	cfg := zap.NewDevelopmentConfig()

	if os.Getenv("RAFT_PROD") == "true" {
		cfg.Level = zap.NewAtomicLevelAt(zap.FatalLevel)
	} else {
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		cfg.DisableStacktrace = true
	}

	return cfg.Build()
}

func GetLogger(component string) (*zap.Logger, error) {
	base, err := GetBaseLogger()
	if err != nil {
		return nil, fmt.Errorf("fail to get base logger, %w", err)
	}
	return base.With(zap.String(LoggerComponent, component)), nil
}

func GetLoggerOrPanic(component string) *zap.Logger {
	logger, err := GetLogger(component)
	if err != nil {
		panic(err)
	}
	return logger
}

func GetKVServerLoggerOrPanic(component string) *zap.Logger {
	return GetLoggerOrPanic("kvserver-" + component)
}

func GetKVClientLoggerOrPanic(component string) *zap.Logger {
	return GetLoggerOrPanic("kvclient-" + component)
}

func GetTestLoggerOrPanic(component string) *zap.Logger {
	return GetLoggerOrPanic("test-" + component)
}

const LoggerComponent = "component"

const (
	LogClerkID   = "clerk"
	LogMessageID = "msgID"
	LogCMDIndex  = "cmdIndex"
	LogKey       = "key"
	LogValue     = "value"
	LogLeaderID  = "leaderID"
)
