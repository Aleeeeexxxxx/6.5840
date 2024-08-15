package kvraft

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

func GetBaseLogger() (*zap.Logger, error) {
	if os.Getenv("MR_PROD") == "true" {
		cfg := zap.NewProductionConfig()
		cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
		return cfg.Build()
	} else {
		cfg := zap.NewDevelopmentConfig()
		cfg.EncoderConfig.StacktraceKey = ""
		return cfg.Build()
	}
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

const LoggerComponent = "component"

const (
	LogClerkID   = "clerk"
	LogMessageID = "msgID"
	LogCMDIndex  = "cmdIndex"
	LogKey       = "key"
	LogValue     = "value"
	LogLeaderID  = "leaderID"
)
