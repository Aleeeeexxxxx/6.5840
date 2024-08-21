package raft

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
	logger, err := GetLogger("raft-" + component)
	if err != nil {
		panic(err)
	}
	return logger
}

const (
	LoggerComponent = "component"
	Term            = "term"
	Peer            = "peer"
	Index           = "me"
	PeerTerm        = "peer term"
)

const (
	followerTimeout       = "follower timeout"
	candidateTimeout      = "candidate timeout"
	candidateBecomeLeader = "candidate become leader"
)
