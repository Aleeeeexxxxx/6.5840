package kvraft

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type Request struct {
	Metadata Metadata
	Err      Err
	Value    string

	ch    chan struct{}
	timer *time.Timer
}

type RequestMngr struct {
	mutex    sync.Mutex
	requests map[int32]*Request

	logger   *zap.Logger
	queueing sync.WaitGroup
}

func NewRequestMngr(me int) *RequestMngr {
	return &RequestMngr{
		requests: make(map[int32]*Request),
		logger:   GetLoggerOrPanic("storage").With(zap.Int("me", me)),
	}
}

func (rm *RequestMngr) Wait(metadata Metadata) (string, Err) {
	req := &Request{
		Metadata: metadata,
		ch:       make(chan struct{}, 1),
		timer:    time.NewTimer(time.Second),
	}

	rm.inQueue(req)

	select {
	case <-req.ch:
		logger := rm.logger.
			With(zap.Int32(LogClerkID, metadata.ClerkID)).
			With(zap.Int64(LogMessageID, metadata.MessageID))
		logger.Info("request finished")
	case <-req.timer.C:
		rm.handleTimeout(req)
	}

	return req.Value, req.Err
}

func (rm *RequestMngr) handleTimeout(req *Request) {
	logger := rm.logger.
		With(zap.Int32(LogClerkID, req.Metadata.ClerkID)).
		With(zap.Int64(LogMessageID, req.Metadata.MessageID))

	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	logger.Info("request timeout")

	key := req.Metadata.ClerkID
	cur, ok := rm.requests[key]
	if ok && cur.Metadata.MessageID == req.Metadata.MessageID {
		logger.Debug("remove request from queue")
		delete(rm.requests, key)
	}
	if req.Err != "" {
		req.Err = ErrTimeout
	}
}

func (rm *RequestMngr) inQueue(req *Request) {
	logger := rm.logger.
		With(zap.Int32(LogClerkID, req.Metadata.ClerkID)).
		With(zap.Int64(LogMessageID, req.Metadata.MessageID))
	logger.Info("inqueue new request")

	key := req.Metadata.ClerkID

	rm.queueing.Add(1)
	defer rm.queueing.Done()

	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	old, ok := rm.requests[key]
	if ok {
		logger.Warn(
			"discard old one for duplicated request",
			zap.Int64("msgID", old.Metadata.MessageID),
		)
		old.Err = ErrDuplicateReq
		old.ch <- struct{}{}
	}
	rm.requests[key] = req
}

func (rm *RequestMngr) Complete(metadata Metadata, value string, err Err) {
	logger := rm.logger.
		With(zap.Int32(LogClerkID, metadata.ClerkID)).
		With(zap.Int64(LogMessageID, metadata.MessageID))
	logger.Debug("try to reply the request via op")

	key := metadata.ClerkID

	rm.mutex.Lock()
	defer rm.mutex.Unlock()

	req, ok := rm.requests[key]
	if !ok {
		logger.Debug("no related request, discard op")
		return
	}

	req.Value = value
	req.Err = err
	req.ch <- struct{}{}

	delete(rm.requests, key)
	logger.Info(
		"reply the request",
		zap.String("value", value),
	)
}

func (rm *RequestMngr) Release() {
	rm.logger.Info("waiting for all inflight requests inqueue")
	rm.queueing.Wait()

	rm.logger.Info("release all requests")
	for _, req := range rm.requests {
		req.Err = ErrWrongLeader
		req.ch <- struct{}{}
	}
}
