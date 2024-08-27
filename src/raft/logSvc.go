package raft

import (
	"errors"
	"fmt"

	"go.uber.org/zap"
)

const (
	TermStartFrom  = 0
	EmptyLogIndex  = 0
	IndexStartFrom = 1
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type LogService struct {
	Storage
	logger *zap.Logger
}

func NewLogService(me int) *LogService {
	return &LogService{
		Storage: Storage{
			Snapshot: nil,
		},
		logger: GetLoggerOrPanic("log service").With(zap.Int(Index, me)),
	}
}

func (ls *LogService) CreateReadOnlyReplicate() *LogService {
	newLs := new(LogService)
	*newLs = *ls
	return ls
}

func (ls *LogService) AppendLogAndReturnNextIndex(
	lastLogIndex, lastLogTerm int, entry *LogEntry) (int, bool) {
	if entry == nil { // heartbeat
		ls.logger.Debug("receive heartbeat")
		return ls.GetLastLogIndex() + 1, false
	}

	match := func(logIndex int, targetTerm int) bool {
		if logIndex < IndexStartFrom {
			return true
		}
		term, err := ls.GetLogTermByIndex(logIndex)
		if err != nil {
			if err == errorRetrieveEntryInSnapshot {
				panic(err)
			}
			return false
		}
		return term == targetTerm
	}

	if match(entry.Index, entry.Term) {
		ls.logger.Debug("log matched, want next")
		return entry.Index + 1, false
	} else if match(lastLogIndex, lastLogTerm) {
		ls.logger.Debug(
			"last log matched, append",
			zap.String("log", fmt.Sprintf("%#v", entry)),
		)
		_ = ls.RemoveEntriesAfterIndex(lastLogIndex)
		ls.AppendNewEntry(entry)
		return entry.Index + 1, true
	} else {
		ls.logger.Debug("no log matched, try older")
		return lastLogIndex, false
	}
}

func (ls *LogService) AppendSnapshotAndReturnNextIndex(snapshot *Snapshot) (int, bool) {
	if err := ls.SaveSnapshot(snapshot); err != nil {
		if err == errorSnapshotExists {
			ls.logger.Debug("snapshot exists, request next index")
		}
		return snapshot.LastLogIndex + 1, false
	}
	return snapshot.LastLogIndex + 1, true
}

func (ls *LogService) AppendNewCommand(term int, command interface{}) *LogEntry {
	ls.logger.Debug(
		"store new command",
		zap.String("command", fmt.Sprintf("%#v", command)),
	)
	entry := &LogEntry{
		Term:    term,
		Index:   ls.GetLastLogIndex() + 1,
		Command: command,
	}
	ls.AppendNewEntry(entry)
	return entry
}

func (ls *LogService) BuildSnapshot(term int, index int, data []byte) error {
	ls.logger.Debug(
		"build snapshot",
		zap.Int("endAt", index),
		zap.Int(Term, term),
	)
	term, err := ls.GetLogTermByIndex(index)
	if err != nil {
		if err == errorRetrieveEntryInSnapshot {
			return errorSnapshotExists
		}
	}
	return ls.SaveSnapshot(&Snapshot{
		BuildTerm:    term,
		LastLogIndex: index,
		LastLogTerm:  term,
		Data:         data,
	})
}

func (ls *LogService) Encode(encoder func(val interface{})) {
	encoder(ls.Storage)
}

func (ls *LogService) Recover(decoder func(p interface{})) {
	decoder(&ls.Storage)
}

type Storage struct {
	Logs     Logs
	Snapshot *Snapshot
}

func (st *Storage) GetLastLogTerm() int {
	if len(st.Logs) > 0 {
		return st.Logs[len(st.Logs)-1].Term
	}
	if st.Snapshot != nil {
		return st.Snapshot.LastLogTerm
	}
	return TermStartFrom
}

func (st *Storage) GetLastLogIndex() int {
	if len(st.Logs) > 0 {
		return st.Logs[len(st.Logs)-1].Index
	}
	if st.Snapshot != nil {
		return st.Snapshot.LastLogIndex
	}
	return EmptyLogIndex
}

func (st *Storage) GetLogEntryByIndex(index int) (*LogEntry, error) {
	if index < IndexStartFrom {
		return nil, errorLogIndexBelowLowerBound
	}
	if st.Snapshot != nil && st.Snapshot.include(index) {
		return nil, errorRetrieveEntryInSnapshot
	}
	return st.Logs.find(index)
}

func (st *Storage) GetLogTermByIndex(index int) (int, error) {
	if index < IndexStartFrom {
		return TermStartFrom, errorLogIndexBelowLowerBound
	}
	if st.Snapshot != nil && st.Snapshot.include(index) {
		if st.Snapshot.LastLogIndex == index {
			return st.Snapshot.LastLogTerm, nil
		}
		return TermStartFrom, errorRetrieveEntryInSnapshot
	}
	entry, err := st.Logs.find(index)
	if err != nil {
		return TermStartFrom, err
	}
	return entry.Term, nil
}

func (st *Storage) AppendNewEntry(entry *LogEntry) {
	st.Logs = append(st.Logs, entry)
}

func (st *Storage) RemoveEntriesAfterIndex(index int) error {
	if index < EmptyLogIndex {
		return errorIllegalLogIndex
	}
	if index == EmptyLogIndex {
		st.Logs.reset()
		st.Snapshot = nil
		return nil
	}
	if st.Snapshot != nil && st.Snapshot.include(index) {
		if st.Snapshot.LastLogIndex == index {
			st.Logs.reset()
			return nil
		}
		return errorTruncateLogsInSnapshot
	}
	st.Logs.removeEntriesAfterIndex(index)
	return nil
}

func (st *Storage) GetSnapshot() *Snapshot {
	return st.Snapshot
}

func (st *Storage) SaveSnapshot(snapshot *Snapshot) error {
	if st.Snapshot != nil && st.Snapshot.include(snapshot.LastLogIndex) {
		return errorSnapshotExists
	}
	if len(st.Logs) != 0 {
		st.Logs.removeEntriesBeforeIndex(snapshot.LastLogIndex + 1)
	}
	st.Snapshot = snapshot
	return nil
}

type Logs []*LogEntry // Do not need to consider lower limit

func (logs Logs) find(index int) (*LogEntry, error) {
	if len(logs) == 0 {
		return nil, errorLogIndexExceedUpperLimit
	}

	start, end := logs.getStartAndEndIndex()
	if index > end {
		return nil, errorLogIndexExceedUpperLimit
	}
	return logs[index-start], nil
}

func (logs *Logs) reset() {
	*logs = (*logs)[:0]
}

func (logs Logs) getStartAndEndIndex() (int, int) {
	return logs[0].Index, logs[len(logs)-1].Index
}

func (logs *Logs) removeEntriesAfterIndex(index int) {
	start, end := logs.getStartAndEndIndex()

	if index < end {
		*logs = (*logs)[:index-start+1]
	}
}

func (logs *Logs) removeEntriesBeforeIndex(index int) {
	start, end := logs.getStartAndEndIndex()

	if index > end {
		logs.reset()
	} else {
		*logs = (*logs)[index-start:]
	}
}

type Snapshot struct {
	BuildTerm    int
	LastLogIndex int
	LastLogTerm  int
	Data         []byte
}

func (sp *Snapshot) include(index int) bool {
	return sp.LastLogIndex >= index
}

var (
	errorIllegalLogIndex          = errors.New("errorIllegalLogIndex")
	errorLogIndexExceedUpperLimit = errors.New("errorLogIndexExceedUpperLimit")
	errorLogIndexBelowLowerBound  = errors.New("errorLogIndexBelowLowerBound")
	errorRetrieveEntryInSnapshot  = errors.New("errorRetrieveEntryInSnapshot")
	errorTruncateLogsInSnapshot   = errors.New("errorTruncateLogsInSnapshot")
	errorSnapshotExists           = errors.New("errorSnapshotExists")
)
