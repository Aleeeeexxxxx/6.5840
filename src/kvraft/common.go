package kvraft

import (
	"fmt"
)

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrDuplicateReq  = "ErrDuplicateReq"
	ErrTimeout       = "ErrTimeout"
	ErrServerStopped = "ErrServerStopped"
)

type Err string

type Metadata struct {
	ClerkID   int32
	MessageID int64
}

func (m Metadata) String() string {
	return fmt.Sprintf("clerkID:%d // MessageID:%d", m.ClerkID, m.MessageID)
}

// Put or Append
type PutAppendArgs struct {
	Metadata
	Key   string
	Value string
	Op    string // "Put" or "Append"
}

func (args PutAppendArgs) String() string {
	return fmt.Sprintf(
		"Key:%s // Value:%s // Op:%s // %s",
		args.Key, args.Value, args.Op, args.Metadata.String(),
	)
}

type GetArgs struct {
	Metadata
	Key string
}

func (ga GetArgs) String() string {
	return fmt.Sprintf("Key:%s // %s", ga.Key, ga.Metadata.String())
}

type CommonReply struct {
	Metadata
	Err   Err
	Value string
}

func (cr CommonReply) String() string {
	return fmt.Sprintf("Value:%s // Err:%s // %s", cr.Value, cr.Err, cr.Metadata.String())
}

func (cr CommonReply) Accept() bool {
	return cr.Err == OK || cr.Err == ErrNoKey
}

type Stringfy interface {
	String() string
}

type Args interface {
	Stringfy
}

type Reply interface {
	Stringfy
	Accept() bool
}
