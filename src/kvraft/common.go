package kvraft

import (
	"fmt"
)

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrDuplicateReq = "ErrDuplicateReq"
	ErrTimeout      = "ErrTimeout"
)

type Err string

type Metadata struct {
	ClerkID   int32
	MessageID int64
}

func (m Metadata) String() string {
	return fmt.Sprintf("clerkID:%d_MessageID:%d", m.ClerkID, m.MessageID)
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
		"Key:%s_Value:%s_Op:%s_%s",
		args.Key, args.Value, args.Op, args.Metadata.String(),
	)
}

type PutAppendReply struct {
	Metadata
	Err Err
}

func (reply PutAppendReply) String() string {
	return fmt.Sprintf("Err:%s_%s", reply.Err, reply.Metadata.String())
}

func (reply PutAppendReply) Accept() bool {
	return reply.Err == OK
}

type GetArgs struct {
	Metadata
	Key string
}

func (ga GetArgs) String() string {
	return fmt.Sprintf("Key:%s_%s", ga.Key, ga.Metadata.String())
}

type GetReply struct {
	Metadata
	Err   Err
	Value string
}

func (gr GetReply) String() string {
	return fmt.Sprintf("Value:%s_Err:%s_%s", gr.Value, gr.Err, gr.Metadata.String())
}

func (gr GetReply) Accept() bool {
	return gr.Err == OK
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
