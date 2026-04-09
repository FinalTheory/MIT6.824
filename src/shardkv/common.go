package shardkv

import (
	"time"

	"6.5840/kvraft"
	"6.5840/raft"
	"6.5840/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrWrongLeader Err = "ErrWrongLeader"
	// errors for txn
	ErrTxnAborted  Err = "ErrTxnAborted"
	ErrTxnInFlight Err = "ErrTxnInFlight"
	ErrTxnNotFound Err = "ErrTxnNotFound"
)

const (
	ConfigChange = "ConfigChange"
	InstallShard = "InstallShard"
	Nop          = "Nop"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        kvraft.OpType // "Put" or "Append"
	ClientId  int64
	SeqNumber int32
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	SeqNumber int32
}

type GetReply struct {
	Err   Err
	Value string
}

type InstallShardArgs struct {
	Shard int
	Data  map[string]string
	Num   int
	Dedup map[DedupKey]DedupEntry
	// for logging purpose
	From      int
	ClientId  int64
	SeqNumber int32
}

type InstallShardReply struct {
	Success bool
}

type ShardInfo struct {
	Shard int
	Num   int
}

func (args *InstallShardArgs) ShardInfo() ShardInfo {
	return ShardInfo{Shard: args.Shard, Num: args.Num}
}

type TxnOpType string

const (
	TxnPrepare TxnOpType = "TxnPrepare"
	TxnCommit  TxnOpType = "TxnCommit"
	TxnAbort   TxnOpType = "TxnAbort"
)

type TxnCmd struct {
	Type       TxnOpType
	TxnId      string
	PrimaryGID int
	Operations []TxnOperation
	Config     shardctrler.Config
	ResultCh   chan TxnResult
}

type TxnStatus string

type TxnState struct {
	Status     TxnStatus
	PrimaryGID int
	Operations []TxnOperation
	Values     []string
}

type TxnResult struct {
	Err    Err
	Values []string
}

// states in participants are slightly different than coordinator
// thus we use separate definition in different namespace
const (
	TxnStatusPrepared  TxnStatus = "TxnStatusPrepared"
	TxnStatusCommitted TxnStatus = "TxnStatusCommitted"
	TxnStatusAborted   TxnStatus = "TxnStatusAborted"
)

type TxnOperation struct {
	Key   string
	Value string
	Op    kvraft.OpType
}

type PrepareArgs struct {
	TxnId         string
	PrimaryGID    int
	TxnOperations []TxnOperation
	Config        shardctrler.Config
}

type PrepareReply struct {
	Err Err
}

type CommitArgs struct {
	TxnId string
}

type CommitReply = TxnResult

type AbortArgs = CommitArgs

type AbortReply = PrepareReply

type QueryTxnStatusArgs struct {
	TxnId string
}

type QueryTxnStatusReply struct {
	Err    Err
	Status TxnStatus
}

const (
	NewCmdTimeOut = 500 * time.Millisecond
)

// PersistCommand keeps retrying rf.Start(cmd) until either:
//  1. the current node loses leadership, in which case it runs
//     notLeaderCallback and returns (zeroValue, false), or
//  2. the command is applied and resultCh yields a result before timeout,
//     in which case it returns (result, true).
//
// The returned bool therefore means "this process successfully observed the
// command result while still being leader", not "the protocol as a whole has
// permanently succeeded".
func PersistCommand[T any](rf *raft.Raft, cmd interface{}, resultCh <-chan T, notLeaderCallback func()) (T, bool) {
	// use while loop to make sure op is persisted
	for {
		var zero T
		_, _, isLeader := rf.Start(cmd)
		if !isLeader {
			notLeaderCallback()
			return zero, false
		}
		result, ok := RecvWithTimeout(resultCh, NewCmdTimeOut)
		if ok {
			return result, true
		}
	}
}

func RecvWithTimeout[T any](ch <-chan T, timeout time.Duration) (T, bool) {
	var zero T
	if timeout <= 0 {
		v, ok := <-ch
		if !ok {
			return zero, false
		}
		return v, true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v, ok := <-ch:
		if !ok {
			return zero, false
		}
		return v, true
	case <-timer.C:
		return zero, false
	}
}

func SafeWriteChannel[T any](ch chan<- T, value T) {
	// Result channels here are only used as best-effort in-process notifications.
	// The real durable state lives in Raft-applied state machines, so apply paths
	// must never block on channel delivery. A receiver may already have timed out,
	// returned after losing leadership, or left an older result sitting in the
	// single-slot buffer due to duplicate/idempotent apply. Blocking here would
	// stall the single-threaded apply loop and turn a transient local wait issue
	// into a replicated state machine liveness bug.
	if ch != nil {
		select {
		case ch <- value:
		default:
		}
	}
}
