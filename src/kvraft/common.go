package kvraft

const (
	OK                = "OK"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrTimeOut        = "ErrTimeOut"
	ErrStaleRequest   = "ErrStaleRequest"
	ErrLostLeadership = "ErrLostLeadership"
	ErrKilled         = "ErrKilled"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	SeqNumber int32
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

const (
	PutOp    = "Put"
	AppendOp = "Append"
	GetOp    = "Get"
)

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
