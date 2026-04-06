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
	Op        OpType // "Put" or "Append"
	ClientId  int64
	SeqNumber int32
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type OpType string

const (
	PutOp    OpType = "Put"
	AppendOp OpType = "Append"
	GetOp    OpType = "Get"
	// Transaction support
	TxnCondEqual    OpType = "TxnCondEqual"
	TxnCondNotEqual OpType = "TxnCondNotEqual"
	TxnCondExist    OpType = "TxnCondExist"
	TxnCondNotExist OpType = "TxnCondNotExist"
)

func IsReadOperation(op OpType) bool {
	return op != PutOp && op != AppendOp
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
