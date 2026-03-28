package coordinator

import (
	"6.5840/shardctrler"
	"6.5840/shardkv"
)

type TxnOperation = shardkv.TxnOperation

type TxnArgs struct {
	TxnId      string
	Operations []TxnOperation
}

type TxnReply = shardkv.TxnResult

type TxnStatus string

const (
	TxnStatusPrepare   TxnStatus = "TxnStatusPrepare"
	TxnStatusCommit    TxnStatus = "TxnStatusCommit"
	TxnStatusAbort     TxnStatus = "TxnStatusAbort"
	TxnStatusCommitted TxnStatus = "TxnStatusCommitted"
	TxnStatusAborted   TxnStatus = "TxnStatusAborted"
)

type TxnCmd struct {
	Status         TxnStatus
	TxnId          string
	Config         *shardctrler.Config
	GroupOps       map[int][]TxnOperation // mapping from gid => list of operations
	GroupOpIndexes map[int][]int          // mapping from gid => original op indexes
	OpsCount       int
	Values         []string
	ExecutedCh     chan bool
}

type TxnState struct {
	Status         TxnStatus
	Config         *shardctrler.Config
	GroupOps       map[int][]TxnOperation
	GroupOpIndexes map[int][]int
	OpsCount       int
	Values         []string
	ResultCh       chan shardkv.Err
}
