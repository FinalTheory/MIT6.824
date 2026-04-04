package coordinator

import (
	"sync"

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

const (
	EventTxnRejectNotLeader    = "TxnRejectNotLeader"
	EventPrepareLostLeader     = "TxnPrepareLostLeader"
	EventPrepareFailed         = "TxnPrepareFailed"
	EventFinalActionLostLeader = "TxnFinalActionLostLeader"
	EventFinalActionRetry      = "TxnFinalActionRetry"
	EventRecoveryDrivePrepare  = "TxnRecoveryDrivePrepare"
	EventRecoveryDriveCommit   = "TxnRecoveryDriveCommit"
	EventRecoveryDriveAbort    = "TxnRecoveryDriveAbort"
	EventSnapshotSave          = "TxnSnapshotSave"
	EventSnapshotLoad          = "TxnSnapshotLoad"
)

const (
	TraceTxnStart          = "TxnStart"
	TraceTxnRejectInFlight = "TxnRejectInFlight"
	TraceTxnWait           = "TxnWait"
	TraceTxnDone           = "TxnDone"
	TraceTxnCoordApply     = "TxnCoordApply"
	TraceTxnPrepare        = "TxnPrepare"
	TraceTxnFinalAction    = "TxnFinalAction"
	TraceTxnStateApply     = "TxnStateApply"
	TraceTxnStateIgnore    = "TxnStateIgnore"
	TraceTxnPrepareApplied = "TxnPrepareApplied"
	TraceTxnCommitApplied  = "TxnCommitApplied"
	TraceTxnAbortApplied   = "TxnAbortApplied"
	TraceTxnCommitted      = "TxnCommitted"
	TraceTxnAborted        = "TxnAborted"
)

type eventRecorder struct {
	mu     sync.Mutex
	counts map[string]int
}

var globalEventRecorder = eventRecorder{counts: make(map[string]int)}

func eventKey(name string, status TxnStatus) string {
	switch name {
	case EventFinalActionLostLeader:
		if status == TxnStatusCommit {
			return "TxnCommitDriveLostLeader"
		}
		if status == TxnStatusAbort {
			return "TxnAbortDriveLostLeader"
		}
	case EventFinalActionRetry:
		if status == TxnStatusCommit {
			return "TxnCommitRetry"
		}
		if status == TxnStatusAbort {
			return "TxnAbortRetry"
		}
	}
	return name
}

func hitEvent(name string, status ...TxnStatus) {
	key := name
	if len(status) > 0 {
		key = eventKey(name, status[0])
	}
	globalEventRecorder.mu.Lock()
	defer globalEventRecorder.mu.Unlock()
	globalEventRecorder.counts[key]++
}

func resetEvents() {
	globalEventRecorder.mu.Lock()
	defer globalEventRecorder.mu.Unlock()
	globalEventRecorder.counts = make(map[string]int)
}

func eventCount(name string, status ...TxnStatus) int {
	key := name
	if len(status) > 0 {
		key = eventKey(name, status[0])
	}
	globalEventRecorder.mu.Lock()
	defer globalEventRecorder.mu.Unlock()
	return globalEventRecorder.counts[key]
}
