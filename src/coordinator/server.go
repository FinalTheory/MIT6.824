package coordinator

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"6.5840/shardkv"
)

const CoordinatorRaftMaxSize = 1024

type Coordinator struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()

	mutex            sync.Mutex
	stateTable       map[string]*TxnState
	lastAppliedIndex int
	config           atomic.Pointer[shardctrler.Config]
	sm               *shardctrler.Clerk
	make_end         func(string) *labrpc.ClientEnd

	// inflightTxns deduplicates concurrent Transaction RPC handlers for the
	// same txnID on this coordinator process.
	inflightMu   sync.Mutex
	inflightTxns map[string]struct{}

	// activeExecutors deduplicates background workers that drive a txn through
	// Prepare/Commit/Abort. It is runtime-only state and is intentionally not
	// persisted, so recovery can re-start these workers after crash/reload.
	activeMu        sync.Mutex
	activeExecutors map[string]struct{}

	// states related to gracefully kill
	killCh               chan bool
	executorKilled       chan bool
	configPullerKilled   chan bool
	recoveryDriverKilled chan bool
}

func (co *Coordinator) tryEnterTxn(txnID string) bool {
	co.inflightMu.Lock()
	defer co.inflightMu.Unlock()
	if _, ok := co.inflightTxns[txnID]; ok {
		return false
	}
	co.inflightTxns[txnID] = struct{}{}
	return true
}

func (co *Coordinator) leaveTxn(txnID string) {
	co.inflightMu.Lock()
	defer co.inflightMu.Unlock()
	delete(co.inflightTxns, txnID)
}

func (co *Coordinator) tryEnterExecutor(txnID string) bool {
	co.activeMu.Lock()
	defer co.activeMu.Unlock()
	if _, ok := co.activeExecutors[txnID]; ok {
		return false
	}
	co.activeExecutors[txnID] = struct{}{}
	return true
}

func (co *Coordinator) leaveExecutor(txnID string) {
	co.activeMu.Lock()
	defer co.activeMu.Unlock()
	delete(co.activeExecutors, txnID)
}

// this is a blocking function call
// it blocks until the txn is committed/aborted, or the server node crashes
func (co *Coordinator) Transaction(args *TxnArgs, reply *TxnReply) {
	raft.DPrintf("[coordinator %d] Transaction start txn=%s ops=%d", co.me, args.TxnId, len(args.Operations))
	// 1. Leader check
	if _, isLeader := co.rf.GetState(); !isLeader {
		raft.DPrintf("[coordinator %d] Transaction reject txn=%s: not leader", co.me, args.TxnId)
		reply.Err = shardkv.ErrWrongLeader
		return
	}
	// 2. Avoid concurrent call on same Txn
	if !co.tryEnterTxn(args.TxnId) {
		raft.DPrintf("[coordinator %d] Transaction reject txn=%s: in flight", co.me, args.TxnId)
		reply.Err = shardkv.ErrTxnInFlight
		return
	}
	defer co.leaveTxn(args.TxnId)
	// 3. Check txn state table
	co.mutex.Lock()
	state, ok := co.stateTable[args.TxnId]
	co.mutex.Unlock()
	if !ok {
		config := co.config.Load()
		// First persist the participants of this transaction
		participants := make(map[int][]TxnOperation)
		participantIndexes := make(map[int][]int)
		for idx, op := range args.Operations {
			shard := shardkv.Key2shard(op.Key)
			gid := config.Shards[shard]
			participants[gid] = append(participants[gid], op)
			participantIndexes[gid] = append(participantIndexes[gid], idx)
		}
		cmd := TxnCmd{
			TxnId:          args.TxnId,
			Status:         TxnStatusPrepare,
			Config:         config,
			GroupOps:       participants,
			GroupOpIndexes: participantIndexes,
			OpsCount:       len(args.Operations),
			ExecutedCh:     make(chan bool, 1),
		}
		if _, ok := shardkv.PersistCommand(co.rf, cmd, cmd.ExecutedCh, func() {
			reply.Err = shardkv.ErrWrongLeader
		}); !ok {
			return
		}
	}
	// 4. Now we should have txn in state machine, read the state
	co.mutex.Lock()
	state, ok = co.stateTable[args.TxnId]
	if !ok {
		panic("invalid state")
	}
	status := state.Status
	co.mutex.Unlock()
	// 5. If txn already finalized, early return
	if status == TxnStatusCommitted {
		reply.Err = shardkv.OK
		reply.Values = state.Values
		return
	}
	if status == TxnStatusAborted {
		reply.Err = shardkv.ErrTxnAborted
		return
	}
	// 6. If not, wait for result and return txn results
	if state.ResultCh == nil {
		reply.Err = shardkv.ErrWrongLeader
		return
	}
	raft.DPrintf("[coordinator %d] Transaction wait result txn=%s status=%s", co.me, args.TxnId, status)
	reply.Err = <-state.ResultCh
	reply.Values = state.Values
	raft.DPrintf("[coordinator %d] Transaction done txn=%s reply=%s", co.me, args.TxnId, reply.Err)
}

func (co *Coordinator) stateMachineExecutor() {
	defer func() {
		co.executorKilled <- true
	}()
	for !co.killed() {
		select {
		case cmd := <-co.applyCh:
			raft.DPrintf("[coordinator %d] applyCh recv valid=%v index=%d", co.me, cmd.CommandValid, cmd.CommandIndex)
			if cmd.SnapshotValid && cmd.SnapshotIndex <= co.lastAppliedIndex {
				panic(fmt.Sprintf("unexpected SnapshotIndex %d <= lastAppliedIndex %d", cmd.SnapshotIndex, co.lastAppliedIndex))
			}
			if cmd.SnapshotValid {
				co.reloadFromSnapshot(cmd.Snapshot)
				co.lastAppliedIndex = cmd.SnapshotIndex
				continue
			}
			if !cmd.CommandValid {
				continue
			}
			if cmd.CommandIndex <= co.lastAppliedIndex {
				panic("unexpected")
			}
			op := cmd.Command.(TxnCmd)
			raft.DPrintf("[coordinator %d] stateMachineExecutor applying txn=%s status=%s index=%d", co.me, op.TxnId, op.Status, cmd.CommandIndex)
			co.applyOperation(op)
			co.lastAppliedIndex = cmd.CommandIndex
			if co.persister.RaftStateSize() >= CoordinatorRaftMaxSize {
				co.doSnapshot(cmd.CommandIndex)
			}
			shardkv.SafeWriteChannel(op.ExecutedCh, true)
		case killed := <-co.killCh:
			if killed {
				return
			}
		}
	}
}

func (co *Coordinator) moveToStatus(txnId string, nextStatus TxnStatus) {
	// use while loop to make sure op is persisted
	cmd := TxnCmd{
		TxnId:      txnId,
		Status:     nextStatus,
		ExecutedCh: make(chan bool, 1),
	}
	shardkv.PersistCommand(co.rf, cmd, cmd.ExecutedCh, func() {})
}

type groupRPCFunc func(srv *labrpc.ClientEnd, cmd TxnCmd, ops []TxnOperation) groupRPCResult

type groupRPCResult struct {
	ok     bool
	err    shardkv.Err
	values []string
	gid    int
}

func (co *Coordinator) sendPrepareRPC(srv *labrpc.ClientEnd, cmd TxnCmd, ops []TxnOperation) groupRPCResult {
	args := shardkv.PrepareArgs{TxnId: cmd.TxnId, Config: *cmd.Config, TxnOperations: ops}
	var reply shardkv.PrepareReply
	ok := srv.Call("ShardKV.Prepare", &args, &reply)
	return groupRPCResult{ok: ok, err: reply.Err}
}

func (co *Coordinator) sendCommitRPC(srv *labrpc.ClientEnd, cmd TxnCmd, ops []TxnOperation) groupRPCResult {
	_ = ops
	args := shardkv.CommitArgs{TxnId: cmd.TxnId}
	var reply shardkv.CommitReply
	ok := srv.Call("ShardKV.Commit", &args, &reply)
	return groupRPCResult{ok: ok, err: reply.Err, values: reply.Values}
}

func (co *Coordinator) sendAbortRPC(srv *labrpc.ClientEnd, cmd TxnCmd, ops []TxnOperation) groupRPCResult {
	_ = ops
	args := shardkv.AbortArgs{TxnId: cmd.TxnId}
	var reply shardkv.AbortReply
	ok := srv.Call("ShardKV.Abort", &args, &reply)
	return groupRPCResult{ok: ok, err: reply.Err}
}

func (co *Coordinator) broadcastToGroups(cmd TxnCmd, state *TxnState, rpcFunc groupRPCFunc, valuesOut []string) bool {
	resultsCh := make(chan groupRPCResult, len(state.GroupOps))
	for gid, ops := range state.GroupOps {
		go func(gid int, ops []TxnOperation) {
			servers, ok := state.Config.Groups[gid]
			if !ok || len(servers) == 0 {
				resultsCh <- groupRPCResult{ok: false}
				return
			}
			for _, server := range servers {
				srv := co.make_end(server)
				result := rpcFunc(srv, cmd, ops)
				if !result.ok {
					continue
				}
				// this is the ONLY happy path
				if result.err == shardkv.OK {
					result.gid = gid
					resultsCh <- result
					return
				}
				// if sent to wrong group, it means the config has changed
				if result.err == shardkv.ErrWrongGroup {
					break
				}
				// if txn aborted on participants side, no need to retry
				if result.err == shardkv.ErrTxnAborted {
					break
				}
			}
			resultsCh <- groupRPCResult{ok: false}
		}(gid, ops)
	}
	for range state.GroupOps {
		result := <-resultsCh
		if !(result.ok && result.err == shardkv.OK) {
			return false
		}
		if valuesOut != nil {
			indexes := state.GroupOpIndexes[result.gid]
			for i, originalIndex := range indexes {
				valuesOut[originalIndex] = result.values[i]
			}
		}
	}
	return true
}

func (co *Coordinator) executePrepare(cmd TxnCmd, state *TxnState) {
	if !co.tryEnterExecutor(cmd.TxnId) {
		return
	}
	defer co.leaveExecutor(cmd.TxnId)
	if _, isLeader := co.rf.GetState(); !isLeader {
		raft.DPrintf("[coordinator %d] executePrepare skip txn=%s: not leader", co.me, cmd.TxnId)
		return
	}
	raft.DPrintf("[coordinator %d] executePrepare txn=%s participants=%d config=%d", co.me, cmd.TxnId, len(state.GroupOps), state.Config.Num)
	if co.broadcastToGroups(cmd, state, co.sendPrepareRPC, nil) {
		co.moveToStatus(cmd.TxnId, TxnStatusCommit)
	} else {
		co.moveToStatus(cmd.TxnId, TxnStatusAbort)
	}
}

func (co *Coordinator) executeFinalAction(cmd TxnCmd, state *TxnState, rpcFunc groupRPCFunc, finalStatus TxnStatus, valuesOut []string) {
	if !co.tryEnterExecutor(cmd.TxnId) {
		return
	}
	defer co.leaveExecutor(cmd.TxnId)
	raft.DPrintf("[coordinator %d] ensure final action %s txn=%s participants=%d config=%d", co.me, cmd.Status, cmd.TxnId, len(state.GroupOps), state.Config.Num)
	for {
		if _, isLeader := co.rf.GetState(); !isLeader {
			raft.DPrintf("[coordinator %d] %s skip txn=%s: not leader", co.me, cmd.Status, cmd.TxnId)
			return
		}
		if co.broadcastToGroups(cmd, state, rpcFunc, valuesOut) {
			nextCmd := TxnCmd{
				TxnId:      cmd.TxnId,
				Status:     finalStatus,
				Values:     valuesOut,
				ExecutedCh: make(chan bool, 1),
			}
			shardkv.PersistCommand(co.rf, nextCmd, nextCmd.ExecutedCh, func() {})
			return
		} else {
			raft.DPrintf("[coordinator %d] ensure final action %s txn=%s: send failed", co.me, cmd.Status, cmd.TxnId)
		}
	}
}

func (co *Coordinator) applyOperation(cmd TxnCmd) {
	raft.DPrintf("[coordinator %d] applyOperation txn=%s status=%s", co.me, cmd.TxnId, cmd.Status)
	co.mutex.Lock()
	defer co.mutex.Unlock()
	state, ok := co.stateTable[cmd.TxnId]
	// to avoid the state machine moving backwards
	if ok && statusOrder(cmd.Status) <= statusOrder(state.Status) {
		raft.DPrintf("[coordinator %d] applyOperation ignore txn=%s incoming=%s current=%s", co.me, cmd.TxnId, cmd.Status, state.Status)
		return
	}
	if ok && state.ResultCh == nil {
		state.ResultCh = make(chan shardkv.Err, 1)
	}
	switch cmd.Status {
	case TxnStatusPrepare:
		raft.DPrintf("[coordinator %d] applyOperation prepare txn=%s", co.me, cmd.TxnId)
		state := TxnState{
			Status:         TxnStatusPrepare,
			Config:         cmd.Config,
			GroupOps:       cmd.GroupOps,
			GroupOpIndexes: cmd.GroupOpIndexes,
			OpsCount:       cmd.OpsCount,
			Values:         nil,
			ResultCh:       make(chan shardkv.Err, 1),
		}
		co.stateTable[cmd.TxnId] = &state
		go co.executePrepare(cmd, &state)
	case TxnStatusCommit:
		raft.DPrintf("[coordinator %d] applyOperation commit txn=%s", co.me, cmd.TxnId)
		state.Status = TxnStatusCommit
		go co.executeFinalAction(
			cmd,
			state,
			co.sendCommitRPC,
			TxnStatusCommitted,
			make([]string, state.OpsCount),
		)
	case TxnStatusAbort:
		raft.DPrintf("[coordinator %d] applyOperation abort txn=%s", co.me, cmd.TxnId)
		state.Status = TxnStatusAbort
		go co.executeFinalAction(
			cmd,
			state,
			co.sendAbortRPC,
			TxnStatusAborted,
			nil,
		)
	case TxnStatusCommitted:
		state.Status = TxnStatusCommitted
		state.Values = cmd.Values
		raft.DPrintf("[coordinator %d] applyOperation committed txn=%s", co.me, cmd.TxnId)
		shardkv.SafeWriteChannel(state.ResultCh, shardkv.OK)
	case TxnStatusAborted:
		state.Status = TxnStatusAborted
		raft.DPrintf("[coordinator %d] applyOperation aborted txn=%s", co.me, cmd.TxnId)
		shardkv.SafeWriteChannel(state.ResultCh, shardkv.ErrTxnAborted)
	}
}

func (co *Coordinator) recoveryDriver() {
	for !co.killed() {
		if _, isLeader := co.rf.GetState(); isLeader {
			co.mutex.Lock()
			for txnID, state := range co.stateTable {
				switch state.Status {
				case TxnStatusPrepare:
					go co.executePrepare(TxnCmd{
						TxnId:  txnID,
						Status: TxnStatusPrepare,
						Config: state.Config,
					}, state)
				case TxnStatusCommit:
					go co.executeFinalAction(
						TxnCmd{
							TxnId:  txnID,
							Status: TxnStatusCommit,
							Config: state.Config,
						},
						state,
						co.sendCommitRPC,
						TxnStatusCommitted,
						make([]string, state.OpsCount),
					)
				case TxnStatusAbort:
					go co.executeFinalAction(
						TxnCmd{
							TxnId:  txnID,
							Status: TxnStatusAbort,
							Config: state.Config,
						},
						state,
						co.sendAbortRPC,
						TxnStatusAborted,
						nil,
					)
				}
			}
			co.mutex.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
	co.recoveryDriverKilled <- true
}

func (co *Coordinator) configPullerThread() {
	for !co.killed() {
		// ask controler for the latest configuration.
		cfg := co.sm.Query(-1)
		co.config.Store(&cfg)
		time.Sleep(100 * time.Millisecond)
	}
	co.configPullerKilled <- true
}

func (co *Coordinator) doSnapshot(index int) {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	if err := e.Encode(co.lastAppliedIndex); err != nil {
		log.Fatal(err)
	}
	co.mutex.Lock()
	if err := e.Encode(co.stateTable); err != nil {
		log.Fatal(err)
	}
	co.mutex.Unlock()
	co.rf.Snapshot(index, buf.Bytes())
}

func (co *Coordinator) reloadFromSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastAppliedIndex int
	var stateTable map[string]*TxnState
	if d.Decode(&lastAppliedIndex) != nil ||
		d.Decode(&stateTable) != nil {
		panic("Failed to reload persisted snapshot into coordinator.")
	}
	co.lastAppliedIndex = lastAppliedIndex
	co.mutex.Lock()
	co.stateTable = stateTable
	co.mutex.Unlock()
}

func (co *Coordinator) killed() bool {
	z := atomic.LoadInt32(&co.dead)
	return z == 1
}

func (co *Coordinator) Kill() {
	if atomic.LoadInt32(&co.dead) == 1 {
		return
	}
	atomic.StoreInt32(&co.dead, 1)
	co.killCh <- true
	co.rf.Kill()
	co.sm.Kill()
	<-co.executorKilled
	<-co.configPullerKilled
	<-co.recoveryDriverKilled
}

func statusOrder(status TxnStatus) int {
	switch status {
	case TxnStatusPrepare:
		return 1
	case TxnStatusCommit:
		return 2
	case TxnStatusAbort:
		return 2
	case TxnStatusCommitted:
		return 3
	case TxnStatusAborted:
		return 3
	default:
		return 0
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Coordinator {
	co := new(Coordinator)
	co.me = me
	labgob.Register(TxnCmd{})
	co.applyCh = make(chan raft.ApplyMsg)
	co.persister = persister
	co.rf = raft.Make(servers, me, persister, co.applyCh)
	co.sm = shardctrler.MakeClerk(ctrlers)
	co.make_end = make_end
	co.stateTable = make(map[string]*TxnState)
	co.inflightTxns = make(map[string]struct{})
	co.activeExecutors = make(map[string]struct{})
	co.lastAppliedIndex = 0
	cfg := co.sm.Query(-1)
	co.config.Store(&cfg)
	co.killCh = make(chan bool, 10)
	co.executorKilled = make(chan bool)
	co.configPullerKilled = make(chan bool)
	co.recoveryDriverKilled = make(chan bool)
	co.reloadFromSnapshot(persister.ReadSnapshot())
	go co.stateMachineExecutor()
	go co.configPullerThread()
	go co.recoveryDriver()
	return co
}
