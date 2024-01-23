package shardkv

import (
	"6.5840/kvraft"
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

type Result struct {
	Value string
	Valid bool
}

type Op struct {
	Key   string
	Value string
	Op    string
	From  int
	// channel used to fetch the operation result
	ResultCh  chan Result
	ClientId  int64
	SeqNumber int32
	// for config change
	NewConfig *shardctrler.Config
	// install shard migration data
	ShardArgs InstallShardArgs
}

func (op *Op) RequestId() kvraft.RequestId {
	return kvraft.RequestId{ClientId: op.ClientId, SeqNumber: op.SeqNumber}
}

type ShardData struct {
	Arg     InstallShardArgs
	Servers []string
}

type ShardKV struct {
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	mck          *shardctrler.Clerk
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	mu              sync.Mutex
	pendingRequests map[int]kvraft.RequestInfo

	// access to K/V state is single threaded, thus no lock needed
	state map[string]string

	rwLock     sync.RWMutex
	dedupTable map[int64]int32
	valueTable map[int64]string

	// states to send shards during migration
	config                atomic.Pointer[shardctrler.Config]
	skippedConfigChangeOp int
	shardsToSend          []ShardData
	muSendShards          sync.Mutex
	condSendShards        *sync.Cond
	// states to handle shard migration
	shardsToRecv  map[int]bool
	pendingShards map[ShardInfo]map[string]string

	// states related to gracefully kill
	dead                int32
	killCh              chan bool
	executorKilled      atomic.Bool
	configFetcherKilled atomic.Bool
	sendShardsKilled    atomic.Bool

	lastAppliedIndex int
}

func (kv *ShardKV) DPrintf(format string, a ...interface{}) (n int, err error) {
	_, isLeader := kv.rf.GetState()
	if isLeader {
		DPrintf(format, a...)
	}
	return
}

// ShouldStartCommand returns whether to accept this RPC
func (kv *ShardKV) ShouldStartCommand(clientId int64, newSeq int32, err *Err, value *string) bool {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		*err = ErrWrongLeader
		return false
	}
	kv.rwLock.RLock()
	defer kv.rwLock.RUnlock()
	seq, ok := kv.dedupTable[clientId]
	if ok {
		switch {
		case newSeq == seq:
			*err = OK
			if value != nil {
				*value = kv.valueTable[clientId]
			}
			return false
		case newSeq < seq:
			*err = kvraft.ErrStaleRequest
			return false
		case newSeq > seq:
			return true
		}
	}
	// we should start command if client ID not in dedup table
	return true
}

func (kv *ShardKV) RecordRequestAtIndex(index int, id kvraft.RequestId, failCh chan bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// there could be multiple requests from different clients accepted by current leader, and indices recorded into `pendingRequests`
	// and then it's no longer a leader, thus these entries are finally overridden by another new leader
	// at commit stage, current server will detect different operations are committed at these recorded indices
	// thus wake up corresponding RPC request and fail them to force client retry.
	kv.pendingRequests[index] = kvraft.RequestInfo{RequestId: id, FailCh: failCh}
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	_, _, isLeader := kv.rf.Start(Op{
		Op:        InstallShard,
		From:      kv.me,
		ShardArgs: *args,
	})
	reply.Success = isLeader
}

func (kv *ShardKV) AddShardToState(data map[string]string) {
	for key, value := range data {
		// TODO: deletion detection
		kv.state[key] = value
	}
}

func (kv *ShardKV) HandleInstallShard(op Op) {
	// each time we receive a shard from other replica group, we're in:
	// 1. waiting for this shard data before able to serve it
	// 2. already got this shard and serving it, then we should ignore any duplication (duplication can happen when leader crash after log commit but before return to client)
	// 3. not yet detect the config change, thus not expecting to receive the shard; we should still save it for future acknowledge
	cfg := kv.config.Load()
	// ignore any shard data from previous config num
	if op.ShardArgs.Num < cfg.Num {
		return
	}
	raft.TraceInstant("InstallShard", kv.me, kv.gid, time.Now().UnixMicro(), map[string]any{
		"GID":           kv.gid,
		"op":            fmt.Sprintf("%+v", op),
		"config":        fmt.Sprintf("%+v", *cfg),
		"shardsToRecv":  fmt.Sprintf("%v", keys(kv.shardsToRecv)),
		"pendingShards": fmt.Sprintf("%v", keys(kv.pendingShards)),
	})
	// first check if we serve this shard in current config
	if cfg.Shards[op.ShardArgs.Shard] == kv.gid {
		// are we still waiting for this shard?
		if _, ok := kv.shardsToRecv[op.ShardArgs.Shard]; ok {
			// we only apply the shard if it is for current config num
			if op.ShardArgs.Num == cfg.Num {
				kv.AddShardToState(op.ShardArgs.Data)
				delete(kv.shardsToRecv, op.ShardArgs.Shard)
				return
			}
		} else {
			// if not waiting for this shard, this might be a duplication and can be ignored
			return
		}
	}
	// if in current config we're not serving this shard at all, simply record it for future use
	kv.pendingShards[op.ShardArgs.ShardInfo()] = op.ShardArgs.Data
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	if !kv.ShouldStartCommand(args.ClientId, args.SeqNumber, &reply.Err, &reply.Value) {
		return
	}
	resultCh := make(chan Result, 1)
	failCh := make(chan bool, 1)
	index, _, isLeader := kv.rf.Start(Op{
		Key:       args.Key,
		Op:        kvraft.GetOp,
		ResultCh:  resultCh,
		From:      kv.me,
		ClientId:  args.ClientId,
		SeqNumber: args.SeqNumber,
	})
	if isLeader {
		DPrintf("[%d][%d] Get start [%d] ClientId:%d SeqNumber:%d", kv.gid, kv.me, index, args.ClientId, args.SeqNumber)
		defer DPrintf("[%d][%d] Get end [%d] ClientId:%d SeqNumber:%d", kv.gid, kv.me, index, args.ClientId, args.SeqNumber)
		kv.RecordRequestAtIndex(index, kvraft.RequestId{ClientId: args.ClientId, SeqNumber: args.SeqNumber}, failCh)
		// block until it's committed
		select {
		case <-failCh:
			reply.Err = kvraft.ErrLostLeadership
		case result := <-resultCh:
			if result.Valid {
				reply.Value = result.Value
				reply.Err = OK
			} else {
				reply.Err = ErrWrongGroup
			}
		case <-time.After(time.Second * kvraft.RPCTimeout):
			reply.Err = kvraft.ErrTimeOut
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if !kv.ShouldStartCommand(args.ClientId, args.SeqNumber, &reply.Err, nil) {
		return
	}
	resultCh := make(chan Result, 1)
	failCh := make(chan bool, 1)
	index, _, isLeader := kv.rf.Start(Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ResultCh:  resultCh,
		From:      kv.me,
		ClientId:  args.ClientId,
		SeqNumber: args.SeqNumber,
	})
	if isLeader {
		DPrintf("[%d][%d] PutAppend start [%d] ClientId:%d SeqNumber:%d", kv.gid, kv.me, index, args.ClientId, args.SeqNumber)
		defer DPrintf("[%d][%d] PutAppend end [%d] ClientId:%d SeqNumber:%d", kv.gid, kv.me, index, args.ClientId, args.SeqNumber)
		kv.RecordRequestAtIndex(index, kvraft.RequestId{ClientId: args.ClientId, SeqNumber: args.SeqNumber}, failCh)
		// block until it's committed
		select {
		case <-failCh:
			reply.Err = kvraft.ErrLostLeadership
		case result := <-resultCh:
			if result.Valid {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongGroup
			}
		case <-time.After(time.Second * kvraft.RPCTimeout):
			reply.Err = kvraft.ErrTimeOut
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) DaemonConfigFetcher() {
	// this is only thread modifies config
	defer kv.configFetcherKilled.Store(true)
	for !kv.killed() {
		newConfig := kv.mck.Query(-1)
		activeConfig := kv.config.Load()
		// there's a config change happened
		if newConfig.Num != activeConfig.Num {
			for num := activeConfig.Num + 1; num <= newConfig.Num; num += 1 {
				cfg := kv.mck.Query(num)
				// only the leader will send the config change he detected to all servers in replica group successfully
				kv.rf.Start(Op{
					Op:        ConfigChange,
					NewConfig: &cfg,
					From:      kv.me,
				})
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (kv *ShardKV) SendShardImpl(data ShardData) {
	start := time.Now().UnixMilli()
	for {
		if time.Now().UnixMilli()-start > kvraft.RPCTimeout*1000 {
			panic(fmt.Sprintf("Failed to send shard to replica group, servers: %v", data.Servers))
		}
		for _, server := range data.Servers {
			srv := kv.make_end(server)
			var reply InstallShardReply
			ok := srv.Call("ShardKV.InstallShard", &data.Arg, &reply)
			if ok && reply.Success == true {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) DaemonSendShard() {
	defer kv.sendShardsKilled.Store(true)
	for !kv.killed() {
		kv.muSendShards.Lock()
		for len(kv.shardsToSend) == 0 && !kv.killed() {
			kv.condSendShards.Wait()
		}
		shardsToSend := kv.shardsToSend
		kv.shardsToSend = make([]ShardData, 0)
		kv.muSendShards.Unlock()
		// do the actual RPC after release lock
		for _, data := range shardsToSend {
			kv.SendShardImpl(data)
		}
	}
}

func (kv *ShardKV) SendShards(shards []int, newConfig *shardctrler.Config) {
	// not lock needed here to access the state
	shardData := make(map[int]ShardData)
	for _, shard := range shards {
		arg := InstallShardArgs{Shard: shard, Data: make(map[string]string), From: kv.gid, Num: newConfig.Num}
		shardData[shard] = ShardData{Arg: arg, Servers: newConfig.Groups[newConfig.Shards[shard]]}
	}
	// copy shard values from current state
	for key, value := range kv.state {
		shard := key2shard(key)
		data, ok := shardData[shard]
		if ok {
			data.Arg.Data[key] = value
			delete(kv.state, key)
		}
	}
	kv.muSendShards.Lock()
	for _, data := range shardData {
		kv.shardsToSend = append(kv.shardsToSend, data)
	}
	kv.condSendShards.Signal()
	kv.muSendShards.Unlock()
}

func (kv *ShardKV) IsConfigChangeValid(op Op) bool {
	// this check ensures config update is happening monotonically
	// also, we can only advance to next config if we have received all pending shards
	return kv.config.Load().Num+1 == op.NewConfig.Num && len(kv.shardsToRecv) == 0
}

func (kv *ShardKV) HandleConfigChange(op Op) {
	// not lock needed here to access the state
	// dedup for config change operation
	if !kv.IsConfigChangeValid(op) {
		kv.skippedConfigChangeOp += 1
		return
	}
	activeConfig := kv.config.Load()
	shardsToSend := make([]int, 0, shardctrler.NShards)
	for shard := 0; shard < shardctrler.NShards; shard++ {
		if activeConfig.Shards[shard] == kv.gid && op.NewConfig.Shards[shard] != kv.gid {
			shardsToSend = append(shardsToSend, shard)
		}
		// we don't need to wait for someone send a shard if this shard never served by other replica group before
		if activeConfig.Shards[shard] != 0 {
			if activeConfig.Shards[shard] != kv.gid && op.NewConfig.Shards[shard] == kv.gid {
				key := ShardInfo{Shard: shard, Num: op.NewConfig.Num}
				if data, ok := kv.pendingShards[key]; ok {
					kv.AddShardToState(data)
					delete(kv.pendingShards, key)
				} else {
					kv.shardsToRecv[shard] = true
				}
			}
		}
	}
	if len(shardsToSend) != 0 {
		kv.SendShards(shardsToSend, op.NewConfig)
	}
	raft.TraceInstant("ConfigChange", kv.me, kv.gid, time.Now().UnixMicro(), map[string]any{
		"GID":           kv.gid,
		"shardsToSend":  fmt.Sprintf("%v", shardsToSend),
		"shardsToRecv":  fmt.Sprintf("%v", keys(kv.shardsToRecv)),
		"pendingShards": fmt.Sprintf("%v", keys(kv.pendingShards)),
		"activeConfig":  fmt.Sprintf("%+v", *kv.config.Load()),
		"newConfig":     fmt.Sprintf("%+v", *op.NewConfig),
	})
	kv.config.Store(op.NewConfig)
}

func (kv *ShardKV) OperationExecutor() {
	for !kv.killed() {
		select {
		// receives committed raft log entry
		case cmd := <-kv.applyCh:
			if cmd.TermChanged {
				kv.FailAllPendingRequests()
				continue
			}
			if cmd.SnapshotValid && cmd.SnapshotIndex > kv.lastAppliedIndex {
				kv.ReloadFromSnapshot(cmd.Snapshot)
				kv.lastAppliedIndex = cmd.SnapshotIndex
				continue
			}
			if !cmd.CommandValid {
				continue
			}
			kv.FailConflictPendingRequests(cmd)
			if cmd.CommandIndex <= kv.lastAppliedIndex {
				continue
			}
			kv.lastAppliedIndex = cmd.CommandIndex
			op := cmd.Command.(Op)
			switch op.Op {
			case ConfigChange:
				kv.DPrintf("[%d][%d] Config Change [%d] Config: %+v", kv.gid, kv.me, cmd.CommandIndex, *op.NewConfig)
				kv.HandleConfigChange(op)
			case InstallShard:
				kv.DPrintf("[%d][%d] Install Shard [%d] Shard: %d, Value: %v", kv.gid, kv.me, cmd.CommandIndex, op.ShardArgs.Shard, op.ShardArgs.Data)
				kv.HandleInstallShard(op)
			default:
				kv.DPrintf("[%d][%d] Apply command [%d] [%+v]", kv.gid, kv.me, cmd.CommandIndex, op)
				cfg := kv.config.Load()
				shard := key2shard(op.Key)
				// do not apply operation if not owing the shard or waiting to receive this shard
				// then the client request will fail and it will finally retry
				_, shardNotReady := kv.shardsToRecv[shard]
				// it is tricky here that if we update the dedup stable status, we should also increase the seqNumber once client received ErrWrongGroup
				// we have to choose to do both or neither, otherwise the previous failed request won't be properly retried once we're ready to serve this shard
				if cfg.Shards[shard] != kv.gid || shardNotReady {
					if op.From == kv.me && op.ResultCh != nil {
						op.ResultCh <- Result{Valid: false}
					}
				} else {
					result := kv.ApplyOperation(op)
					// only notify completion when request waiting on same server and channel available
					// we also need to ensure `ResultCh` is not nil, because if server restarts before this entry committed
					// log will be reloaded from persistent state and channel will be set to nil since it's non-serializable
					// if the source server happened to become leader again to commit this entry, it will pass first check and cause dead lock in Raft
					if op.From == kv.me && op.ResultCh != nil {
						op.ResultCh <- Result{Valid: true, Value: result}
					}
				}
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.DoSnapshot(cmd)
			}
		case killed := <-kv.killCh:
			if killed {
				break
			}
		}
	}
	kv.executorKilled.Store(true)
}

func (kv *ShardKV) FailAllPendingRequests() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for k, v := range kv.pendingRequests {
		v.FailCh <- true
		delete(kv.pendingRequests, k)
	}
}

func (kv *ShardKV) FailConflictPendingRequests(cmd raft.ApplyMsg) {
	op := cmd.Command.(Op)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	info, ok := kv.pendingRequests[cmd.CommandIndex]
	if ok && info.RequestId != op.RequestId() {
		info.FailCh <- true
	}
	delete(kv.pendingRequests, cmd.CommandIndex)
}

func (kv *ShardKV) ApplyOperation(op Op) string {
	result := ""
	// No lock need here because the only competing goroutine is read only
	seq, ok := kv.dedupTable[op.ClientId]
	if ok && op.SeqNumber == seq {
		return kv.valueTable[op.ClientId]
	}
	// Q: will there be op.SeqNumber < seq?
	// no, because the sequence numbers occurred at commit stage is non-decreasing, it can only have duplicate caused by leader crash
	// this is guaranteed by the fact that client will only increase sequence number when previous RPC has finished
	// which means all lower sequence numbers have been committed for at least once
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()
	if op.Op == kvraft.PutOp {
		kv.state[op.Key] = op.Value
	} else {
		value, ok_ := kv.state[op.Key]
		if !ok_ {
			value = ""
		}
		switch op.Op {
		case kvraft.AppendOp:
			kv.state[op.Key] = value + op.Value
		case kvraft.GetOp:
			result = value
		}
	}
	raft.TraceInstant("Apply", kv.me, kv.gid, time.Now().UnixMicro(), map[string]any{
		"GID":    kv.gid,
		"op":     fmt.Sprintf("%+v", op),
		"state":  kv.state[op.Key],
		"config": fmt.Sprintf("%+v", *kv.config.Load()),
	})
	kv.dedupTable[op.ClientId] = op.SeqNumber
	kv.valueTable[op.ClientId] = result
	return result
}

func (kv *ShardKV) DoSnapshot(cmd raft.ApplyMsg) {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	if err := e.Encode(kv.lastAppliedIndex); err != nil {
		log.Fatal(err)
	}
	if err := e.Encode(kv.state); err != nil {
		log.Fatal(err)
	}
	if err := e.Encode(kv.dedupTable); err != nil {
		log.Fatal(err)
	}
	if err := e.Encode(kv.valueTable); err != nil {
		log.Fatal(err)
	}
	if err := e.Encode(*kv.config.Load()); err != nil {
		log.Fatal(err)
	}
	if err := e.Encode(kv.shardsToRecv); err != nil {
		log.Fatal(err)
	}
	if err := e.Encode(kv.pendingShards); err != nil {
		log.Fatal(err)
	}
	kv.muSendShards.Lock()
	if err := e.Encode(kv.shardsToSend); err != nil {
		log.Fatal(err)
	}
	kv.muSendShards.Unlock()
	state := buf.Bytes()
	kv.rf.Snapshot(cmd.CommandIndex, state)
}

func (kv *ShardKV) ReloadFromSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastAppliedIndex int
	var state map[string]string
	var dedupTable map[int64]int32
	var valueTable map[int64]string
	var cfg shardctrler.Config
	var shardsToRecv map[int]bool
	var pendingShards map[ShardInfo]map[string]string
	var shardsToSend []ShardData
	if d.Decode(&lastAppliedIndex) != nil || d.Decode(&state) != nil || d.Decode(&dedupTable) != nil || d.Decode(&valueTable) != nil || d.Decode(&cfg) != nil || d.Decode(&shardsToRecv) != nil || d.Decode(&pendingShards) != nil || d.Decode(&shardsToSend) != nil {
		panic("Failed to reload persisted snapshot into application.")
	}
	// configuration update related states
	kv.config.Store(&cfg)
	kv.shardsToRecv = shardsToRecv
	kv.pendingShards = pendingShards
	kv.muSendShards.Lock()
	kv.shardsToSend = shardsToSend
	kv.muSendShards.Unlock()
	// KV server state machine related
	kv.rwLock.Lock()
	kv.state = state
	kv.dedupTable = dedupTable
	kv.valueTable = valueTable
	kv.lastAppliedIndex = lastAppliedIndex
	kv.rwLock.Unlock()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.killCh <- true
	kv.condSendShards.Broadcast()
	go func(start int64) {
		for !kv.CheckKillComplete() {
			time.Sleep(time.Millisecond * 100)
			timeout := int64(10)
			if time.Now().UnixMilli()-start > timeout*1000 {
				log.Printf("Spent more than %ds to kill ShardKV %p", timeout, kv)
			}
		}
	}(time.Now().UnixMilli())
}

func (kv *ShardKV) CheckKillComplete() bool {
	return kv.executorKilled.Load() && kv.configFetcherKilled.Load() && kv.sendShardsKilled.Load()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.lastAppliedIndex = 0
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.killCh = make(chan bool, 10)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.GID = kv.gid
	// KV server states
	kv.state = make(map[string]string)
	kv.dedupTable = make(map[int64]int32)
	kv.valueTable = make(map[int64]string)
	kv.pendingRequests = make(map[int]kvraft.RequestInfo)
	// config migration related
	kv.shardsToRecv = make(map[int]bool)
	kv.pendingShards = make(map[ShardInfo]map[string]string)
	kv.condSendShards = sync.NewCond(&kv.muSendShards)
	// load config
	cfg := kv.mck.Query(0)
	kv.config.Store(&cfg)
	// we might override the config with snapshot
	kv.ReloadFromSnapshot(persister.ReadSnapshot())

	go kv.OperationExecutor()
	go kv.DaemonConfigFetcher()
	go kv.DaemonSendShard()
	return kv
}
