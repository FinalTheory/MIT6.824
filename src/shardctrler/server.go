package shardctrler

import (
	"6.5840/kvraft"
	"6.5840/raft"
	"fmt"
	"log"
	"reflect"
	"slices"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type DedupEntry struct {
	SeqNumber int32
	Value     *Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	rwLock     sync.RWMutex
	dedupTable map[int64]DedupEntry

	pendingRequests  map[int]kvraft.RequestInfo
	lastAppliedIndex int
	configs          []Config // indexed by config num

	// states related to gracefully kill
	killCh         chan bool
	executorKilled atomic.Bool
}

type Op struct {
	Type       string
	ToClientCh chan *Config
	From       int
	Join       JoinArgs
	Move       MoveArgs
	Leave      LeaveArgs
	Query      QueryArgs
}

func (op *Op) ClientId() int64 {
	switch op.Type {
	case JoinOp:
		return op.Join.ClientId
	case LeaveOp:
		return op.Leave.ClientId
	case MoveOp:
		return op.Move.ClientId
	case QueryOp:
		return op.Query.ClientId
	}
	panic("Invalid op type")
}

func (op *Op) SeqNumber() int32 {
	switch op.Type {
	case JoinOp:
		return op.Join.SeqNumber
	case LeaveOp:
		return op.Leave.SeqNumber
	case MoveOp:
		return op.Move.SeqNumber
	case QueryOp:
		return op.Query.SeqNumber
	}
	panic("Invalid op type")
}

func (op *Op) RequestId() kvraft.RequestId {
	return kvraft.RequestId{ClientId: op.ClientId(), SeqNumber: op.SeqNumber()}
}

// shouldStartCommand returns whether to accept this RPC
func (sc *ShardCtrler) shouldStartCommand(clientId int64, newSeq int32, err *Err, wrongLeader *bool, value *Config) bool {
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		*wrongLeader = true
		return false
	}
	sc.rwLock.RLock()
	defer sc.rwLock.RUnlock()
	entry, ok := sc.dedupTable[clientId]
	if ok {
		switch {
		case newSeq == entry.SeqNumber:
			*err = OK
			if value != nil {
				*value = *entry.Value
			}
			return false
		case newSeq < entry.SeqNumber:
			*err = kvraft.ErrStaleRequest
			return false
		case newSeq > entry.SeqNumber:
			return true
		}
	}
	// we should start command if client ID not in dedup table
	return true
}

func (sc *ShardCtrler) recordRequestAtIndex(index int, id kvraft.RequestId, failCh chan kvraft.Err) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// there could be multiple requests from different clients accepted by current leader, and indices recorded into `pendingRequests`
	// and then it's no longer a leader, thus these entries are finally overridden by another new leader
	// at commit stage, current server will detect different operations are committed at these recorded indices
	// thus wake up corresponding RPC request and fail them to force client retry.
	sc.pendingRequests[index] = kvraft.RequestInfo{RequestId: id, FailCh: failCh}
}

func (sc *ShardCtrler) requestHandler(op Op, err *Err, wrongLeader *bool, value *Config) {
	requestId := op.RequestId()
	if !sc.shouldStartCommand(requestId.ClientId, requestId.SeqNumber, err, wrongLeader, value) {
		return
	}
	failCh := make(chan kvraft.Err, 1)
	op.ToClientCh = make(chan *Config, 1)
	op.From = sc.me

	index, _, isLeader := sc.rf.Start(op)
	if isLeader {
		DPrintf("[%s][%d] requestHandler start index=%d ClientId:%d SeqNumber:%d", op.Type, sc.me, index, requestId.ClientId, requestId.SeqNumber)
		defer DPrintf("[%s][%d] requestHandler end index=%d ClientId:%d SeqNumber:%d", op.Type, sc.me, index, requestId.ClientId, requestId.SeqNumber)
		sc.recordRequestAtIndex(index, kvraft.RequestId{ClientId: requestId.ClientId, SeqNumber: requestId.SeqNumber}, failCh)
		*wrongLeader = false
		// block until it's committed
		select {
		case e := <-failCh:
			*err = Err(e)
			*wrongLeader = true
		case cfg := <-op.ToClientCh:
			*err = OK
			if value != nil {
				*value = *cfg
			}
		case <-time.After(time.Second * kvraft.RPCTimeout):
			*err = kvraft.ErrTimeOut
			*wrongLeader = true
		}
	} else {
		*err = kvraft.ErrWrongLeader
		*wrongLeader = true
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Type: JoinOp,
		Join: *args,
	}
	sc.requestHandler(op, &reply.Err, &reply.WrongLeader, nil)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Type:  LeaveOp,
		Leave: *args,
	}
	sc.requestHandler(op, &reply.Err, &reply.WrongLeader, nil)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Type: MoveOp,
		Move: *args,
	}
	sc.requestHandler(op, &reply.Err, &reply.WrongLeader, nil)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Type:  QueryOp,
		Query: *args,
	}
	sc.requestHandler(op, &reply.Err, &reply.WrongLeader, &reply.Config)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	sc.killCh <- true
	sc.failAllPendingRequests(kvraft.ErrKilled)
	raft.CheckKillFinish(10, func() bool { return sc.executorKilled.Load() }, sc)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) stateMachineExecutor() {
	defer sc.executorKilled.Store(true)
	for !sc.killed() {
		DPrintf("[%d] waiting for op", sc.me)
		select {
		// receives committed raft log entry
		case cmd := <-sc.applyCh:
			if cmd.TermChanged {
				sc.failAllPendingRequests(kvraft.ErrLostLeadership)
				continue
			}
			if !cmd.CommandValid {
				continue
			}
			sc.failConflictPendingRequests(cmd)
			if cmd.CommandIndex <= sc.lastAppliedIndex {
				panic("unexpected")
			}
			op := cmd.Command.(Op)
			DPrintf("[%d] Apply command [%d] [%+v]", sc.me, cmd.CommandIndex, op)
			result := sc.applyOperation(op)
			sc.lastAppliedIndex = cmd.CommandIndex
			if op.From == sc.me && op.ToClientCh != nil {
				op.ToClientCh <- result
			}
		case killed := <-sc.killCh:
			if killed {
				break
			}
		}
	}
}

func (sc *ShardCtrler) failAllPendingRequests(err kvraft.Err) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for k, v := range sc.pendingRequests {
		v.FailCh <- err
		delete(sc.pendingRequests, k)
	}
}

func (sc *ShardCtrler) failConflictPendingRequests(cmd raft.ApplyMsg) {
	op := cmd.Command.(Op)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	info, ok := sc.pendingRequests[cmd.CommandIndex]
	if ok && info.RequestId != op.RequestId() {
		info.FailCh <- kvraft.ErrLostLeadership
	}
	delete(sc.pendingRequests, cmd.CommandIndex)
}

func (sc *ShardCtrler) applyOperation(op Op) *Config {
	emptyConfig := Config{}
	result := &emptyConfig
	entry, ok := sc.dedupTable[op.ClientId()]
	if ok && op.SeqNumber() == entry.SeqNumber {
		return entry.Value
	}
	sc.rwLock.Lock()
	defer sc.rwLock.Unlock()
	switch op.Type {
	case JoinOp:
		sc.joinImpl(&op.Join)
	case LeaveOp:
		sc.leaveImpl(&op.Leave)
	case MoveOp:
		sc.moveImpl(&op.Move)
	case QueryOp:
		result = sc.queryImpl(&op.Query)
	default:
		panic("Invalid op type")
	}
	raft.TraceInstant("Apply", sc.me, 0, time.Now().UnixMicro(), map[string]any{
		"op":           fmt.Sprintf("%+v", op),
		"latestConfig": fmt.Sprintf("%+v", sc.configs[len(sc.configs)-1]),
	})
	sc.dedupTable[op.ClientId()] = DedupEntry{Value: result, SeqNumber: op.SeqNumber()}
	return result
}

func copyConfig(src *Config) *Config {
	dst := Config{}
	dst.Num = src.Num
	for i := 0; i < NShards; i++ {
		dst.Shards[i] = src.Shards[i]
	}
	dst.Groups = make(map[int][]string)
	for k, v := range src.Groups {
		values := make([]string, len(v))
		copy(values, v)
		dst.Groups[k] = values
	}
	return &dst
}

func ReBalance(cfg *Config) {
	DPrintf("Before Rebalance: %+v", *cfg)
	if len(cfg.Groups) == 0 {
		return
	}
	// most groups should own `expected` shards
	expected := NShards / len(cfg.Groups)
	// num of groups allowed to have `expected + 1` shards due to remainderGroups
	remainder := NShards % len(cfg.Groups)
	// and a set to deduplicate those GIDs
	remainderGroups := make(map[int]bool)

	// 0. Collect all GIDs
	gids := make([]int, 0, len(cfg.Groups))
	for gid, _ := range cfg.Groups {
		gids = append(gids, gid)
	}
	slices.Sort(gids)
	// 1. Count shards by GID
	gidToShards := make(map[int]int)
	for _, gid := range gids {
		gidToShards[gid] = 0
	}
	for i := 0; i < NShards; i++ {
		if cfg.Shards[i] > 0 {
			gidToShards[cfg.Shards[i]] += 1
		}
	}
	// 2. Collect shards that needs to be re-allocated
	shardsToAssign := make([]int, 0, NShards)
	for i := 0; i < NShards; i++ {
		gid := &cfg.Shards[i]
		if *gid <= 0 {
			shardsToAssign = append(shardsToAssign, i)
			*gid = 0
		} else {
			if gidToShards[*gid] > expected {
				_, exists := remainderGroups[*gid]
				if gidToShards[*gid] == expected+1 && (len(remainderGroups) < remainder || exists) {
					remainderGroups[*gid] = true
					continue
				}
				shardsToAssign = append(shardsToAssign, i)
				gidToShards[*gid] -= 1
				*gid = 0
			}
		}
	}
	// 3. Allocate the dangling shards
	for len(shardsToAssign) > 0 {
		for _, gid := range gids {
			if len(shardsToAssign) == 0 {
				break
			}
			allowOneMore := gidToShards[gid] == expected && len(remainderGroups) < remainder
			if gidToShards[gid] < expected || allowOneMore {
				if allowOneMore {
					remainderGroups[gid] = true
				}
				shard := shardsToAssign[0]
				shardsToAssign = shardsToAssign[1:]
				cfg.Shards[shard] = gid
				gidToShards[gid] += 1
			}
		}
	}
	// 4. Additional check
	for i := 0; i < NShards; i++ {
		if cfg.Shards[i] == 0 {
			panic(fmt.Sprintf("Shard %d is not allocated!", i))
		}
	}
	DPrintf("After Rebalance: %+v %v", *cfg, gidToShards)
}

func (sc *ShardCtrler) joinImpl(args *JoinArgs) {
	cfg := copyConfig(&sc.configs[len(sc.configs)-1])
	cfg.Num += 1
	for gid, servers := range args.Servers {
		currentServers, ok := cfg.Groups[gid]
		if ok {
			slices.Sort(currentServers)
			slices.Sort(servers)
			if !reflect.DeepEqual(currentServers, servers) {
				panic(fmt.Sprintf("[%d] Redundant GID %d, current servers: %v, new servers: %v", sc.me, gid, currentServers, servers))
			}
		}
		cfg.Groups[gid] = servers
	}
	ReBalance(cfg)
	sc.configs = append(sc.configs, *cfg)
}

func (sc *ShardCtrler) leaveImpl(args *LeaveArgs) {
	cfg := copyConfig(&sc.configs[len(sc.configs)-1])
	cfg.Num += 1
	for _, gid := range args.GIDs {
		delete(cfg.Groups, gid)
	}
	validGID := 0
	for i := 0; i < NShards; i++ {
		if _, ok := cfg.Groups[cfg.Shards[i]]; ok {
			validGID = cfg.Shards[i]
			break
		}
	}
	for i := 0; i < NShards; i++ {
		if _, ok := cfg.Groups[cfg.Shards[i]]; !ok {
			cfg.Shards[i] = validGID
		}
	}
	ReBalance(cfg)
	sc.configs = append(sc.configs, *cfg)
}

func (sc *ShardCtrler) moveImpl(args *MoveArgs) {
	cfg := copyConfig(&sc.configs[len(sc.configs)-1])
	cfg.Num += 1
	cfg.Shards[args.Shard] = args.GID
	ReBalance(cfg)
	sc.configs = append(sc.configs, *cfg)
}

func (sc *ShardCtrler) queryImpl(args *QueryArgs) *Config {
	i := args.Num
	if args.Num < 0 || args.Num >= len(sc.configs) {
		i = len(sc.configs) - 1
	}
	return copyConfig(&sc.configs[i])
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.configs = make([]Config, 1)
	sc.configs[0].Num = 0
	sc.configs[0].Groups = map[int][]string{}
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.killCh = make(chan bool, 10)
	sc.lastAppliedIndex = 0
	sc.dedupTable = make(map[int64]DedupEntry)
	sc.pendingRequests = make(map[int]kvraft.RequestInfo)
	go sc.stateMachineExecutor()
	return sc
}
