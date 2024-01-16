package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func merge(maps ...map[string]any) map[string]any {
	merged := make(map[string]any)
	for _, m := range maps {
		for k, v := range m {
			merged[k] = v
		}
	}
	return merged
}

func trace() string {
	pc, _, _, ok := runtime.Caller(2)
	if !ok {
		return "?"
	}

	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return "?"
	}
	return fn.Name()
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	TermChanged  bool
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Leader Role = iota
	Follower
	Candidate
)

// use -1 to represent null
const Null = -1

func (r Role) String() string {
	switch r {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return ""
	}
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type LogContainer struct {
	Data              []LogEntry
	StartFrom         int
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (c *LogContainer) Put(entry LogEntry) int {
	c.Data = append(c.Data, entry)
	return c.LastLogIndex()
}

func (c *LogContainer) Append(entry []LogEntry) {
	c.Data = append(c.Data, entry...)
}

func (c *LogContainer) Length() int {
	return len(c.Data) + c.StartFrom
}

func (c *LogContainer) LastLogIndex() int {
	return c.StartFrom + len(c.Data) - 1
}

func (c *LogContainer) LastLogTerm() int {
	if c.IsEmpty() {
		return -1
	} else {
		return c.TermAt(c.LastLogIndex())
	}
}

func (c *LogContainer) SliceFrom(start int) []LogEntry {
	actualStart := start - c.StartFrom
	if actualStart > c.Length() || actualStart < 0 {
		panic(fmt.Sprintf("Invalid slice start %d", actualStart))
	}
	return c.Data[actualStart:]
}

func (c *LogContainer) TruncateTo(to int) {
	actualTo := to - c.StartFrom
	if actualTo > len(c.Data) || actualTo < 0 {
		panic(fmt.Sprintf("Invalid truncate end %d", actualTo))
	}
	c.Data = c.Data[:actualTo]
}

// TruncateFrom keep the data starting from the index
func (c *LogContainer) TruncateFrom(from int) {
	actualFrom := from - c.StartFrom
	if actualFrom < 0 || actualFrom > len(c.Data) {
		panic(fmt.Sprintf("Invalid truncate start %d", actualFrom))
	}
	c.Data = c.Data[actualFrom:]
	c.StartFrom = from
}

func (c *LogContainer) EntryValidAt(index int) bool {
	return index >= c.StartFrom && index < c.Length()
}

func (c *LogContainer) TermValidAt(index int) bool {
	return index >= max(c.LastIncludedIndex, 0) && index < c.Length()
}

// TermAt return -1 if the log entry term at index not found
func (c *LogContainer) TermAt(index int) int {
	if index >= c.Length() {
		panic(fmt.Sprintf("Invalid term index %d, valid: [%d, %d)", index, c.LastIncludedIndex, c.Length()))
	}
	if index < 0 {
		return -1
	}
	if index == c.LastIncludedIndex {
		return c.LastIncludedTerm
	}
	if index >= c.StartFrom && index < c.Length() {
		return c.Get(index).Term
	}
	return -1
}

func (c *LogContainer) Get(index int) LogEntry {
	realIndex := index - c.StartFrom
	if realIndex >= len(c.Data) || realIndex < 0 {
		panic(fmt.Sprintf("Invalid get index %d", realIndex))
	}
	return c.Data[realIndex]
}

func (c *LogContainer) IsEmpty() bool {
	return c.Length() == 0
}

func (c *LogContainer) SnapshotTo(index int) {
	// notice this index is internal and starts from 0
	c.LastIncludedIndex = index
	c.LastIncludedTerm = c.Get(index).Term
	// snapshot has all entry include `index` applied, we can safely truncate them
	c.TruncateFrom(index + 1)
}

func (c *LogContainer) Reset(lastIncludedIndex, lastIncludedTerm int) {
	c.TruncateFrom(c.Length())
	c.StartFrom = lastIncludedIndex + 1
	c.LastIncludedIndex = lastIncludedIndex
	c.LastIncludedTerm = lastIncludedTerm
}

func (c *LogContainer) GetTraceState() map[string]any {
	return map[string]any{
		"log.Size":              c.Length(),
		"log.ActualSize":        len(c.Data),
		"log.LastIncludedIndex": c.LastIncludedIndex,
		"log.LastIncludedTerm":  c.LastIncludedTerm,
		"log.StartFrom":         c.StartFrom,
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	lockOwner string
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   *chan ApplyMsg

	// Your Data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	votedFor    int
	voteCount   int
	currentTerm int
	commitIndex int
	lastApplied int

	nextIndex         []int
	matchIndex        []int
	condAppendEntries *sync.Cond
	condApplyEntries  *sync.Cond

	log      LogContainer
	snapshot []byte

	lastReceivedRPC int64
	electionTimeout int64

	// states used to kill raft service
	tickerKilled    atomic.Bool
	heartbeatKilled atomic.Bool
	daemonKilled    []atomic.Bool
	killCh          chan bool
	killChSize      int
}

func (rf *Raft) DebugLock() {
	for !rf.mu.TryLock() {
		log.Printf("[%p][%d] Failed to acquire lock hold by %s", rf, rf.me, rf.lockOwner)
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
	rf.lockOwner = trace()
}

func (rf *Raft) Unlock() {
	rf.lockOwner = ""
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	term := rf.currentTerm
	isLeader := rf.role == Leader
	rf.Unlock()
	return term, isLeader
}

func (rf *Raft) GetTraceState() map[string]any {
	return merge(rf.log.GetTraceState(), map[string]any{
		"raft.currentTerm": rf.currentTerm,
		"raft.commitIndex": rf.commitIndex,
		"raft.lastApplied": rf.lastApplied,
	})
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	if err := e.Encode(rf.log); err != nil {
		log.Fatal(err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		log.Fatal(err)
	}
	if err := e.Encode(rf.currentTerm); err != nil {
		log.Fatal(err)
	}
	state := buf.Bytes()
	rf.persister.Save(state, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor, currentTerm int
	c := LogContainer{}
	if d.Decode(&c) != nil || d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil {
		panic("Failed to reload persisted state.")
	}
	rf.votedFor = votedFor
	rf.currentTerm = currentTerm
	rf.log = c
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// notice this index from tester and start from 1
	func(idx int, s []byte) {
		rf.Lock()
		defer rf.Unlock()
		if idx < rf.log.StartFrom {
			return
		}
		TraceInstant("Snapshot", rf.me, time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
			"index":        idx,
			"snapshotSize": len(s),
		}))
		// the index here is internal and start from 0
		rf.log.SnapshotTo(idx)
		rf.snapshot = s
		rf.persist()
	}(index-1, snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

type AppendEntriesReply struct {
	Term                int
	Success             bool
	XTerm, XIndex, XLen int
}

type LastLogInfo struct {
	index, term int
}

func MoreOrEqualUpToDateThan(lhs LastLogInfo, rhs LastLogInfo) bool {
	if lhs.index == -1 && rhs.index == -1 {
		return true
	} else if lhs.index == -1 {
		return false
	} else if rhs.index == -1 {
		return true
	} else {
		if lhs.term != rhs.term {
			return lhs.term >= rhs.term
		} else {
			return lhs.index >= rhs.index
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()
	rf.HandleTermUpdateLocked(args.Term)
	reply.Term = rf.currentTerm
	// candidate's term is valid
	if args.Term >= rf.currentTerm {
		if rf.votedFor == args.CandidateId || rf.votedFor == Null {
			candidateLastLogInfo := LastLogInfo{index: args.LastLogIndex, term: args.LastLogTerm}
			if MoreOrEqualUpToDateThan(candidateLastLogInfo, rf.GetLastLogInfoLocked()) {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				rf.lastReceivedRPC = time.Now().UnixMilli()
				TraceInstant("Vote", rf.me, time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
					"voteFor":              args.CandidateId,
					"selfLastLogInfo":      fmt.Sprintf("%v", rf.GetLastLogInfoLocked()),
					"candidateLastLogInfo": fmt.Sprintf("%v", candidateLastLogInfo),
				}))
				rf.persist()
				return
			}
		}
	}
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()
	rf.HandleTermUpdateLocked(args.Term)
	eventName := "AppendEntries"
	if len(args.Entries) == 0 {
		eventName = "Heartbeat"
	}
	defer func() {
		TraceInstant(eventName, rf.me, time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
			"args.PrevLogIndex": args.PrevLogIndex,
			"args.PrevLogTerm":  args.PrevLogTerm,
			"args.Term":         args.Term,
			"args.LeaderId":     args.LeaderId,
			"args.LeaderCommit": args.LeaderCommit,
			"args.Entries":      fmt.Sprintf("%v", args.Entries),
			"success":           reply.Success,
		}))
	}()

	// only update timestamp when receiving RPC from current leader
	if args.Term == rf.currentTerm {
		rf.lastReceivedRPC = time.Now().UnixMilli()
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term == rf.currentTerm && rf.role == Candidate {
		rf.SwitchToFollower()
	}

	// PrevLogIndex = PrevLogTerm = -1 when leader log is empty
	if !rf.log.IsEmpty() && args.PrevLogIndex != -1 {
		if rf.log.TermValidAt(args.PrevLogIndex) {
			term := rf.log.TermAt(args.PrevLogIndex)
			// find the first log entry index with the mismatch term
			if term != args.PrevLogTerm {
				reply.XTerm = term
				for i := args.PrevLogIndex; rf.log.TermValidAt(i); i -= 1 {
					if rf.log.TermAt(i) == term {
						reply.XIndex = i
					}
				}
				reply.XLen = rf.log.Length()
				reply.Success = false
				return
			}
		} else {
			// also return false if we don't find PrevLogIndex in own log
			goto failed
		}
	} else if rf.log.IsEmpty() && args.PrevLogIndex != -1 {
		goto failed
	} else if !rf.log.IsEmpty() && args.PrevLogIndex == -1 {
		// we'll later truncate our logs
	} else if rf.log.IsEmpty() && args.PrevLogIndex == -1 {
		// we'll simply append logs later
	}

	rf.AppendNewEntriesLocked(args)
	// commit the new logs
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		// when PrevLogIndex == -1 and args.Entries empty, we simply reset commitIndex to its default value -1
		newCommitIndex := min(args.LeaderCommit, lastNewEntryIndex)
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.condApplyEntries.Signal()
		}
	}
	reply.Success = true
	return
failed:
	reply.XLen = rf.log.Length()
	reply.XTerm = -1
	reply.XIndex = -1
	reply.Success = false
	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()
	rf.HandleTermUpdateLocked(args.Term)
	TraceInstant("InstallSnapshot", rf.me, time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
		"args.LastIncludedIndex": args.LastIncludedIndex,
		"args.LastIncludedTerm":  args.LastIncludedTerm,
		"args.Term":              args.Term,
		"From":                   args.LeaderId,
		"snapshotSize":           len(args.Data),
	}))
	if args.Term == rf.currentTerm {
		rf.lastReceivedRPC = time.Now().UnixMilli()
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.LastIncludedIndex < rf.log.StartFrom {
		return
	}
	// follower contains more log entries than snapshot
	if args.LastIncludedIndex < rf.log.Length() && rf.log.TermAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.log.SnapshotTo(args.LastIncludedIndex)
		rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
		rf.lastApplied = max(args.LastIncludedIndex, rf.lastApplied)
	} else {
		// discard entire log
		rf.log.Reset(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.snapshot = args.Data
	rf.persist()
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      clone(rf.snapshot),
		// notice we need to transform internal index to tester
		SnapshotIndex: args.LastIncludedIndex + 1,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	go func(m ApplyMsg) {
		select {
		case *rf.applyCh <- m:
		case <-rf.killCh:
			return
		}
	}(msg)
}

func (rf *Raft) AppendNewEntriesLocked(args *AppendEntriesArgs) {
	// check if we need to truncate invalid log entries
	logUpdated := false
	truncateTo := -1
	i := 0
	for ; i < len(args.Entries); i++ {
		entry := args.Entries[i]
		index := args.PrevLogIndex + 1 + i
		if index < rf.log.Length() {
			// if we reach out here, we have verified term is valid at PrevLogIndex in follower log
			// thus there exists term until end of follower log, meaning `rf.log.EntryValidAt(index) == true` in this loop
			if rf.log.TermAt(index) != entry.Term {
				truncateTo = index
				break
			}
		} else {
			break
		}
	}
	if truncateTo != -1 {
		rf.log.TruncateTo(truncateTo)
		logUpdated = true
	}
	// append remaining entries into log
	newEntries := args.Entries[i:]
	if len(newEntries) != 0 {
		logUpdated = true
	}
	rf.log.Append(newEntries)
	if logUpdated {
		rf.persist()
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.Lock()
	defer rf.Unlock()
	isLeader := rf.role == Leader
	if !isLeader || rf.killed() {
		return index, term, false
	}
	index = rf.log.Put(LogEntry{Term: rf.currentTerm, Command: command})
	term = rf.currentTerm
	// TODO(later): it is possible that this signal could be missed if the very first call of `Start` happens before SendLogDaemon blocked on wait.
	rf.condAppendEntries.Broadcast()
	rf.persist()
	TraceInstant("StartCommand", rf.me, time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
		"entry": fmt.Sprintf("%v", command),
	}))
	return index + 1, term, isLeader
}

func (rf *Raft) ShouldSendEntriesToServerLocked(server int, isHeartBeat bool) bool {
	if rf.killed() {
		return false
	}
	if rf.role != Leader {
		return false
	}
	return rf.log.Length() > rf.nextIndex[server] || isHeartBeat
}

func (rf *Raft) SendLogDaemon(server int) {
	for !rf.killed() {
		rf.Lock()
		for !rf.ShouldSendEntriesToServerLocked(server, false) && !rf.killed() {
			rf.condAppendEntries.Wait()
		}
		rf.Unlock()
		rf.SendLogEntriesToServer(server, false)
	}
	rf.daemonKilled[server].Store(true)
}

func (rf *Raft) SendLogEntriesToServer(server int, isHeartBeat bool) {
	for {
		sendSnapshot := false
		var copiedEntries []LogEntry = nil
		var copiedSnapshot []byte = nil

		rf.Lock()
		if !rf.ShouldSendEntriesToServerLocked(server, isHeartBeat) {
			rf.Unlock()
			return
		}

		term := rf.currentTerm
		commitIndex := rf.commitIndex
		nextIndex := rf.nextIndex[server]
		prevLogTerm := rf.log.TermAt(nextIndex - 1)
		if nextIndex >= 1 && prevLogTerm == -1 {
			// we expect to see a valid `PrevLogTerm` but it's already in snapshot
			sendSnapshot = true
		}
		if sendSnapshot {
			copiedSnapshot = make([]byte, len(rf.snapshot))
			copy(copiedSnapshot, rf.snapshot)
		} else {
			entries := rf.log.SliceFrom(nextIndex)
			copiedEntries = make([]LogEntry, len(entries))
			copy(copiedEntries, entries)
		}
		lastIncludedIndex := rf.log.LastIncludedIndex
		lastIncludedTerm := rf.log.LastIncludedTerm
		rf.Unlock()

		if sendSnapshot {
			if rf.SendSnapshotOnce(server, term, copiedSnapshot, lastIncludedIndex, lastIncludedTerm) || isHeartBeat || rf.killed() {
				break
			} else {
				// sleep for a while if RPC failed
				time.Sleep(time.Millisecond * 10)
			}
		} else {
			if rf.SendLogEntriesOnce(server, term, commitIndex, copiedEntries, nextIndex-1, prevLogTerm, nextIndex) || isHeartBeat || rf.killed() {
				break
			} else {
				// sleep for a while if RPC failed
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func (rf *Raft) SendLogEntriesOnce(server int, term int, commitIndex int, entries []LogEntry, prevLogIndex int, prevLogTerm int, nextIndex int) bool {
	reply := AppendEntriesReply{}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		LeaderCommit: commitIndex,
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}
	if !rf.sendAppendEntries(server, &args, &reply) {
		return false
	}

	rf.Lock()
	defer rf.Unlock()
	ignoreResponse := rf.currentTerm != args.Term
	rf.HandleTermUpdateLocked(reply.Term)
	if ignoreResponse {
		// drop the reply and return if term mismatch
		// we should no longer try to send entries
		return true
	}

	if reply.Success {
		rf.nextIndex[server] = max(nextIndex+len(entries), rf.nextIndex[server])
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		termOutDated := args.Term < reply.Term
		// we only decrement nextIndex if AppendEntries fails because of log inconsistency
		// replicate should finally succeed when nextIndex == 0, unless currentTerm is out-dated
		if !termOutDated {
			rf.HandleNextIndexBacktrackLocked(server, &args, &reply)
		}
		return false
	}
	rf.UpdateCommitIndexLocked()
	return true
}

func (rf *Raft) SendSnapshotOnce(server int, term int, snapshot []byte, lastIncludedIndex int, lastIncludedTerm int) bool {
	if lastIncludedIndex == -1 || lastIncludedTerm == -1 {
		panic("invalid last include index/term")
	}
	reply := InstallSnapshotReply{}
	args := InstallSnapshotArgs{Term: term, LeaderId: rf.me, LastIncludedIndex: lastIncludedIndex, LastIncludedTerm: lastIncludedTerm, Data: snapshot}

	if !rf.sendInstallSnapshot(server, &args, &reply) {
		return false
	}

	rf.Lock()
	defer rf.Unlock()
	rf.HandleTermUpdateLocked(reply.Term)
	rf.nextIndex[server] = lastIncludedIndex + 1
	rf.matchIndex[server] = max(rf.matchIndex[server], lastIncludedIndex)
	return true
}

func (rf *Raft) FindTermInLogLocked(start int, term int) int {
	for i := start; rf.log.EntryValidAt(i); i -= 1 {
		if rf.log.TermAt(i) == term {
			return i
		}
	}
	return -1
}

func (rf *Raft) HandleNextIndexBacktrackLocked(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.XTerm == -1 {
		rf.nextIndex[server] = reply.XLen
	} else {
		// check if the conflict term in follower also exists in leader
		// and return the last log entry index of the term
		idx := rf.FindTermInLogLocked(args.PrevLogIndex, reply.XTerm)
		if idx == -1 {
			// mismatched term (from follower) not in leader log, we're safe to skip the entire mismatch term in follower log
			rf.nextIndex[server] = reply.XIndex
		} else {
			// if found, we know at least to some index before PrevLogIndex in the term, the follower has matching logs with leader
			// if we skip this entire term in leader's log, we're skipping too much
			// thus we resend the last entry in the term and see if we could succeed
			// e.g.
			// leader:   0 0 1 1 1 2 2
			// follower: 0 0 1 1 1 1
			// if leader send the last entry with term `2` to follower, it will fail because previous log term mismatch (2 != 1)
			// but it will be step back too much if we skip the term 1 entirely and send from last term `0` entry
			// thus we retry with the last entry of term `1` in leader log, and we can expect it's likely succeed
			rf.nextIndex[server] = idx
			// Notice: above could also be `nextIndex[server] = idx + 1`, based on description from https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
			// > If it finds an entry in its log with that term, it should set nextIndex to be the one **beyond** the index of the last entry in that term in its log.
			// Either way is safe, since it guarantees:
			// 1. XIndex <= PrevLogIndex
			// 2. idx < PrevLogIndex or idx == -1
			// thus the alternative way also guarantees nextIndex is keep decreasing
		}
	}
}

func (rf *Raft) UpdateCommitIndexLocked() {
	// only log entries <= commitIndex can be trimmed to snapshot
	// thus it's guaranteed entry with index commitIndex + 1 is still available in log
	for n := rf.commitIndex + 1; n < rf.log.Length(); n += 1 {
		if rf.log.TermAt(n) != rf.currentTerm {
			continue
		}
		// logs already replicated on leader himself
		count := 1
		for i := 0; i < len(rf.peers); i += 1 {
			if i != rf.me && rf.matchIndex[i] >= n {
				count += 1
			}
		}
		if count >= rf.MajorityNum() {
			rf.commitIndex = n
		}
	}
	rf.condApplyEntries.Signal()
}

func (rf *Raft) DaemonApplyEntry() {
	for !rf.killed() {
		rf.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.condApplyEntries.Wait()
		}
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		entries := make([]LogEntry, 0, rf.commitIndex-rf.lastApplied)
		messages := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i += 1 {
			entry := rf.log.Get(i)
			entries = append(entries, entry)
			messages = append(messages, ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: i + 1,
			})
			rf.lastApplied = i
		}
		rf.Unlock()

		TraceInstant("Commit", rf.me, time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
			"inclusiveStart": lastApplied + 1,
			"inclusiveEnd":   commitIndex,
			"entries":        fmt.Sprintf("%v", entries),
		}))

		for _, msg := range messages {
			select {
			case *rf.applyCh <- msg:
				// do nothing
			case <-rf.killCh:
				return
			}
		}
	}
}

func (rf *Raft) CheckKillComplete() bool {
	if !rf.tickerKilled.Load() {
		return false
	}
	if !rf.heartbeatKilled.Load() {
		return false
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && !rf.daemonKilled[i].Load() {
			return false
		}
	}
	return true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.condAppendEntries.Broadcast()
	for i := 0; i < rf.killChSize; i += 1 {
		rf.killCh <- true
	}
	// TODO: why Raft can not be fully killed? also need a check for kv server
	go func(start int64) {
		for !rf.CheckKillComplete() {
			time.Sleep(time.Millisecond * 100)
			timeout := int64(10)
			if time.Now().UnixMilli()-start > timeout*1000 {
				log.Printf("Spent more than %ds to kill %p", timeout, rf)
				fid, _ := os.Create("goroutine.txt")
				pprof.Lookup("goroutine").WriteTo(fid, 2)
			}
		}
	}(time.Now().UnixMilli())
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 200 and 300
		// milliseconds.
		rf.electionTimeout = 200 + (rand.Int63() % 200)
		time.Sleep(time.Duration(rf.electionTimeout) * time.Millisecond)
		// Check if a leader election should be started.
		// this holds true on start
		rf.TryStartNewElection()
	}
	rf.tickerKilled.Store(true)
}

func (rf *Raft) TryStartNewElection() {
	if rf.killed() {
		return
	}
	rf.Lock()
	defer rf.Unlock()
	if time.Now().UnixMilli()-rf.lastReceivedRPC <= rf.electionTimeout {
		return
	}
	rf.lastReceivedRPC = time.Now().UnixMilli()
	// do nothing if already leader
	if rf.role == Leader {
		return
	}
	// it will be persisted during switch to candidate
	rf.currentTerm += 1
	TraceInstant("NewElection", rf.me, time.Now().UnixMicro(), rf.GetTraceState())
	rf.SwitchToCandidate()
	info := rf.GetLastLogInfoLocked()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// request for vote in parallel
			go rf.RequestVoteFromServer(i, info, rf.currentTerm)
		}
	}
}

func (rf *Raft) GetLastLogInfoLocked() LastLogInfo {
	return LastLogInfo{index: rf.log.LastLogIndex(), term: rf.log.LastLogTerm()}
}

func (rf *Raft) RequestVoteFromServer(server int, info LastLogInfo, term int) {
	arg := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: info.index,
		LastLogTerm:  info.term,
	}
	reply := RequestVoteReply{}
	if !rf.sendRequestVote(server, &arg, &reply) {
		return
	}
	// lock Data and handle logic
	rf.Lock()
	defer rf.Unlock()
	// ignore the response if already in next term
	ignoreResponse := arg.Term != rf.currentTerm
	rf.HandleTermUpdateLocked(reply.Term)
	if !ignoreResponse && reply.VoteGranted {
		rf.HandleVoteGrantedLocked()
	}
}

func (rf *Raft) SwitchToCandidate() {
	now := time.Now().UnixMicro()
	if rf.role == Leader {
		panic("Leader can not become candidate")
	}
	if rf.role == Follower {
		TraceEventEnd(!rf.killed(), Follower.String(), rf.me, now, nil)
	}
	if rf.role != Candidate {
		TraceEventBegin(!rf.killed(), Candidate.String(), rf.me, now, rf.GetTraceState())
	}
	rf.role = Candidate
	rf.voteCount = 1
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) SwitchToFollower() {
	now := time.Now().UnixMicro()
	if rf.role == Leader {
		TraceEventEnd(!rf.killed(), Leader.String(), rf.me, now, nil)
	}
	if rf.role == Candidate {
		TraceEventEnd(!rf.killed(), Candidate.String(), rf.me, now, nil)
	}
	if rf.role != Follower {
		TraceEventBegin(!rf.killed(), Follower.String(), rf.me, now, rf.GetTraceState())
	}
	rf.role = Follower
	rf.voteCount = 0
	rf.votedFor = Null
	rf.persist()
}

func (rf *Raft) SwitchToLeader() {
	if rf.role == Follower {
		panic("Follower can not become leader")
	}
	now := time.Now().UnixMicro()
	if rf.role == Candidate {
		TraceEventEnd(!rf.killed(), Candidate.String(), rf.me, now, nil)
	}
	state := rf.GetTraceState()
	state["voteCount"] = rf.voteCount
	TraceEventBegin(!rf.killed(), Leader.String(), rf.me, now, state)
	rf.role = Leader
	rf.voteCount = 0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log.Length()
		rf.matchIndex[i] = -1
	}
	rf.heartbeatImpl()
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		time.Sleep(100 * time.Millisecond)
		if _, isLeader := rf.GetState(); isLeader {
			rf.heartbeatImpl()
		}
	}
	rf.heartbeatKilled.Store(true)
}

func (rf *Raft) heartbeatImpl() {
	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			go rf.SendLogEntriesToServer(i, true)
		}
	}
}

func (rf *Raft) HandleTermUpdateLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.SwitchToFollower()
		msg := ApplyMsg{TermChanged: true, CommandValid: false}
		select {
		case *rf.applyCh <- msg:
		case <-rf.killCh:
		}
	}
}

func (rf *Raft) HandleVoteGrantedLocked() {
	if rf.role != Candidate {
		return
	}
	rf.voteCount += 1
	if rf.voteCount >= rf.MajorityNum() {
		rf.SwitchToLeader()
	}
}

func (rf *Raft) MajorityNum() int {
	return len(rf.peers)/2 + 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	TraceEventBegin(true, "Follower", me, time.Now().UnixMicro(), nil)
	rf := &Raft{}
	rf.applyCh = &applyCh
	// in case Kill() is called more than once or more goroutines needs to be terminated
	rf.killChSize = 16
	rf.killCh = make(chan bool, rf.killChSize)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.log = LogContainer{Data: make([]LogEntry, 0, 512), StartFrom: 0, LastIncludedTerm: -1, LastIncludedIndex: -1}
	rf.snapshot = nil

	// Your initialization code here (2A, 2B, 2C).
	rf.electionTimeout = 0
	rf.lastReceivedRPC = 0
	rf.role = Follower
	rf.voteCount = 0
	rf.votedFor = Null
	rf.condAppendEntries = sync.NewCond(&rf.mu)
	rf.condApplyEntries = sync.NewCond(&rf.mu)

	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.daemonKilled = make([]atomic.Bool, len(rf.peers))
	for i := 0; i < len(rf.peers); i += 1 {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = -1
		if i != rf.me {
			go rf.SendLogDaemon(i)
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	// skip log entries that already in snapshot
	rf.commitIndex = rf.log.LastIncludedIndex
	rf.lastApplied = rf.log.LastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()
	go rf.DaemonApplyEntry()

	return rf
}
