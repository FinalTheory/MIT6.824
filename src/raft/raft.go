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
	"runtime"
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
	if actualStart > len(c.Data) || actualStart < 0 {
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
	if index != c.LastIncludedIndex {
		c.LastIncludedIndex = index
		c.LastIncludedTerm = c.Get(index).Term
	}
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
	lockOwner atomic.Pointer[string]
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	GID       int32
	dead      int32 // set by Kill()
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
	condApplyMsg      *sync.Cond

	log      LogContainer
	snapshot []byte

	serverStartTime int64
	nextTimeout     int64
	sleepTo         int64

	msgQueue []ApplyMsg

	// states used to kill raft service
	tickerKilled    atomic.Bool
	heartbeatKilled atomic.Bool
	daemonKilled    []atomic.Bool
	applierKilled   atomic.Bool
	killCh          chan bool
	killChSize      int
}

func (rf *Raft) DebugLock() {
	for !rf.mu.TryLock() {
		owner := rf.lockOwner.Load()
		if owner != nil {
			log.Printf("[%d][%d] Failed to acquire lock hold by %s", rf.getGID(), rf.me, *owner)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
	name := trace()
	rf.lockOwner.Store(&name)
}

func (rf *Raft) Unlock() {
	rf.lockOwner.Store(nil)
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
		"GID":              rf.getGID(),
		"raft.currentTerm": rf.currentTerm,
		"raft.commitIndex": rf.commitIndex,
		"raft.lastApplied": rf.lastApplied,
		"raft.nextIndex":   fmt.Sprintf("%v", rf.nextIndex),
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
		// here we allow a snapshot with `index == rf.log.StartFrom-1`
		// so as to "override" the current snapshot with same last included index
		// this is only to pass lab 4 deletion challenge
		if idx < rf.log.StartFrom-1 {
			return
		}
		TraceInstant("Snapshot", rf.me, rf.getGID(), time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
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

func moreOrEqualUpToDateThan(lhs LastLogInfo, rhs LastLogInfo) bool {
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
	rf.handleTermUpdateLocked(args.Term)
	reply.Term = rf.currentTerm
	// candidate's term is valid
	if args.Term >= rf.currentTerm {
		if rf.votedFor == args.CandidateId || rf.votedFor == Null {
			candidateLastLogInfo := LastLogInfo{index: args.LastLogIndex, term: args.LastLogTerm}
			if moreOrEqualUpToDateThan(candidateLastLogInfo, rf.getLastLogInfoLocked()) {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				rf.resetElectionTimeout()
				TraceInstant("Vote", rf.me, rf.getGID(), time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
					"voteFor":              args.CandidateId,
					"selfLastLogInfo":      fmt.Sprintf("%v", rf.getLastLogInfoLocked()),
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
	rf.handleTermUpdateLocked(args.Term)
	eventName := "AppendEntries"
	if len(args.Entries) == 0 {
		eventName = "Heartbeat"
	}
	defer func() {
		TraceInstant(eventName, rf.me, rf.getGID(), time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
			"args.PrevLogIndex": args.PrevLogIndex,
			"args.PrevLogTerm":  args.PrevLogTerm,
			"args.Term":         args.Term,
			"args.LeaderId":     args.LeaderId,
			"args.LeaderCommit": args.LeaderCommit,
			"args.Entries":      fmt.Sprintf("%v", args.Entries),
			"reply.Term":        reply.Term,
			"reply.Success":     reply.Success,
			"reply.XLen":        reply.XLen,
			"reply.XIndex":      reply.XIndex,
			"reply.XTerm":       reply.XTerm,
		}))
	}()

	// only update timestamp when receiving RPC from current leader
	if args.Term == rf.currentTerm {
		rf.resetElectionTimeout()
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term == rf.currentTerm && rf.role == Candidate {
		rf.switchToFollower()
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
			// return failure if we don't find `PrevLogIndex` in own log, which could be either log too short or this index already in snapshot
			// in the latter case, follower might actually have longer log size than leader, and we should reply with the first index that not yet in snapshot
			if args.PrevLogIndex >= rf.log.Length() {
				reply.XLen = rf.log.Length()
			} else {
				reply.XLen = rf.log.StartFrom
			}
			goto failed
		}
	} else if rf.log.IsEmpty() && args.PrevLogIndex != -1 {
		reply.XLen = 0
		goto failed
	} else if !rf.log.IsEmpty() && args.PrevLogIndex == -1 {
		// we'll later truncate our logs
	} else if rf.log.IsEmpty() && args.PrevLogIndex == -1 {
		// we'll simply append logs later
	}

	rf.appendNewEntriesLocked(args)
	// commit the new logs
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		// when PrevLogIndex == -1 and args.Entries empty, we simply reset commitIndex to its default value -1
		newCommitIndex := min(args.LeaderCommit, lastNewEntryIndex)
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyEntryLocked()
		}
	}
	reply.Success = true
	return
failed:
	reply.XTerm = -1
	reply.XIndex = -1
	reply.Success = false
	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()
	rf.handleTermUpdateLocked(args.Term)
	TraceInstant("InstallSnapshot", rf.me, rf.getGID(), time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
		"args.LastIncludedIndex": args.LastIncludedIndex,
		"args.LastIncludedTerm":  args.LastIncludedTerm,
		"args.Term":              args.Term,
		"From":                   args.LeaderId,
		"snapshotSize":           len(args.Data),
	}))
	if args.Term == rf.currentTerm {
		rf.resetElectionTimeout()
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.LastIncludedIndex < rf.log.StartFrom {
		return
	}
	applySnapshot := true
	// follower contains more log entries than snapshot
	if args.LastIncludedIndex < rf.log.Length() && rf.log.TermAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		// if we reach out here, it means snapshot from leader contain more logs than in current snapshot, thus we should persist this snapshot
		rf.log.SnapshotTo(args.LastIncludedIndex)
		// but snapshot from leader might not contain the logs up to `commitIndex` yet, thus we do not apply the snapshot to application
		if rf.commitIndex >= args.LastIncludedIndex {
			applySnapshot = false
		}
	} else {
		// discard entire log
		rf.log.Reset(args.LastIncludedIndex, args.LastIncludedTerm)
	}
	rf.snapshot = args.Data
	rf.persist()
	if !applySnapshot {
		return
	}
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      clone(rf.snapshot),
		// notice we need to transform internal index to tester
		SnapshotIndex: args.LastIncludedIndex + 1,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.msgQueue = append(rf.msgQueue, msg)
	rf.condApplyMsg.Signal()
}

func (rf *Raft) appendNewEntriesLocked(args *AppendEntriesArgs) {
	// check if we need to truncate invalid log entries
	logUpdated := false
	truncateTo := -1
	i := 0
	for ; i < len(args.Entries); i++ {
		entry := args.Entries[i]
		index := args.PrevLogIndex + 1 + i
		// it is possible an RPC suffered long latency and entry at `index` is already in snapshot
		// thus we only truncate the part that still available in log
		if rf.log.EntryValidAt(index) {
			if rf.log.TermAt(index) != entry.Term {
				truncateTo = index
				break
			}
		}
		if index >= rf.log.Length() {
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
	// TODO(later): it is possible that this signal could be missed if the very first call of `Start` happens before sendLogDaemon blocked on wait.
	rf.condAppendEntries.Broadcast()
	rf.persist()
	TraceInstant("StartCommand", rf.me, rf.getGID(), time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
		"entry": fmt.Sprintf("%v", command),
	}))
	return index + 1, term, isLeader
}

func (rf *Raft) shouldSendEntriesToServerLocked(server int, isHeartBeat bool) bool {
	if rf.killed() {
		return false
	}
	if rf.role != Leader {
		return false
	}
	return rf.log.Length() > rf.nextIndex[server] || isHeartBeat
}

func (rf *Raft) sendLogDaemon(server int) {
	for !rf.killed() {
		rf.Lock()
		for !rf.shouldSendEntriesToServerLocked(server, false) && !rf.killed() {
			rf.condAppendEntries.Wait()
		}
		rf.Unlock()
		rf.sendLogEntriesToServer(server, false)
	}
	rf.daemonKilled[server].Store(true)
}

func (rf *Raft) sendLogEntriesToServer(server int, isHeartBeat bool) {
	for {
		sendSnapshot := false
		var copiedEntries []LogEntry = nil
		var copiedSnapshot []byte = nil

		rf.Lock()
		if !rf.shouldSendEntriesToServerLocked(server, isHeartBeat) {
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
		if nextIndex == 0 && rf.log.StartFrom > 0 {
			// if we need to send log from very beginning, need to ensure those log entries still exists
			// otherwise we send snapshot instead
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
			if rf.sendSnapshotOnce(server, term, copiedSnapshot, lastIncludedIndex, lastIncludedTerm) || isHeartBeat || rf.killed() {
				break
			} else {
				// sleep for a while if RPC failed
				time.Sleep(time.Millisecond * 10)
			}
		} else {
			if rf.sendLogEntriesOnce(server, term, commitIndex, copiedEntries, nextIndex-1, prevLogTerm, nextIndex) || isHeartBeat || rf.killed() {
				break
			} else {
				// sleep for a while if RPC failed
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func (rf *Raft) sendLogEntriesOnce(server int, term int, commitIndex int, entries []LogEntry, prevLogIndex int, prevLogTerm int, nextIndex int) bool {
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
	rf.handleTermUpdateLocked(reply.Term)
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
		// or trigger to send a snapshot
		if !termOutDated {
			rf.nextIndex[server] = rf.getNextIndexBacktrackLocked(&args, &reply)
		}
		return false
	}
	rf.updateCommitIndexLocked()
	return true
}

func (rf *Raft) sendSnapshotOnce(server int, term int, snapshot []byte, lastIncludedIndex int, lastIncludedTerm int) bool {
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
	rf.handleTermUpdateLocked(reply.Term)
	rf.nextIndex[server] = lastIncludedIndex + 1
	rf.matchIndex[server] = max(rf.matchIndex[server], lastIncludedIndex)
	return true
}

func (rf *Raft) findTermInLogLocked(start int, term int) int {
	for i := start; rf.log.EntryValidAt(i); i -= 1 {
		if rf.log.TermAt(i) == term {
			return i
		}
	}
	return -1
}

func (rf *Raft) getNextIndexBacktrackLocked(args *AppendEntriesArgs, reply *AppendEntriesReply) int {
	if reply.XTerm == -1 {
		return reply.XLen
	} else {
		// check if the conflict term in follower also exists in leader
		// and return the last log entry index of the term
		idx := rf.findTermInLogLocked(args.PrevLogIndex, reply.XTerm)
		if idx == -1 {
			// mismatched term (from follower) not in leader log, we're safe to skip the entire mismatch term in follower log
			return reply.XIndex
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
			return idx
			// Notice: above could also be `nextIndex[server] = idx + 1`, based on description from https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
			// > If it finds an entry in its log with that term, it should set nextIndex to be the one **beyond** the index of the last entry in that term in its log.
			// Either way is safe, since it guarantees:
			// 1. XIndex <= PrevLogIndex
			// 2. idx < PrevLogIndex or idx == -1
			// thus the alternative way also guarantees nextIndex is keep decreasing
		}
	}
}

func (rf *Raft) updateCommitIndexLocked() {
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
		if count >= rf.majorityNum() {
			rf.commitIndex = n
		}
	}
	rf.applyEntryLocked()
}

func (rf *Raft) applyEntryLocked() {
	lastApplied := rf.lastApplied
	commitIndex := rf.commitIndex
	entries := make([]LogEntry, 0, rf.commitIndex-rf.lastApplied)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i += 1 {
		entry := rf.log.Get(i)
		entries = append(entries, entry)
		rf.msgQueue = append(rf.msgQueue, ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: i + 1,
		})
		rf.lastApplied = i
	}
	if len(entries) > 0 {
		rf.condApplyMsg.Signal()
		TraceInstant("Commit", rf.me, rf.getGID(), time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
			"inclusiveStart": lastApplied + 1,
			"inclusiveEnd":   commitIndex,
			"entries":        fmt.Sprintf("%v", entries),
		}))
	}
}

func (rf *Raft) daemonApplyMsg() {
	defer rf.applierKilled.Store(true)
	for !rf.killed() {
		rf.Lock()
		for len(rf.msgQueue) == 0 && !rf.killed() {
			rf.condApplyMsg.Wait()
		}
		data := rf.msgQueue
		rf.msgQueue = make([]ApplyMsg, 0)
		rf.Unlock()

		for _, msg := range data {
			select {
			case *rf.applyCh <- msg:
				// do nothing
			case <-rf.killCh:
				return
			}
		}
	}
}

func (rf *Raft) checkKillComplete() bool {
	if !rf.tickerKilled.Load() {
		return false
	}
	if !rf.heartbeatKilled.Load() {
		return false
	}
	if !rf.applierKilled.Load() {
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
	rf.condApplyMsg.Broadcast()
	for i := 0; i < rf.killChSize; i += 1 {
		rf.killCh <- true
	}
	CheckKillFinish(10, func() bool { return rf.checkKillComplete() }, rf)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getGID() int {
	return int(atomic.LoadInt32(&rf.GID))
}

func (rf *Raft) resetElectionTimeout() {
	// pause for a random amount of time between 200 and 400
	// milliseconds.
	timeout := 400 + (rand.Int63() % 400)
	atomic.StoreInt64(&rf.nextTimeout, time.Now().UnixMilli()+timeout)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// only used for once
		if rf.serverStartTime != 0 {
			TraceEventBegin(true, "Follower", rf.me, rf.getGID(), rf.serverStartTime, nil)
			rf.serverStartTime = 0
		}
		time.Sleep(time.Millisecond * 10)
		if time.Now().UnixMilli() > atomic.LoadInt64(&rf.nextTimeout) {
			rf.resetElectionTimeout()
			rf.startNewElection()
		}
	}
	rf.tickerKilled.Store(true)
}

func (rf *Raft) startNewElection() {
	if rf.killed() {
		return
	}
	rf.Lock()
	defer rf.Unlock()
	// do nothing if already leader
	if rf.role == Leader {
		return
	}
	// it will be persisted during switch to candidate
	rf.currentTerm += 1
	TraceInstant("NewElection", rf.me, rf.getGID(), time.Now().UnixMicro(), rf.GetTraceState())
	rf.switchToCandidate()
	info := rf.getLastLogInfoLocked()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// request for vote in parallel
			go rf.requestVoteFromServer(i, info, rf.currentTerm)
		}
	}
}

func (rf *Raft) getLastLogInfoLocked() LastLogInfo {
	return LastLogInfo{index: rf.log.LastLogIndex(), term: rf.log.LastLogTerm()}
}

func (rf *Raft) requestVoteFromServer(server int, info LastLogInfo, term int) {
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
	rf.handleTermUpdateLocked(reply.Term)
	if !ignoreResponse && reply.VoteGranted {
		rf.handleVoteGrantedLocked()
		TraceInstant("GetVote", rf.me, rf.getGID(), time.Now().UnixMicro(), merge(rf.GetTraceState(), map[string]any{
			"voteFrom":         server,
			"currentVoteCount": rf.voteCount,
			"arg.Term":         arg.Term,
			"reply.Term":       reply.Term,
			"ignoreResponse":   ignoreResponse,
		}))
	}
}

func (rf *Raft) switchToCandidate() {
	now := time.Now().UnixMicro()
	if rf.role == Leader {
		panic("Leader can not become candidate")
	}
	if rf.role == Follower {
		TraceEventEnd(!rf.killed(), Follower.String(), rf.me, rf.getGID(), now, nil)
	}
	if rf.role != Candidate {
		TraceEventBegin(!rf.killed(), Candidate.String(), rf.me, rf.getGID(), now, rf.GetTraceState())
	}
	rf.role = Candidate
	rf.voteCount = 1
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) switchToFollower() {
	now := time.Now().UnixMicro()
	if rf.role == Leader {
		TraceEventEnd(!rf.killed(), Leader.String(), rf.me, rf.getGID(), now, nil)
	}
	if rf.role == Candidate {
		TraceEventEnd(!rf.killed(), Candidate.String(), rf.me, rf.getGID(), now, nil)
	}
	if rf.role != Follower {
		TraceEventBegin(!rf.killed(), Follower.String(), rf.me, rf.getGID(), now, rf.GetTraceState())
	}
	rf.role = Follower
	rf.voteCount = 0
	rf.votedFor = Null
	rf.persist()
}

func (rf *Raft) switchToLeader() {
	if rf.role == Follower {
		panic("Follower can not become leader")
	}
	now := time.Now().UnixMicro()
	if rf.role == Candidate {
		TraceEventEnd(!rf.killed(), Candidate.String(), rf.me, rf.getGID(), now, nil)
	}
	state := rf.GetTraceState()
	state["voteCount"] = rf.voteCount
	TraceEventBegin(!rf.killed(), Leader.String(), rf.me, rf.getGID(), now, state)
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
		for time.Now().UnixMilli() < rf.sleepTo {
			time.Sleep(time.Millisecond * 10)
		}
		rf.sleepTo = time.Now().UnixMilli() + 100
		if _, isLeader := rf.GetState(); isLeader {
			rf.heartbeatImpl()
		}
	}
	rf.heartbeatKilled.Store(true)
}

func (rf *Raft) heartbeatImpl() {
	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			go rf.sendLogEntriesToServer(i, true)
		}
	}
}

func (rf *Raft) handleTermUpdateLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.switchToFollower()
		msg := ApplyMsg{TermChanged: true, CommandValid: false}
		rf.msgQueue = append(rf.msgQueue, msg)
		rf.condApplyMsg.Signal()
	}
}

func (rf *Raft) handleVoteGrantedLocked() {
	if rf.role != Candidate {
		return
	}
	rf.voteCount += 1
	if rf.voteCount >= rf.majorityNum() {
		rf.switchToLeader()
	}
}

func (rf *Raft) majorityNum() int {
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
	rf := &Raft{}
	rf.serverStartTime = time.Now().UnixMicro()
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

	rf.resetElectionTimeout()
	rf.role = Follower
	rf.voteCount = 0
	rf.votedFor = Null
	rf.condAppendEntries = sync.NewCond(&rf.mu)
	rf.condApplyMsg = sync.NewCond(&rf.mu)

	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.daemonKilled = make([]atomic.Bool, len(rf.peers))
	for i := 0; i < len(rf.peers); i += 1 {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = -1
		if i != rf.me {
			go rf.sendLogDaemon(i)
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
	go rf.daemonApplyMsg()

	return rf
}
