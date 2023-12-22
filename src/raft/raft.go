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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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
	data []LogEntry
}

func (c *LogContainer) Length() int {
	return len(c.data)
}

func (c *LogContainer) LastLogIndex() int {
	return len(c.data) - 1
}

func (c *LogContainer) LastLogTerm() int {
	if c.Length() == 0 {
		return 0
	} else {
		return c.Get(c.Length() - 1).Term
	}
}

func (rf *Raft) GetTraceState() map[string]any {
	return map[string]any{
		"term":        rf.currentTerm,
		"commitIndex": rf.commitIndex,
	}
}

func (c *LogContainer) Get(index int) LogEntry {
	return c.data[index]
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	votedFor    int
	voteCount   int
	currentTerm int
	commitIndex int

	cond *sync.Cond

	log LogContainer

	lastReceivedRPC int64
	electionTimeout int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.role == Leader
	rf.mu.Unlock()
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

type AppendEntriesReply struct {
	Term    int
	Success bool
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.HandleResponseTermLocked(args.Term)
	reply.Term = rf.currentTerm
	// candidate's term is valid
	if args.Term >= rf.currentTerm {
		if rf.votedFor == args.CandidateId || rf.votedFor == Null {
			if MoreOrEqualUpToDateThan(LastLogInfo{index: args.LastLogIndex, term: args.LastLogTerm}, rf.GetLastLogInfoLocked()) {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				return
			}
		}
	}
	reply.VoteGranted = false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.HandleResponseTermLocked(args.Term)
	reply.Success = true
	reply.Term = rf.currentTerm
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
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	// Your code here, if desired.
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
}

func (rf *Raft) TryStartNewElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if time.Now().UnixMilli()-rf.lastReceivedRPC <= rf.electionTimeout {
		return
	}
	// do nothing if already leader
	if rf.role == Leader {
		return
	}
	rf.currentTerm += 1
	rf.SwitchToCandidate()
	info := rf.GetLastLogInfoLocked()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// request for vote in parallel
			go rf.RequestVoteFromServer(i, info)
		}
	}
}

func (rf *Raft) GetLastLogInfoLocked() LastLogInfo {
	return LastLogInfo{index: rf.log.LastLogIndex(), term: rf.log.LastLogTerm()}
}

func (rf *Raft) RequestVoteFromServer(server int, info LastLogInfo) {
	rf.mu.Lock()
	arg := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: info.index,
		LastLogTerm:  info.term,
	}
	rf.mu.Unlock()
	reply := RequestVoteReply{}
	for {
		if rf.sendRequestVote(server, &arg, &reply) {
			break
		}
	}
	// lock data and handle logic
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// ignore the response if already in next term
	ignoreResponse := arg.Term != rf.currentTerm
	rf.HandleResponseTermLocked(reply.Term)
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
		TraceEventEnd(!rf.killed(), Follower.String(), rf.me, now, rf.GetTraceState())
	}
	if rf.role != Candidate {
		TraceEventBegin(!rf.killed(), Candidate.String(), rf.me, now, rf.GetTraceState())
	}
	rf.role = Candidate
	rf.voteCount = 1
	rf.votedFor = rf.me
}

func (rf *Raft) SwitchToFollower() {
	now := time.Now().UnixMicro()
	if rf.role == Leader {
		TraceEventEnd(!rf.killed(), Leader.String(), rf.me, now, rf.GetTraceState())
	}
	if rf.role == Candidate {
		TraceEventEnd(!rf.killed(), Candidate.String(), rf.me, now, rf.GetTraceState())
	}
	if rf.role != Follower {
		TraceEventBegin(!rf.killed(), Follower.String(), rf.me, now, rf.GetTraceState())
	}
	rf.role = Follower
	rf.voteCount = 0
	rf.votedFor = Null
}

func (rf *Raft) SwitchToLeader() {
	if rf.role == Follower {
		panic("Follower can not become leader")
	}
	now := time.Now().UnixMicro()
	state := rf.GetTraceState()
	state["voteCount"] = rf.voteCount
	if rf.role == Candidate {
		TraceEventEnd(!rf.killed(), Candidate.String(), rf.me, now, state)
	}
	TraceEventBegin(!rf.killed(), Leader.String(), rf.me, now, state)
	rf.role = Leader
	rf.voteCount = 0
	rf.votedFor = Null
	rf.cond.Signal()
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.SendHeartBeatImpl()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) SendHeartBeatImpl() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// block the thread if self not a leader
	for rf.role != Leader {
		rf.cond.Wait()
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int, term int, commitIndex int) {
				arg := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					LeaderCommit: commitIndex,
				}
				reply := AppendEntriesReply{}
				// leader don't care about whether heart beat succeed
				rf.sendAppendEntries(server, &arg, &reply)
			}(i, rf.currentTerm, rf.commitIndex)
		}
	}
}

func (rf *Raft) HandleResponseTermLocked(term int) {
	// this method will be called on every RPC, thus we update the timestamp here
	rf.lastReceivedRPC = time.Now().UnixMilli()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.SwitchToFollower()
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
	if me == 0 {
		InitNewTrace()
		TraceInstant("Start", 0, time.Now().UnixMicro())
	}
	TraceEventBegin(true, "Follower", me, time.Now().UnixMicro(), nil)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	rf.currentTerm = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.electionTimeout = 0
	rf.lastReceivedRPC = 0
	rf.voteCount = 0
	rf.votedFor = Null
	rf.cond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()

	return rf
}
