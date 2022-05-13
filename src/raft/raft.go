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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	None             int = -1
	ElectionTimeout  int = 150
	HeartbeatTimeout int = 50
	ApplyTimeout     int = 10
	Follower         int = 1
	Candidate        int = 2
	Leader           int = 3
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type Entry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg
	nPeers  int
	role    int

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         map[int]Entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Time count for heartbeat or election
	electTimer *time.Timer
	heartTimer *time.Timer
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) UnLock() {
	rf.mu.Unlock()
}

func (rf *Raft) ResetHeartTimer() {
	// not concurrently secure
	if !rf.heartTimer.Stop() {
		select {
		case <-rf.heartTimer.C:
		default:
		}
	}
	rf.heartTimer.Reset(time.Duration(HeartbeatTimeout) * time.Millisecond)
	log.Printf("[Term %v]: [Node %v][Role %v] call ResetHeartTimer to %v ms.\n", rf.currentTerm, rf.me, rf.role, HeartbeatTimeout)
}

func (rf *Raft) ResetElectTimer() {
	// not concurrently secure
	if !rf.electTimer.Stop() {
		select {
		case <-rf.electTimer.C:
		default:
		}
	}
	d := rand.Int()%ElectionTimeout + ElectionTimeout
	rf.electTimer.Reset(time.Duration(d) * time.Millisecond)
	log.Printf("[Term %v]: [Node %v][Role %v] call ResetElectTimer to %v ms.\n", rf.currentTerm, rf.me, rf.role, d)
}

func (rf *Raft) TurnToLeader() {
	rf.Lock()
	log.Printf("[Term %v]: [Node %v][Role %v] turn to Leader.\n", rf.currentTerm, rf.me, rf.role)
	if rf.role != Candidate {
		log.Panicf("[Node %d] role should be Candidate when turn to leader.\n", rf.me)
	}
	rf.role = Leader
	for i := 0; i < rf.nPeers; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.UnLock()

	go rf.heartLoop()

}

func (rf *Raft) TurnToCandidate() {
	rf.Lock()
	log.Printf("[Term %v]: [Node %v][Role %v] turn to Candidate.\n", rf.currentTerm, rf.me, rf.role)
	if rf.role != Follower {
		log.Panicf("[Node %d] role should be follower when turn to candidate.\n", rf.me)
	}
	rf.role = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.ResetElectTimer()
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.UnLock()

	grantedCount := 1
	finishedCount := 1
	voteCh := make(chan bool)
	for i := 0; i < rf.nPeers; i++ {
		if i != rf.me {
			go func(server int) {
				isGranted := rf.callToRequestVote(server, rf.currentTerm, rf.me, lastLogIndex, lastLogTerm)
				voteCh <- isGranted
			}(i)
		}
	}

	for grantedCount < rf.nPeers/2+1 && finishedCount < rf.nPeers {
		v := <-voteCh
		if v {
			grantedCount += 1
		}
		finishedCount += 1
	}

	if grantedCount >= rf.nPeers/2+1 {
		rf.TurnToLeader()
	} else {
		rf.Lock()
		log.Printf("[Term %v]: [Node %v][Role %v] got %d granted, not enough, turn to follower.\n",
			rf.currentTerm, rf.me, rf.role, grantedCount)
		rf.role = Follower
		rf.votedFor = None
		rf.UnLock()
	}
}

func (rf *Raft) callToRequestVote(server, currentTerm, me, lastLogIndex, lastLogTerm int) bool {

	rf.Lock()
	log.Printf("[Term %v]: [Node %v][Role %v] call RequestVote rpc to Node %v.\n", currentTerm, me, rf.role, server)
	rf.UnLock()

	args := RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}

	ok := rf.sendRequestVote(server, &args, &reply)
	if ok {
		rf.Lock()
		if reply.Term > rf.currentTerm {

			log.Printf("[Term %v]: [Node %v][Role %v] found newer term %v > %v. turn to follower\n",
				rf.currentTerm, rf.me, rf.role, reply.Term, rf.currentTerm)
			rf.role = Follower
			rf.votedFor = None
			rf.currentTerm = reply.Term

		}
		rf.UnLock()
	}

	return reply.VoteGranted
}

func (rf *Raft) callToAppendEntries(server, currentTerm, me, preLogIndex, preLogTerm, commitIndex int, entries []Entry) bool {

	rf.Lock()
	log.Printf("[Term %v]: [Node %v][Role %v] call AppendEntries rpc to Node %v.\n", currentTerm, me, rf.role, server)
	rf.UnLock()

	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, &args, &reply)
	if ok {
		rf.Lock()
		if reply.Term > rf.currentTerm {
			log.Printf("[Term %v]: [Node %v][Role %v] found newer term %v > %v. turn to follower\n",
				rf.currentTerm, rf.me, rf.role, reply.Term, rf.currentTerm)
			rf.role = Follower
			rf.votedFor = None
			rf.currentTerm = reply.Term
		}
		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} else {
			preIndex := args.PrevLogIndex
			for rf.log[preIndex].Term == args.PrevLogTerm && rf.nextIndex[server] >= 1 {
				rf.nextIndex[server]--
				preIndex = rf.nextIndex[server] - 1
			}
		}
		rf.UnLock()
	}

	return reply.Success
}

func (rf *Raft) heartLoop() {
	for {

		rf.Lock()
		if rf.role != Leader || rf.killed() {
			rf.votedFor = None
			rf.role = Follower
			rf.UnLock()
			break
		}
		rf.ResetElectTimer()
		rf.UnLock()

		rf.Lock()
		N := findKthLargest(rf.matchIndex, rf.nPeers/2)
		if rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N
		}

		commitIndex := rf.commitIndex
		currentTerm := rf.currentTerm
		rf.UnLock()

		for i := 0; i < rf.nPeers; i++ {
			if i != rf.me {
				go func(server int) {

					rf.Lock()

					preLogIndex := rf.nextIndex[server] - 1
					preLogTerm := rf.log[preLogIndex].Term

					entries := make([]Entry, 0, 10)
					if len(rf.log)-1 >= rf.nextIndex[server] {
						for j := rf.nextIndex[server]; j <= len(rf.log)-1; j++ {
							entries = append(entries, rf.log[j])
						}
					}

					rf.UnLock()

					rf.callToAppendEntries(server, currentTerm, rf.me, preLogIndex, preLogTerm, commitIndex, entries)

				}(i)
			}
		}

		<-rf.heartTimer.C
		rf.Lock()
		rf.ResetHeartTimer()
		rf.UnLock()

	}
}

func (rf *Raft) applyLoop() {
	for {
		if rf.killed() {
			break
		}
		time.Sleep(time.Duration(ApplyTimeout) * time.Millisecond)
		rf.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			entry := rf.log[rf.lastApplied]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			log.Printf("[Term %v]: [Node %v][Role %v] commitIndex: %v, lastApplied: %v, apply command %v.\n",
				rf.currentTerm, rf.me, rf.role, rf.commitIndex, rf.lastApplied, entry.Command)
			rf.applyCh <- applyMsg
		}
		rf.UnLock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electLoop() {
	for !rf.killed() {
		<-rf.electTimer.C
		rf.TurnToCandidate()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock()
	defer rf.UnLock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.Lock()
	defer rf.UnLock()
	var err error
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err = e.Encode(rf.currentTerm)
	if err != nil {
		log.Panicln("[Persist] error when encode currentTerm ", err)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		log.Panicln("[Persist] error when encode votedFor ", err)
	}
	err = e.Encode(rf.log)
	if err != nil {
		log.Panicln("[Persist] error when encode log[] ", err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.Lock()
	defer rf.UnLock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = None
		rf.log = make(map[int]Entry)
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
	var err error
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	err = d.Decode(&rf.currentTerm)
	if err != nil {
		log.Panicln("[Persist] error when decode currentTerm ", err)
	}
	err = d.Decode(&rf.votedFor)
	if err != nil {
		log.Panicln("[Persist] error when decode votedFor ", err)
	}
	rf.log = make(map[int]Entry)
	if err != nil {
		log.Panicln("[Persist] error when decode log[] ", err)
	}
	err = d.Decode(&rf.log)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.UnLock()

	if args.Term < rf.currentTerm {

		reply.VoteGranted = false
		rf.votedFor = None
		log.Printf("[Term %v]: [Node %v][Role %v] receive request from %v, but term is older %v < %v, refuse it.\n",
			rf.currentTerm, rf.me, rf.role, args.CandidateId, args.Term, rf.currentTerm)

	} else {

		if args.Term > rf.currentTerm {

			log.Printf("[Term %v]: [Node %v][Role %v] receive request from %v, turn to follower because seen new term %v > %v.\n",
				rf.currentTerm, rf.me, rf.role, args.CandidateId, args.Term, rf.currentTerm)
			rf.currentTerm = args.Term
			rf.role = Follower
			rf.votedFor = None

		}

		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
		if (rf.votedFor == None || rf.votedFor == args.CandidateId) &&
			(lastLogTerm > args.LastLogTerm || lastLogIndex >= args.LastLogIndex) {

			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			log.Printf("[Term %v]: [Node %v][Role %v] receive request from %v, vote to it.\n",
				rf.currentTerm, rf.me, rf.role, args.CandidateId)
			rf.ResetElectTimer()

		} else {
			reply.VoteGranted = false
			log.Printf("[Term %v]: [Node %v][Role %v] receive request from %v, already vote or not up-to-date, "+
				"(lastLogIndex, lastLogTerm) = (%v, %v) vs (%v, %v), votedFor is %v refuse it.\n",
				rf.currentTerm, rf.me, rf.role, args.CandidateId, lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
		}

	}
	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.UnLock()

	if args.Term < rf.currentTerm {

		reply.Success = false
		log.Printf("[Term %v]: [Node %v][Role %v] receive request from %v, but term is older %v < %v, refuse it.\n",
			rf.currentTerm, rf.me, rf.role, args.LeaderId, args.Term, rf.currentTerm)

	} else {

		if args.Term > rf.currentTerm {
			log.Printf("[Term %v]: [Node %v][Role %v] receive request from %v, turn to follower because seen new term %v > %v.\n",
				rf.currentTerm, rf.me, rf.role, args.LeaderId, args.Term, rf.currentTerm)
			rf.currentTerm = args.Term
			rf.role = Follower
			rf.votedFor = None
		}

		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

			reply.Success = false
			log.Printf("[Term %v]: [Node %v][Role %v] receive request from %v, but preLogIndex %v prevLogTerm %v unequal args.prevLogTerm %v, refuse it.\n",
				rf.currentTerm, rf.me, rf.role, args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)

		} else {

			idx := args.PrevLogIndex + 1
			if _, ok := rf.log[idx]; ok && rf.log[idx].Term != args.Term {
				log.Printf("[Term %v]: [Node %v][Role %v] receive request from %v, idx %v term %v unequal args.Term %v, delete all following.\n",
					rf.currentTerm, rf.me, rf.role, args.LeaderId, idx, rf.log[idx].Term, args.Term)
				idxRight := len(rf.log)
				for ; idx < idxRight; idx += 1 {
					delete(rf.log, idx)
				}
			}
			idx = args.PrevLogIndex
			for _, entry := range args.Entries {
				idx += 1
				rf.log[idx] = entry
			}
			log.Printf("LeaderCommit %v, index of last new entry %v\n", args.LeaderCommit, idx)
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit < idx {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = idx
				}
			}
			reply.Success = true
			log.Printf("[Term %v]: [Node %v][Role %v][CommitIndex %v] receive request from %v, reset timer.\n",
				rf.currentTerm, rf.me, rf.role, rf.commitIndex, args.LeaderId)
			rf.ResetElectTimer()

		}
	}
	reply.Term = rf.currentTerm
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.Lock()
	term = rf.currentTerm
	isLeader = rf.role == Leader

	if isLeader {
		log.Printf("[Term %v]: [Node %v][Role %v] client request to add %v.\n",
			rf.currentTerm, rf.me, rf.role, command)

		nextIndex := rf.nextIndex[rf.me]
		rf.log[nextIndex] = Entry{command, rf.currentTerm}
		rf.nextIndex[rf.me] += 1

	}
	index = len(rf.log) - 1
	rf.UnLock()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh

	rf.nPeers = len(peers)
	log.Printf("[Init %v] set nPeers to %v.\n", rf.me, rf.nPeers)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, rf.nPeers)
	rf.matchIndex = make([]int, rf.nPeers)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.log[0] = Entry{
		Command: nil,
		Term:    0,
	}
	rf.role = Follower

	rand.Seed(time.Now().UnixNano())
	d := rand.Int()%ElectionTimeout + ElectionTimeout
	rf.electTimer = time.NewTimer(time.Duration(d) * time.Millisecond)
	rf.heartTimer = time.NewTimer(time.Duration(HeartbeatTimeout) * time.Microsecond)

	log.Printf("[Init %v] set period timer to %v.\n", rf.me, d)

	// start goroutine to start elections
	go rf.electLoop()
	go rf.applyLoop()

	return rf
}
