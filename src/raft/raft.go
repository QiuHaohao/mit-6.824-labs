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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

const (
	HeartbeatIntervalInMs = 110
	ElectionTimerMin      = 310
	ElectionTimerMax      = 930
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Status int

const (
	Follower = iota
	Candidate
	Leader
)

const (
	Nobody = -1
)

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
	resetElectionTimer chan bool
	status             Status
	currentTerm        int
	votedFor           int
	// log

	// commitIndex
	// lastApplied

	// nextIndex
	// matchIndex
}

func getElectionTimerDuration() time.Duration {
	return time.Millisecond * time.Duration(ElectionTimerMin+rand.Int()%(ElectionTimerMax-ElectionTimerMin))
}

func (rf *Raft) runElectionTimer() {
	for {
		DPrintf("[%d] - currentTerm: %v, status: %v, votedFor: %v", rf.me, rf.currentTerm, rf.status, rf.votedFor)
		select {
		case <-time.After(getElectionTimerDuration()):
			rf.onElectionTimerTimeout()
		case <-rf.resetElectionTimer:
			continue
		}
	}
}

func (rf *Raft) onElectionTimerTimeout() {
	if rf.status != Leader {
		go rf.startNewElection()
	}
}

func (rf *Raft) startNewElection() {
	rf.mu.Lock()
	rf.status = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	me := rf.me
	DPrintf("[%d] - Starting new election for term %d done", me, currentTerm)

	rf.mu.Unlock()

	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	count := 1

	for i := range rf.peers {
		if i != rf.me {
			go func(x int) {
				ok, term, voteGranted := rf.sendRequestVote(x, currentTerm, me)
				if ok {
					rf.mu.Lock()
					rf.updateTerm(term)
					rf.mu.Unlock()
					if voteGranted {
						cond.L.Lock()
						count = count + 1
						cond.Broadcast()
						cond.L.Unlock()
					}
				}
			}(i)
		}
	}

	for {
		cond.L.Lock()
		cond.Wait()
		rf.mu.Lock()
		DPrintf("[%d] - New vote for term %d, now %d votes", rf.me, rf.currentTerm, count)
		if count >= len(rf.peers)/2+1 && rf.status == Candidate && rf.currentTerm == currentTerm {
			rf.status = Leader
			DPrintf("[%d] - Elected for term %d", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			rf.sendHeartbeats()
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		cond.L.Unlock()
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go func(x int) {
				ok, termRecved, _ := rf.sendAppendEntries(x, currentTerm, me)
				if ok {
					rf.mu.Lock()
					rf.updateTerm(termRecved)
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

func (rf *Raft) runHeartbeat() {
	for range time.NewTicker(HeartbeatIntervalInMs * time.Millisecond).C {
		if rf.status == Leader {
			rf.sendHeartbeats()
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.status == Leader
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
}

//
// restore previously persisted state.
//
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

func (rf *Raft) updateTerm(termRecved int) {
	if termRecved > rf.currentTerm {
		rf.currentTerm = termRecved
		rf.votedFor = Nobody
		rf.resetElectionTimer <- true
		rf.status = Follower
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int
	CandidateID int
	// lastLogIndex int
	// lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.updateTerm(args.Term)
	DPrintf("[%d] - RequestVote for term %d received from %d, votedFor: %v", rf.me, args.Term, args.CandidateID, rf.votedFor)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = rf.votedFor == Nobody || rf.votedFor == args.CandidateID
	}
	if reply.VoteGranted {
		rf.votedFor = args.CandidateID
	}
	DPrintf("[%d] - RequestVote for term %d from %d done: %v, votedFor: %v", rf.me, args.Term, args.CandidateID, reply, rf.votedFor)
	rf.mu.Unlock()
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
func (rf *Raft) sendRequestVote(server int, term int, candidateID int) (bool, int, bool) {
	DPrintf("[%d] - RequestVote for term %d sent to %d", rf.me, term, server)
	args := &RequestVoteArgs{
		Term:        term,
		CandidateID: candidateID,
	}
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		DPrintf("[%d] - RequestVote for term %d to %d received, granted?: %v", rf.me, term, server, reply.VoteGranted)
	}
	return ok, reply.Term, reply.VoteGranted
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	// prevLogIndex int
	// prevLogTerm int
	// entries
	// leaderCommit
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[%d] - AppendEntries for term %d received from %d", rf.me, args.Term, args.LeaderID)
	rf.mu.Lock()
	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.Success = args.Term >= rf.currentTerm

	if reply.Success {
		rf.resetElectionTimer <- true
		if rf.status == Candidate {
			rf.status = Follower
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, term int, leaderID int) (bool, int, bool) {
	DPrintf("[%d] - AppendEntries for term %d sent to %d", rf.me, term, server)
	args := &AppendEntriesArgs{
		Term:     term,
		LeaderID: leaderID,
	}
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok, reply.Term, reply.Success
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
	rf := &Raft{
		peers:              peers,
		persister:          persister,
		me:                 me,
		resetElectionTimer: make(chan bool),
		status:             Follower,
		currentTerm:        0,
		votedFor:           Nobody,
	}

	go rf.runElectionTimer()
	go rf.runHeartbeat()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
