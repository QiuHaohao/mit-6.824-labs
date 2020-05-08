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
	"errors"
	"sort"
	"sync"
	"sync/atomic"

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

type LogEntry struct {
	Command interface{}
	Term    int
}

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
	Nobody  = -1
	Nothing = -1
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
	applyCh            chan ApplyMsg
	log                []*LogEntry
	commitIndex        int
	lastApplied        int
	// leader states
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) getNPeers() int {
	return len(rf.peers)
}

func (rf *Raft) getNMajority() int {
	return rf.getNPeers()/2 + 1
}

// add for now, may need to change later if add snapshot
func (rf *Raft) getLogAtIndex(index int) (*LogEntry, error) {
	return getLogFromPartialLog(rf.log, 0, index)
}

func (rf *Raft) getLogSlice(startIndex int, endIndex int) []*LogEntry {
	return getLogSliceFromPartialLog(rf.log, 0, startIndex, endIndex)
}

func (rf *Raft) getLogSliceFrom(startIndex int) []*LogEntry {
	return rf.getLogSlice(startIndex, rf.getLastLogIndex()+1)
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLogTerm(logIndex int) (int, error) {
	if logIndex > -1 {
		logEntry, err := rf.getLogAtIndex(logIndex)
		if err != nil {
			return 0, err
		}
		return logEntry.Term, nil
	}
	return 0, errors.New("invalid index")
}
func (rf *Raft) getLastLogTerm() (int, error) {
	return rf.getLogTerm(rf.getLastLogIndex())
}
func (rf *Raft) isAtLeastAsUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastLogTerm, err := rf.getLastLogTerm()
	// if the log is empty, anything is at least as up-to-date as it.
	if err != nil {
		return true
	}
	myLastLogIndex := rf.getLastLogIndex()
	return (lastLogTerm > myLastLogTerm) ||
		(lastLogTerm == myLastLogTerm && lastLogIndex > myLastLogIndex)
}

func (rf *Raft) containsLog(index, term int) bool {
	log, err := rf.getLogAtIndex(index)
	if err != nil {
		return false
	}
	return log.Term == term
}

func (rf *Raft) isLogConsistentFrom(log []*LogEntry, fromIndex int) bool {
	for i, entry := range log {
		if !rf.containsLog(fromIndex+i, entry.Term) {
			return false
		}
	}
	return true
}

// can use selection instead of sort here
func (rf *Raft) getIndexOfLastConsensus() int {
	var sortedMatchIndex sort.IntSlice = make([]int, rf.getNPeers())
	copy(sortedMatchIndex, rf.matchIndex)
	sortedMatchIndex.Sort()
	return sortedMatchIndex[rf.getNMajority()-1]
}

func (rf *Raft) appendLogEntries(log []*LogEntry, prevIndex int) {
	// if (# of entries after prevIndex is less than # of entries to add)
	// 		or (# of entries after prevIndex is greater than # of entries to add
	//					and there is any conflict between the existing log and the one to append)
	if !rf.isLogConsistentFrom(log, prevIndex+1) {
		rf.log = append(
			rf.getLogSlice(0, prevIndex+1),
			log...,
		)
	}
}

func (rf *Raft) updateTerm(termRecved int) {
	if termRecved > rf.currentTerm {
		rf.currentTerm = termRecved
		rf.votedFor = Nobody
		rf.resetElectionTimer <- true
		rf.status = Follower
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	isLeader := rf.status == Leader
	rf.mu.Unlock()
	return currentTerm, isLeader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !(rf.status == Leader) {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, &LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	return rf.getLastLogIndex(), rf.currentTerm, true
}

// take the lock until all msgs are consumed by the clients
// maybe can
func (rf *Raft) applyNewMsgs() {
	for rf.commitIndex > rf.lastApplied {
		commandIndex := rf.lastApplied + 1
		logEntry, err := rf.getLogAtIndex(commandIndex)
		if err != nil {
			panic("log entry that should be committed is not found")
		}
		rf.applyCh <- ApplyMsg{
			Command:      logEntry.Command,
			CommandIndex: commandIndex,
			CommandValid: true,
		}
		rf.lastApplied++
	}
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
		applyCh:            applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).
	go rf.runElectionTimer()
	go rf.runHeartbeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
