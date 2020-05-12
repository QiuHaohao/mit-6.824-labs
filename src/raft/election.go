package raft

import (
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) initLeaderState() {
	rf.nextIndex = make([]int, rf.getNPeers())
	rf.matchIndex = make([]int, rf.getNPeers())
	nextIndexInitValue := rf.getLastLogIndex() + 1

	for i := range rf.nextIndex {
		rf.nextIndex[i] = nextIndexInitValue
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = -1
	}
}

func (rf *Raft) becomeLeader() {
	rf.initLeaderState()
	rf.status = Leader
	go rf.sendHeartbeats()
	DPrintf("[%d] - Elected for term %d", rf.me, rf.currentTerm)
}

func getElectionTimerDuration() time.Duration {
	return time.Millisecond * time.Duration(ElectionTimerMin+rand.Int()%(ElectionTimerMax-ElectionTimerMin))
}

func (rf *Raft) runElectionTimer() {
	for {
		select {
		case <-time.After(getElectionTimerDuration()):
			rf.onElectionTimerTimeout()
		case <-rf.resetElectionTimer:
			continue
		}
	}
}

func (rf *Raft) onElectionTimerTimeout() {
	rf.mu.Lock()
	if rf.status != Leader {
		DPrintf("[%d] - ElectionTimeout", rf.me)
		go rf.startNewElection()
	}
	rf.mu.Unlock()

}

func (rf *Raft) startNewElection() {
	rf.mu.Lock()
	rf.status = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	me := rf.me
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm, err := rf.getLastLogTerm()
	// if the log is empty
	if err != nil {
		lastLogTerm = 0
	}
	rf.persist()
	DPrintf("[%d] - Starting new election for term %d done", me, currentTerm)
	rf.mu.Unlock()

	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	count := 1

	for i := range rf.peers {
		if i != rf.me {
			go func(x int) {
				ok, term, voteGranted := rf.sendRequestVote(
					x, currentTerm, me,
					lastLogIndex,
					lastLogTerm,
				)
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
		if count >= rf.getNMajority() && rf.status == Candidate && rf.currentTerm == currentTerm {
			rf.becomeLeader()
		}
		rf.mu.Unlock()
		cond.L.Unlock()
	}
}
