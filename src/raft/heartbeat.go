package raft

import "time"

func (rf *Raft) sendHeartbeats() {
	// # of peers doesn't change, not critical section.
	prevLogIndices := make([]int, rf.getNPeers())
	prevLogTerms := make([]int, rf.getNPeers())

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	commitIndex := rf.commitIndex
	for i := range rf.nextIndex {
		prevLogIndices[i] = rf.nextIndex[i] - 1
		prevLogTerm, err := rf.getLogTerm(prevLogIndices[i])
		// set to -1 if log is empty
		if err != nil {
			prevLogTerm = -1
		}
		prevLogTerms[i] = prevLogTerm
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go rf.handleAppendEntries(
				i, currentTerm,
				prevLogIndices[i],
				prevLogTerms[i],
				[]*LogEntry{},
				commitIndex,
			)
		}
	}
}

func (rf *Raft) runHeartbeat() {
	for range time.NewTicker(HeartbeatIntervalInMs * time.Millisecond).C {
		rf.mu.Lock()
		if rf.status == Leader {
			go rf.sendHeartbeats()
		}
		rf.mu.Unlock()
	}
}
