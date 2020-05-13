package raft

import "time"

func (rf *Raft) runHeartbeat() {
	for range time.NewTicker(HeartbeatIntervalInMs * time.Millisecond).C {
		rf.mu.Lock()
		if rf.status == Leader {
			go rf.sendAppendEntriesToAll()
		}
		rf.mu.Unlock()
		if rf.killed() {
			return
		}
	}
}
