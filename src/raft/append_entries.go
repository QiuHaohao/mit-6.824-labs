package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) == 0 {
		DPrintf("[%d] - Empty AppendEntries for term %d received from %d", rf.me, args.Term, args.LeaderID)
	} else {
		DPrintf("[%d] - Non-empty AppendEntries for term %d received from %d", rf.me, args.Term, args.LeaderID)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(args.Term)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		// If this is not from the current leader, do not need to
		// reset the elecion timer
		return
	} else if !rf.containsLog(args.PrevLogIndex, args.PrevLogTerm) && (args.PrevLogIndex != -1) {
		reply.Success = false
	} else {
		reply.Success = true
		rf.appendLogEntries(args.Entries, args.PrevLogIndex)
	}
	// reset election timer
	rf.resetElectionTimer <- true
	// update commit index
	if args.LeaderCommit > rf.commitIndex {
		indexLastNewEntry := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < indexLastNewEntry {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = indexLastNewEntry
		}
		rf.applyNewMsgs()
	}
	// convert to follower if is candidate
	if rf.status == Candidate {
		rf.status = Follower
	}
}

func (rf *Raft) sendAppendEntries(server int, term int,
	prevLogIndex int, prevLogTerm int, entries []*LogEntry, leaderCommit int) (bool, int, bool) {
	if len(entries) == 0 {
		DPrintf("[%d] - Empty AppendEntries for term %d sent to %d", rf.me, term, server)
	} else {
		DPrintf("[%d] - Non-empty AppendEntries for term %d sent to %d", rf.me, term, server)
	}
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok, reply.Term, reply.Success
}

func (rf *Raft) handleAppendEntries(server int, term int,
	prevLogIndex int, prevLogTerm int, entries []*LogEntry, leaderCommit int) {
	ok, termRecved, success := rf.sendAppendEntries(
		server, term,
		prevLogIndex,
		prevLogTerm,
		entries,
		leaderCommit,
	)
	rf.mu.Lock()
	// if the request succeeded and I am still the leader of the same term
	if ok && term == rf.currentTerm && rf.status == Leader {
		if success {
			indexLastLogSent := prevLogIndex + len(entries)
			// matchIndex is monotonically increasing
			DPrintf("[%d] - AppendEntries from %d success and OK, indexLastLogSent: %v, rf.matchIndex[%d]: %d", rf.me, server, indexLastLogSent, server, rf.matchIndex[server])
			if indexLastLogSent > rf.matchIndex[server] {
				rf.matchIndex[server] = indexLastLogSent
				indexOfLastConsensus := rf.getIndexOfLastConsensus()
				termOfLastConsensus, _ := rf.getLogTerm(indexOfLastConsensus)
				if indexOfLastConsensus > rf.commitIndex && termOfLastConsensus == rf.currentTerm {
					rf.commitIndex = indexOfLastConsensus
					rf.applyNewMsgs()
				}
			}
			rf.nextIndex[server] = indexLastLogSent + 1
			// failing because of log inconsistency
		} else if termRecved == rf.currentTerm && rf.nextIndex[server] > 0 {
			rf.nextIndex[server]--
			go rf.retryHandleAppendEntries(server, term, prevLogIndex-1)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) retryHandleAppendEntries(server int, term int, prevLogIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// do not retry if the term has already past
	if term != rf.currentTerm {
		return
	}

	prevLogTerm, err := rf.getLogTerm(prevLogIndex)
	if err != nil {
		return
	}

	go rf.handleAppendEntries(
		server, term,
		prevLogIndex,
		prevLogTerm,
		rf.getLogSliceFrom(prevLogTerm+1),
		rf.commitIndex,
	)
}
func (rf *Raft) sendAppendEntriesToAll() {
	var relevantLog []*LogEntry

	// # of peers doesn't change, not critical section.
	prevLogIndices := make([]int, rf.getNPeers())
	prevLogTerms := make([]int, rf.getNPeers())

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	commitIndex := rf.commitIndex

	for i := range rf.nextIndex {
		prevLogIndices[i] = rf.nextIndex[i] - 1
		prevLogTerm, err := rf.getLogTerm(prevLogIndices[i])
		if err != nil {
			prevLogTerm = 0
		}
		prevLogTerms[i] = prevLogTerm
	}
	startRelevantLogIndex := min(rf.nextIndex)
	endRelevantLogIndex := rf.getLastLogIndex() + 1
	lenRelevantLog := endRelevantLogIndex - startRelevantLogIndex
	relevantLog = make([]*LogEntry, lenRelevantLog)
	copy(relevantLog, rf.getLogSlice(startRelevantLogIndex, endRelevantLogIndex))
	rf.mu.Unlock()

	DPrintf("[%d] - ! Sending to non-empty append entries to all", rf.me)

	for i := range rf.peers {
		if i != rf.me {
			DPrintf("[%d] - ! Sending to non-empty append entries to %d", rf.me, i)
			logEntriesToSend := getLogSliceFromPartialLog(
				relevantLog,
				startRelevantLogIndex,
				prevLogIndices[i]+1,
				endRelevantLogIndex,
			)
			DPrintf("[%d] - ! %v", rf.me, logEntriesToSend)
			go rf.handleAppendEntries(
				i, currentTerm,
				prevLogIndices[i],
				prevLogTerms[i],
				logEntriesToSend,
				commitIndex,
			)
		}
	}
}
