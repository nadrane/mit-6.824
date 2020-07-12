package raft

import "time"

func (rf *Raft) syncLogEntries() {

	for {
		if !rf.isStillLeader() {
			return
		}

		rf.sendRPCToAll(func(peerNum int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}

			rf.sendAppendEntries(peerNum, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// If one of the other server's has a higher term than this server,
			// Then this server likely experienced a partition and can no longer be considered the leader
			if reply.Term > rf.currentTerm {
				rf.serverState = follower
				rf.currentTerm = reply.Term
				return
			}
		})

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) isStillLeader() bool {
	// A server should not be sending any data (let alone log entries) to other
	// servers if it is not the leader

	// This situation can arise in the case where a server is elected leader, loses connection,
	// and then rejoins the cluster. When the new leader sends out it's heartbeat, the
	// old leader will assume a follower role. We need a way to exit this loop in that circumstance
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState != leader {
		DPrintf("[%v]-[%v] Exiting Append Entries Loop - Server attempted to send log entries when not the leader", rf.me, rf.currentTerm)
		return false
	}
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%v]-[%v] Append entries from %v at term %v", rf.me, rf.currentTerm, args.LeaderId, args.Term)

	// The leader's claim is valid
	if rf.currentTerm <= args.Term {
		DPrintf("[%v]-[%v] Append entries claim valid", rf.me, rf.currentTerm)
		rf.serverState = follower
		rf.currentTerm = args.Term
		rf.needElection = false
		// The leader's term is out of date and needs to be updated to this machine's term
	} else {
		reply.Term = rf.currentTerm
	}
}
