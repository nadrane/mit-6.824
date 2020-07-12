package raft

import (
	"math"
	"math/rand"
	"time"
)

func (rf *Raft) loopElection() {
	rand.Seed(time.Now().UnixNano())
	for {
		// Make sure that every election has a random timeout
		rf.setElectionTimeout()

		time.Sleep(rf.electionTimeout)

		if !rf.readyForElection() {
			continue
		}

		DPrintf("[%v] Beginning election", rf.me)

		rf.mu.Lock()
		rf.currentTerm++
		rf.serverState = candidate
		rf.voteHistory[rf.currentTerm] = true // Ensure that this server does not vote for any other server for this term since it's going to vote for itself
		rf.mu.Unlock()

		// The candidate will always vote for themselves, so this is initialized to 1 instead of 0
		voteCount := 1

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(peerNum int) {

					args := RequestVoteArgs{
						CandidateId:  rf.me,
						LastLogIndex: 0,
						LastLogTerm:  rf.currentTerm, // get from logs
						Term:         rf.currentTerm,
					}

					reply := RequestVoteReply{}

					rf.mu.Lock()
					DPrintf("[%v-%v] Sending request vote to %v", rf.me, rf.currentTerm, peerNum)
					rf.mu.Unlock()

					ok := rf.sendRequestVote(peerNum, &args, &reply)
					if !ok {

						rf.mu.Lock()
						DPrintf("[%v-%v] Rpc to %v failed", rf.me, rf.currentTerm, peerNum)
						rf.mu.Unlock()

						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()

					DPrintf("[%v-%v] Vote received %v from %v", rf.me, rf.currentTerm, reply.VoteGranted, peerNum)

					// Sometimes a server is elected leader before the last vote comes back
					// Or, perhaps another reply had a greater term, and this machine is now a follower
					if rf.serverState != candidate {
						DPrintf("[%v-%v] Reply ignored because server is a %v", rf.me, rf.currentTerm, rf.serverState)
						return
					}

					// The other machines are already operating at a higher term,
					// which makes this election invalid
					if rf.currentTerm < reply.Term {
						DPrintf("[%v-%v] Candidate does not have highest term. Term %v received from %v", rf.me,
							rf.currentTerm, reply.Term, peerNum)

						rf.currentTerm = reply.Term
						rf.serverState = follower
						return
					}

					if reply.VoteGranted {
						voteCount++
						// Ensure that
						// 1. this server got the majority of votes
						// 2. The reply is for the current election. It's entirely possible a reply come back for an older election
						if voteCount >= rf.majority() {
							DPrintf("[%v-%v] Assuming leader with %v votes", rf.me, rf.currentTerm, voteCount)
							rf.serverState = leader
							go rf.syncLogEntries()
						}
					}
				}(i)
			}
		}
	}
}

func (rf *Raft) setElectionTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.needElection = true
	rf.electionTimeout = time.Duration(rand.Intn(500)+300) * time.Millisecond
}

func (rf *Raft) readyForElection() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check to see if the election timeout was reset while the server was asleep.
	// This would happen if another server initiated an election during this time
	if !rf.needElection {
		return false
	}

	// The leader is never going to initiate an election
	if rf.serverState == leader {
		return false
	}

	return true
}

func (rf *Raft) majority() int {
	return int(math.Floor(float64(len(rf.peers)/2)) + 1)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%v-%v] Received request to vote from %v for term %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	if !rf.voteHistory[args.Term] && rf.currentTerm < args.Term {
		// Increase the term on this new server so it doesn't try to initiate an election for the same term
		rf.currentTerm = args.Term

		// Reset this server's election timeout to avoid split votes
		rf.needElection = true

		// Ensure that we only vote on a given term once
		rf.voteHistory[args.Term] = true

		// We should send back the exact term we're voting on since RequestVote replies could come back out of order
		reply.Term = args.Term

		reply.VoteGranted = true
	}
}
