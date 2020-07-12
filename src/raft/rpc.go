package raft

type AppendEntriesArgs struct {
	Term         int // Candidate's term
	LeaderId     int // Candidate requesting vote
	PrevLogIndex int // Index of candidate's last log entry
	PrevLogTerm  int // Term of candidate's last log entry
	Entries      []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // True if follower contains entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // CurrentTerm for candidate to update itself
	VoteGranted bool // True means cadidate received the vote
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRPCToAll(f func(peerNum int)) {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go f(i)
		}
	}
}
