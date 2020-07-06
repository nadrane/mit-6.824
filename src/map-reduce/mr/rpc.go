package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type DelegateWorkArgs struct{}
type DelegateWorkReply struct {
	FilePath    string
	FileName    string
	PieceNumber int
	MasterState string
}

type HeartbeatArgs struct {
	Id        int
	Timestamp int64
}
type HeartbeatReply struct{}

type MarkMapCompleteArgs struct {
	Piece int
}
type MarkMapCompleteReply struct{}

type GetnReduceArgs struct{}
type GetnReduceReply struct {
	NReduce int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
