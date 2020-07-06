package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
)

type MasterState int

const (
	masterMapping MasterState = iota
	masterReducing
	masterComplete
)

type ChunkState int

const (
	newChunk ChunkState = iota
	mappingChunk
	completeChunk
)

type PartitionState int

const (
	newPartition PartitionState = iota
	reducingPartition
	completePartition
)

type WorkerData struct {
	id            int
	lastHeartbeat int64
}

// Input file chunks
type Chunk struct {
	pieceNumber          int // Unique identifier for this input chunk
	state                ChunkState
	name                 string // The file name
	filepath             string // The absolute file path
	intermediateFilePath string // The location of the results from mapping this file
	owner                int    // Which worker is working on this chunk
}

type ReducePartition struct {
	partitionNumber int
	state           PartitionState
}

type Master struct {
	chunks     map[int]*Chunk
	workers    map[int]*WorkerData
	partitions map[int]*ReducePartition
	nReduce    int
	nMap       int
	chunkLock  sync.Mutex
	workerLock sync.Mutex
}

func (m *Master) MarkMapComplete(args *MarkMapCompleteArgs, reply *MarkMapCompleteReply) error {

	m.chunkLock.Lock()
	m.chunks[args.Piece].state = completeChunk
	m.chunkLock.Unlock()

	return nil
}

func (m *Master) MarkPartitionComplete(args *MarkPartitionCompleteArgs, reply *MarkPartitionCompleteReply) error {

	m.chunkLock.Lock()
	m.partitions[args.Partition].state = completePartition
	m.chunkLock.Unlock()

	return nil
}

func (m *Master) DelegateWork(args *DelegateWorkArgs, reply *DelegateWorkReply) error {
	masterState := m.GetState()

	m.chunkLock.Lock()
	defer m.chunkLock.Unlock()

	if masterState == masterMapping {
		// TODO What happens when there are no available chunks?
		chunk := m.FindNextChunk()
		chunk.state = mappingChunk

		reply.FileName = chunk.name
		reply.FilePath = chunk.filepath
		reply.PieceNumber = chunk.pieceNumber
	} else if masterState == masterReducing {
		// TODO What happens when there are no available partition?
		partition := m.FindNextPartition()
		partition.state = reducingPartition

		reply.PartitionNumber = partition.partitionNumber
	}
	reply.MasterState = masterState

	return nil
}

func (m *Master) GetState() MasterState {
	m.chunkLock.Lock()
	defer m.chunkLock.Unlock()

	for _, chunk := range m.chunks {
		if chunk.state == newChunk || chunk.state == mappingChunk {
			return masterMapping
		}
	}

	// All chunks must be complete. Start looking at partitions now to see if
	// those are complete
	for _, partition := range m.partitions {
		if partition.state == newPartition || partition.state == reducingPartition {
			return masterReducing
		}
	}

	// All partitions must be complete
	return masterComplete

}

func (m *Master) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	m.workerLock.Lock()
	defer m.workerLock.Unlock()

	_, exists := m.workers[args.Id]
	if exists {
		m.workers[id].lastHeartbeat = args.Timestamp
	} else {
		m.workers[id] = &WorkerData{id: args.Id, lastHeartbeat: args.Timestamp}
	}

	return nil
}

func (m *Master) GetConfig(args *GetConfigArgs, reply *GetConfigReply) error {
	reply.NReduce = m.nReduce
	reply.NMap = m.nMap
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Find a new chunk
func (m *Master) FindNextChunk() *Chunk {
	var ret *Chunk

	for _, ret = range m.chunks {
		if ret.state == newChunk {
			return ret
		}
	}

	return ret
}

// Find a new partition
func (m *Master) FindNextPartition() *ReducePartition {
	var ret *ReducePartition

	for _, ret = range m.partitions {
		if ret.state == newPartition {
			return ret
		}
	}

	return ret
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nReduce: nReduce, nMap: len(files)}
	m.workers = make(map[int]*WorkerData)
	m.chunks = make(map[int]*Chunk)
	m.partitions = make(map[int]*ReducePartition)

	for i, file := range files {
		absFilePath, err := filepath.Abs(file)

		if err != nil {
			fmt.Println("Error converting file to absolute path")
		}

		m.chunks[i] = &Chunk{
			filepath:    absFilePath,
			name:        file,
			pieceNumber: i,
			state:       newChunk,
			owner:       -1,
		}
	}

	for i := 0; i < nReduce; i++ {
		m.partitions[i] = &ReducePartition{
			partitionNumber: i,
			state:           newPartition,
		}
	}

	m.server()
	return &m
}
