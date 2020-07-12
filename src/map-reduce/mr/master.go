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
	"time"
)

type MasterState string

const (
	masterMapping  MasterState = "mapping"
	masterReducing             = "reducing"
	masterComplete             = "complete"
)

type ChunkState string

const (
	newChunk      ChunkState = "new chunk"
	mappingChunk             = "mapping chunk"
	completeChunk            = "compelte chunk"
)

type PartitionState string

const (
	newPartition      PartitionState = "new partition"
	reducingPartition                = "reducing partition"
	completePartition                = "complete partition"
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
	owner           int // which worker is processing this partition
}

type Master struct {
	chunks        map[int]*Chunk
	workers       map[int]*WorkerData
	partitions    map[int]*ReducePartition
	nReduce       int
	nMap          int
	chunkLock     sync.Mutex
	partitionLock sync.Mutex
	workerLock    sync.Mutex
}

func (m *Master) MarkMapComplete(args *MarkMapCompleteArgs, reply *MarkMapCompleteReply) error {

	m.chunkLock.Lock()
	m.chunks[args.Piece].state = completeChunk
	fmt.Println("chunk mapped", args.Piece)
	m.chunkLock.Unlock()

	return nil
}

func (m *Master) MarkPartitionComplete(args *MarkPartitionCompleteArgs, reply *MarkPartitionCompleteReply) error {

	m.chunkLock.Lock()
	m.partitions[args.Partition].state = completePartition
	fmt.Println("partition reduced", args.Partition)
	m.chunkLock.Unlock()

	if m.GetState() == masterComplete {
		fmt.Println("Map Reduce Job Complete")
		os.Exit(0)
	}

	return nil
}

func (m *Master) DelegateWork(args *DelegateWorkArgs, reply *DelegateWorkReply) error {
	masterState := m.GetState()

	m.chunkLock.Lock()
	defer m.chunkLock.Unlock()

	m.partitionLock.Lock()
	defer m.partitionLock.Unlock()

	if masterState == masterMapping {
		chunk, readyForWork := m.findNextChunk()

		if readyForWork {
			chunk.state = mappingChunk
			chunk.owner = args.WorkerId
			reply.FileName = chunk.name
			reply.FilePath = chunk.filepath
			reply.PieceNumber = chunk.pieceNumber
		} else {
			reply.Busy = true
		}
	} else if masterState == masterReducing {
		partition, readyForWork := m.findNextPartition()

		if readyForWork {
			partition.state = reducingPartition
			partition.owner = args.WorkerId
			reply.PartitionNumber = partition.partitionNumber
		} else {
			reply.Busy = true
		}
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
	m.partitionLock.Lock()
	defer m.partitionLock.Unlock()

	for _, partition := range m.partitions {
		if partition.state == newPartition || partition.state == reducingPartition {
			return masterReducing
		}
	}

	// All partitions must be complete
	return masterComplete

}

func (m *Master) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	fmt.Println("PROCESSING HEARTBEAT", args.Id)
	m.workerLock.Lock()
	_, exists := m.workers[args.Id]
	if exists {
		m.workers[args.Id].lastHeartbeat = args.Timestamp
	} else {
		fmt.Println("registering new worker", args.Id)
		m.workers[args.Id] = &WorkerData{id: args.Id, lastHeartbeat: args.Timestamp}
	}
	m.workerLock.Unlock()

	m.identifyAndResetFailedWokers()

	return nil
}

func (m *Master) identifyAndResetFailedWokers() {
	masterState := m.GetState()

	m.workerLock.Lock()
	defer m.workerLock.Unlock()
	m.chunkLock.Lock()
	defer m.chunkLock.Unlock()
	m.partitionLock.Lock()
	defer m.partitionLock.Unlock()

	fmt.Println("EXPIRING WORKERS")
	currentTime := time.Now().Unix()
	fmt.Println("Current Time", currentTime)
	expiredWorkers := make(map[int]*WorkerData)
	fmt.Println("all workers", m.workers)
	for _, worker := range m.workers {
		fmt.Println("Worker", worker.id, worker.lastHeartbeat)
		if worker.lastHeartbeat+10 < currentTime {
			fmt.Println("Expired worker found")
			expiredWorkers[worker.id] = worker
		}
	}

	if masterState == masterMapping {
		fmt.Println("ALL CHUNKS")
		for _, chunk := range m.chunks {
			fmt.Println(chunk)
			_, isExpired := expiredWorkers[chunk.owner]
			if isExpired {
				fmt.Println("expired chunk found", chunk)
				chunk.state = newChunk
				chunk.owner = -1
			}
		}
	} else if masterState == masterReducing {
		for _, partition := range m.partitions {
			_, isExpired := expiredWorkers[partition.owner]
			if isExpired {
				partition.state = newPartition
				partition.owner = -1
			}
		}
	}

	for workerId := range expiredWorkers {
		delete(m.workers, workerId)
	}

	fmt.Println()
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
func (m *Master) findNextChunk() (*Chunk, bool) {
	var ret *Chunk

	for _, ret = range m.chunks {
		if ret.state == newChunk {
			return ret, true
		}
	}

	return ret, false
}

// Find a new partition
func (m *Master) findNextPartition() (*ReducePartition, bool) {
	var ret *ReducePartition

	for _, ret = range m.partitions {
		if ret.state == newPartition {
			return ret, true
		}
	}

	return ret, false
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
