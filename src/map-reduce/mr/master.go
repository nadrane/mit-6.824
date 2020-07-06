package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
)

type WorkerData struct {
	id            int
	lastHeartbeat int64
}

type Chunk struct {
	pieceNumber          int    // Unique identifier for this input chunk
	state                string // new/mapping/mapped/reducing/complete
	name                 string // The file name
	filepath             string // The absolute file path
	intermediateFilePath string // The location of the results from mapping this file
}

type Master struct {
	chunks     map[int]Chunk
	workers    map[int]WorkerData
	nReduce    int
	chunkLock  sync.Mutex
	workerLock sync.Mutex
}

func (m *Master) MarkMapComplete(args *MarkMapCompleteArgs, reply *MarkMapCompleteReply) error {

	m.chunkLock.Lock()
	defer m.chunkLock.Unlock()

	chunk := m.chunks[args.Piece]
	m.chunks[args.Piece] = Chunk{
		filepath:    chunk.filepath,
		name:        chunk.name,
		pieceNumber: chunk.pieceNumber,
		state:       "mapped",
	}

	return nil
}

func (m *Master) DelegateWork(args *DelegateWorkArgs, reply *DelegateWorkReply) error {
	masterState := m.GetState()

	m.chunkLock.Lock()
	defer m.chunkLock.Unlock()

	var chunk Chunk
	var nextState string
	var err error

	fmt.Println("master state", masterState)
	if masterState == "mapping" {
		chunk, err = m.FindNextChunk("new")
		nextState = "mapping"
	} else if masterState == "reducing" {
		chunk, err = m.FindNextChunk("mapped")
		nextState = "reducing"
	}

	if err != nil {
		reply.MasterState = "done"
	} else {
		m.chunks[chunk.pieceNumber] = Chunk{filepath: chunk.filepath, name: chunk.name, pieceNumber: chunk.pieceNumber, state: nextState}
		reply.FileName = chunk.name
		reply.FilePath = chunk.filepath
		reply.PieceNumber = chunk.pieceNumber
		reply.MasterState = nextState
	}

	return nil

}

func (m *Master) GetState() string {
	m.chunkLock.Lock()
	defer m.chunkLock.Unlock()

	for _, chunk := range m.chunks {
		if chunk.state == "new" || chunk.state == "mapping" {
			return "mapping"
		}
	}

	for _, chunk := range m.chunks {
		if chunk.state == "mapped" || chunk.state == "reducing" {
			return "reducing"
		}
	}

	return "done"

}

func (m *Master) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	m.workerLock.Lock()
	defer m.workerLock.Unlock()

	_, exists := m.workers[args.Id]
	if exists {
		m.workers[id] = WorkerData{id: args.Id}
	} else {
		m.workers[id] = WorkerData{id: args.Id, lastHeartbeat: args.Timestamp}
	}

	return nil
}

func (m *Master) GetnReduce(args *GetnReduceArgs, reply *GetnReduceReply) error {
	reply.NReduce = m.nReduce
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

// Find a file that has not yet been processed
// Returns an empty string if all files have been processed already
func (m *Master) FindNextChunk(state string) (Chunk, error) {
	var ret Chunk

	for _, ret = range m.chunks {
		if ret.state == state {
			return ret, nil
		}
	}

	return ret, errors.New("Cannot find chunk with state " + state)
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
	m := Master{nReduce: nReduce}
	m.workers = make(map[int]WorkerData)
	m.chunks = make(map[int]Chunk)

	for i, file := range files {
		absFilePath, err := filepath.Abs(file)

		if err != nil {
			fmt.Println("Error converting file to absolute path")
		}

		m.chunks[i] = Chunk{filepath: absFilePath, name: file, pieceNumber: i, state: "new"}
	}

	m.server()
	return &m
}
