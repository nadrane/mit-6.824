package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
type IntermediateFiles = map[int]*os.File
type Partitions = map[int][]KeyValue
type MapFunc = func(string, string) []KeyValue
type ReduceFunc = func(string, []string) string
type GroupByKey = map[string][]string
type ReduceResults = map[string]string

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var id = rand.Int()

func getPartitionNumber(key string, nReduce int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7fffffff) % nReduce
}

func mapNext(job DelegateWorkReply, mapf MapFunc, config GetConfigReply) {
	// Map file contents
	content := ReadFile(job.FilePath)
	keyValues := mapf(job.FileName, string(content))

	// Write out keys to correct files
	for partitionKey, kvs := range groupByPartition(keyValues, config.NReduce) {
		intermediateFilePath := GenerateFileName(job.PieceNumber, partitionKey)
		fileHandler, err := os.Create(intermediateFilePath)

		enc := json.NewEncoder(fileHandler)
		valueByKey := groupByKey(kvs)
		err = enc.Encode(&valueByKey)
		if err != nil {
			fmt.Println("Failed to encode json", err)
		}

		fileHandler.Close()
	}

	markMapComplete(job.PieceNumber)
}

func reduceNext(job DelegateWorkReply, reducef ReduceFunc, config GetConfigReply) {
	allGroups := make(GroupByKey)

	for i := 0; i < config.NMap; i++ {
		fileName := GenerateFileName(i, job.PartitionNumber)

		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		dec := json.NewDecoder(file)
		nextGroup := make(GroupByKey)
		err = dec.Decode(&nextGroup)
		if err != nil {
			fmt.Println("cannot read json file", err)
		}

		allGroups = mergeGroups(allGroups, nextGroup)
	}

	// Collect results
	var results = make(ReduceResults)
	for key := range allGroups {
		results[key] = reducef(key, allGroups[key])
	}

	// Sort keys
	sortedKeys := make([]string, 0, len(allGroups))
	for k := range allGroups {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Create temp file
	tempFile := GenerateTempFile()

	// Write results to temp file
	datawriter := bufio.NewWriter(tempFile)

	for _, k := range sortedKeys {
		_, err := datawriter.WriteString(k + " " + results[k] + "\n")
		if err != nil {
			fmt.Println("Failed to write results to temp file", err)
		}
	}

	datawriter.Flush()

	// Atomically rename temp file
	os.Rename(tempFile.Name(), "mr-out-"+strconv.Itoa(job.PartitionNumber))

	markPartitionComplete(job.PartitionNumber)
}

func groupByPartition(kvs []KeyValue, nReduce int) Partitions {
	partitions := make(Partitions)

	for _, kv := range kvs {
		partition := getPartitionNumber(kv.Key, nReduce)
		partitions[partition] = append(partitions[partition], kv)
	}

	return partitions
}

func groupByKey(kvs []KeyValue) GroupByKey {
	groups := make(map[string][]string)

	for _, kv := range kvs {
		existingValues, ok := groups[kv.Key]
		if ok {
			groups[kv.Key] = append(existingValues, kv.Value)
		} else {
			groups[kv.Key] = []string{kv.Value}
		}
	}

	return groups
}

func mergeGroups(group1 GroupByKey, group2 GroupByKey) GroupByKey {
	for key, values := range group2 {
		_, ok := group1[key]

		if ok {
			group1[key] = append(group1[key], values...)
		} else {
			group1[key] = values
		}
	}

	return group1
}

func Worker(mapf MapFunc, reducef ReduceFunc) {
	config := getConfig()
	go loopHeartbeat()

	loopStateFunction(mapf, reducef, config)
}

func loopHeartbeat() {
	for {
		Heartbeat()
		time.Sleep(2 * time.Second)
	}
}

func loopStateFunction(mapf MapFunc, reducef ReduceFunc, config GetConfigReply) {
	for {
		nextJob := getWork()
		// fmt.Println("nextjob", id, nextJob)
		if nextJob.Busy {
			time.Sleep(2 * time.Second)
		} else if nextJob.MasterState == masterMapping {
			mapNext(nextJob, mapf, config)
		} else if nextJob.MasterState == masterReducing {
			reduceNext(nextJob, reducef, config)
		} else if nextJob.MasterState == masterComplete {
			fmt.Println("done!")
			os.Exit(0)
		}
	}
}

// RPC Functions

func getConfig() GetConfigReply {

	args := GetConfigArgs{}
	reply := GetConfigReply{}

	// send the RPC request, wait for the reply.
	call("Master.GetConfig", &args, &reply)
	return reply
}

func getWork() DelegateWorkReply {

	args := DelegateWorkArgs{}
	reply := DelegateWorkReply{}

	// send the RPC request, wait for the reply.
	call("Master.DelegateWork", &args, &reply)

	return reply
}

func Heartbeat() HeartbeatReply {

	args := HeartbeatArgs{Id: id, Timestamp: time.Now().Unix()}
	reply := HeartbeatReply{}

	// send the RPC request, wait for the reply.
	call("Master.Heartbeat", &args, &reply)

	return reply
}

func markMapComplete(piece int) {
	args := MarkMapCompleteArgs{Piece: piece}
	reply := MarkMapCompleteReply{}

	// send the RPC request, wait for the reply.
	call("Master.MarkMapComplete", &args, &reply)

	return
}

func markPartitionComplete(partition int) {
	args := MarkPartitionCompleteArgs{Partition: partition}
	reply := MarkPartitionCompleteReply{}

	// send the RPC request, wait for the reply.
	call("Master.MarkPartitionComplete", &args, &reply)

	return
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
//TODO need to handle errors in here
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
