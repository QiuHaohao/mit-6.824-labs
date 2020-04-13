package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := workerReady()
		if mapTask := reply.MapTask; mapTask != nil {
			intFilenames, err := runMapTask(mapf, mapTask)
			if err != nil {
				log.Fatal(err)
				continue
			}
			mapTaskDone(mapTask.TaskNum, intFilenames)
		} else if reduceTask := reply.ReduceTask; reduceTask != nil {
			err := runReduceTask(reducef, reduceTask)
			if err != nil {
				log.Fatal(err)
				continue
			}
			reduceTaskDone(reduceTask.TaskNum)
		}
	}
}

func runMapTask(mapf func(string, string) []KeyValue, mapTask *MapTask) ([]string, error) {
	content, err := readTextFile(mapTask.FileName)
	if err != nil {
		return nil, err
	}
	kvs := mapf(mapTask.FileName, content)
	return writeIntFiles(mapTask.TaskNum, mapTask.NReduce, kvs)
}

// TO-DO: Imp
func runReduceTask(reducef func(string, []string) string, reduceTask *ReduceTask) error {
	var intKvs []KeyValue
	for _, intFilename := range reduceTask.IntFilenames {
		intKvsFile, err := readIntFile(intFilename)
		if err != nil {
			return err
		}
		intKvs = append(intKvs, intKvsFile...)
	}
	sort.Sort(ByKey(intKvs))

	oname := fmt.Sprintf("mr-out-%d", reduceTask.TaskNum)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(intKvs) {

		j := i + 1
		for j < len(intKvs) && intKvs[j].Key == intKvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intKvs[k].Value)
		}
		output := reducef(intKvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intKvs[i].Key, output)

		i = j
	}
	return nil
}

func workerReady() *WorkerReadyReply {
	reply := &WorkerReadyReply{}
	call("Master.WorkerReady", &WorkerReadyArgs{}, reply)

	return reply
}

func mapTaskDone(taskNum int, intFilenames []string) {
	reply := &MapTaskDoneReply{}
	call("Master.MapTaskDone", &MapTaskDoneArgs{
		TaskNum:      taskNum,
		IntFilenames: intFilenames,
	}, reply)
}

func reduceTaskDone(taskNum int) {
	reply := &ReduceTaskDoneReply{}
	call("Master.ReduceTaskDone", &ReduceTaskDoneArgs{
		TaskNum: taskNum,
	}, reply)
}

// IO functions

func readTextFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func writeIntFiles(taskNum int, nReduce int, kvs []KeyValue) ([]string, error) {
	// figure out busket sizes
	busketSizes := make([]int, nReduce)
	for _, kv := range kvs {
		hash := ihash(kv.Key) % nReduce
		busketSizes[hash]++
	}
	// init buskets with correct sizes
	buskets := make([][]KeyValue, 0, nReduce)
	for _, busketSize := range busketSizes {
		buskets = append(buskets, make([]KeyValue, 0, busketSize))
	}
	// split into buskets
	for _, kv := range kvs {
		hash := ihash(kv.Key) % nReduce
		buskets[hash] = append(buskets[hash], kv)
	}
	// prepare file names
	intFilenames := make([]string, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		intFilenames = append(intFilenames, fmt.Sprintf("mr-%d-%d", taskNum, i))
	}
	// write files
	for i, intFilename := range intFilenames {
		err := writeIntFile(intFilename, buskets[i])
		if err != nil {
			return nil, err
		}
	}
	return intFilenames, nil
}

func writeIntFile(intFilename string, kvsBusket []KeyValue) error {
	intFile, err := os.Create(intFilename)
	if err != nil {
		return err
	}
	defer intFile.Close()
	enc := json.NewEncoder(intFile)
	for _, kv := range kvsBusket {
		err := enc.Encode(kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func readIntFile(intFilename string) ([]KeyValue, error) {
	intFile, err := os.Open(intFilename)
	if err != nil {
		return nil, err
	}
	defer intFile.Close()
	dec := json.NewDecoder(intFile)
	var kvs []KeyValue
	for dec.More() {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			return nil, err
		}
		kvs = append(kvs, kv)
	}
	return kvs, nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
