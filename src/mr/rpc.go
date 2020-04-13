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

// MapTask describes a map task
type MapTask struct {
	TaskNum  int
	NReduce  int
	FileName string
}

// ReduceTask describes a reduce task
type ReduceTask struct {
	TaskNum      int
	IntFilenames []string
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// WorkerReadyArgs is sent when a worker is ready to start a new task
type WorkerReadyArgs struct{}

// WorkerReadyReply contains a pointer to either a mapTask or a reduceTask.
// Pointer to MapTask is nil if the task allocated is a reduce task,
// and vice versa
type WorkerReadyReply struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

// MapTaskDoneArgs is sent once the worker finishes a map task
type MapTaskDoneArgs struct {
	TaskNum      int
	IntFilenames []string
}

// MapTaskDoneReply is empty
type MapTaskDoneReply struct{}

// ReduceTaskDoneArgs is sent once the worker finishes a reduce task, and it's empty
type ReduceTaskDoneArgs struct {
	TaskNum int
}

// ReduceTaskDoneReply is empty
type ReduceTaskDoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
