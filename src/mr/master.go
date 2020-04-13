package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	Task      interface{}
	BeginChan chan bool
	DoneChan  chan bool
}

type Master struct {
	nReduce       int
	inputFiles    []string
	intFiles      [][]string
	taskChan      chan *Task
	mapTasks      []*Task
	reduceTasks   []*Task
	timeoutInSecs int
	done          bool
	lock          sync.Mutex
}

func (m *Master) taskManager(task *Task, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		m.taskChan <- task

		<-task.BeginChan

		select {
		case <-task.DoneChan:
			break
		case <-time.After(time.Duration(m.timeoutInSecs) * time.Second):
			continue
		}
	}
}

func (m *Master) runTasks(taskMaker func(int) interface{}, tasks []*Task, nTasks int) {
	var wg sync.WaitGroup
	// init tasks
	for i := 0; i < nTasks; i++ {
		tasks[i] = &Task{
			Task:      taskMaker(i),
			BeginChan: make(chan bool),
			DoneChan:  make(chan bool),
		}
	}
	wg.Add(nTasks)
	// run task managers
	for _, task := range tasks {
		go m.taskManager(task, &wg)
	}
	wg.Wait()
}

func (m *Master) newMapTask(i int) interface{} {
	return &MapTask{
		TaskNum:  i,
		NReduce:  m.nReduce,
		FileName: m.inputFiles[i],
	}
}

func (m *Master) newReduceTask(i int) interface{} {
	return &ReduceTask{
		TaskNum:  i,
		IntFiles: m.intFiles[i],
	}
}

func (m *Master) run() {
	m.server()
	m.runTasks(m.newMapTask, m.mapTasks, len(m.inputFiles))
	m.runTasks(m.newReduceTask, m.reduceTasks, m.nReduce)
	m.done = true
}

// Your code here -- RPC handlers for the worker to call.

// WorkerReady allocates the worker a task to perform
func (m *Master) WorkerReady(args *WorkerReadyArgs, reply *WorkerReadyReply) error {
	task := <-m.taskChan
	task.BeginChan <- true
	prepWorkerReadyReply(task.Task, reply)
	return nil
}

// MapTaskDone signals completion of a map task
func (m *Master) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	m.mapTasks[args.TaskNum].DoneChan <- true
	m.lock.Lock()
	m.intFiles[args.TaskNum] = args.IntFiles
	m.lock.Unlock()
	return nil
}

// ReduceTaskDone signals completion of a reduce task
func (m *Master) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	m.reduceTasks[args.TaskNum].DoneChan <- true
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

// Done is called periodically by main/mrmaster.go to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.done
}

// MakeMaster create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:       nReduce,
		inputFiles:    files,
		intFiles:      make([][]string, 0, nReduce),
		taskChan:      make(chan *Task),
		mapTasks:      make([]*Task, len(files)),
		reduceTasks:   make([]*Task, nReduce),
		timeoutInSecs: 10,
		done:          false,
	}

	go m.run()
	return &m
}
