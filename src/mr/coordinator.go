package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TaskStatus int

const (
	TaskPending TaskStatus = iota
	TaskInProgress
	TaskCompleted
)

type Task struct {
	TaskID   int
	Filename string
}

type Coordinator struct {
	mapTasks    []Task
	reduceTasks []Task

	mapTaskStatus    map[int]TaskStatus
	reduceTaskStatus map[int]TaskStatus

	inputFiles        []string
	intermediateFiles map[int][]string

	nReduce int
	timeout time.Duration
}

// RPC call to get the number of reduce tasks
type GetNReduceArgs struct{}
type GetNReduceReply struct {
	NReduce int
}

func (c *Coordinator) GetNReduce(args *GetNReduceArgs, reply *GetNReduceReply) error {
	reply.NReduce = c.nReduce
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// Get a task from the queue, then set it as InProgres so
	// that we can't get it again
	// Assign the task to the TaskReply object
	return nil
}

func (c *Coordinator) ReportCompletion(args *TaskArgs, reply *TaskReply) error {
	// Once the worker is done, tell the server it was completed and update
	// its task status
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:          make([]Task, len(files)),
		reduceTasks:       make([]Task, nReduce),
		mapTaskStatus:     make(map[int]TaskStatus),
		reduceTaskStatus:  make(map[int]TaskStatus),
		inputFiles:        files,
		intermediateFiles: make(map[int][]string),
		nReduce:           nReduce,
		timeout:           10 * time.Second,
	}

	// Initialize task queues
	for i, file := range files {
		c.mapTasks[i] = Task{TaskID: i, Filename: file}
		c.mapTaskStatus[i] = TaskPending
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{TaskID: i}
		c.reduceTaskStatus[i] = TaskPending
	}
	c.server()
	return &c
}
