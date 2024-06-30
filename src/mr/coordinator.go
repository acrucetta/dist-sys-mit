package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	TaskPending TaskStatus = iota
	TaskInProgress
	TaskCompleted
)

type Task struct {
	ID        int
	Filename  string
	Status    TaskStatus
	StartTime time.Time
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

type Coordinator struct {
	mapTasks    map[int]Task
	reduceTasks map[int]Task

	inputFiles        []string
	intermediateFiles map[int][]string

	nReduce int
	timeout time.Duration

	mu sync.Mutex
}

// RPC call to get the number of reduce tasks
type GetNReduceArgs struct{}
type GetNReduceReply struct {
	NReduce int
}

type TaskCompletionArgs struct {
	TaskId   int
	TaskType TaskType
}

type TaskCompletionReply struct {
}

func (c *Coordinator) GetNReduce(args *GetNReduceArgs, reply *GetNReduceReply) error {
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) GetMapTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, task := range c.mapTasks {
		if task.Status == TaskPending {
			reply.Task = task
			c.mapTasks[i] = Task{ID: i, Filename: task.Filename, Status: TaskInProgress, StartTime: time.Now()}
			return nil
		}
	}
	for _, task := range c.mapTasks {
		if task.Status != TaskCompleted {
			return nil
		}
	}
	reply.TasksDone = c.AllMapTasksDone()
	return nil
}

func (c *Coordinator) GetReduceTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, task := range c.reduceTasks {
		if task.Status == TaskPending {
			reply.Task = task
			c.reduceTasks[i] = Task{ID: i, Filename: task.Filename, Status: TaskInProgress, StartTime: time.Now()}
			return nil
		}
	}
	reply.TasksDone = c.AllReduceTasksDone()
	return nil
}

func (c *Coordinator) ReportTaskCompletion(args *TaskCompletionArgs, reply *TaskCompletionReply) error {
	switch args.TaskType {
	case MapTask:
		task := c.mapTasks[args.TaskId]
		task.Status = TaskCompleted
		c.mapTasks[args.TaskId] = task
	case ReduceTask:
		task := c.reduceTasks[args.TaskId]
		task.Status = TaskCompleted
		c.reduceTasks[args.TaskId] = task
	}
	return nil
}

func (c *Coordinator) AllMapTasksDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	allTasksDone := true
	for _, task := range c.mapTasks {
		if task.Status != TaskCompleted {
			allTasksDone = false
		}
	}
	return allTasksDone
}

func (c *Coordinator) AllReduceTasksDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	allTasksDone := true
	for _, task := range c.reduceTasks {
		if task.Status != TaskCompleted {
			allTasksDone = false
		}
	}
	return allTasksDone
}

// This function will constantly iterate over the tasks and check
// if their duration has exceeded 10 seconds; if so, it will set them
// back to pending.
func (c *Coordinator) MonitorTasks() {
	for !c.AllMapTasksDone() {
		c.mu.Lock()
		println("Monitoring map tasks...")
		for i, task := range c.mapTasks {
			if task.Status == TaskInProgress {
				t := time.Now()
				elapsed := t.Sub(task.StartTime)
				if elapsed.Seconds() > 10 {
					fmt.Printf("Task %v is overdue, setting it back to Pending\n", i)
					c.mapTasks[i] = Task{ID: i, Filename: task.Filename, Status: TaskPending}
				}
			}
		}
		c.mu.Unlock()
	}
	for !c.AllReduceTasksDone() {
		println("Monitoring reduce tasks...")
		c.mu.Lock()
		for i, task := range c.reduceTasks {
			if task.Status == TaskInProgress {
				t := time.Now()
				elapsed := t.Sub(task.StartTime)
				if elapsed.Seconds() > 10 {
					fmt.Printf("Task %v is overdue, setting it back to Pending\n", i)
					c.reduceTasks[i] = Task{ID: i, Filename: task.Filename, Status: TaskPending}
				}
			}
		}
		c.mu.Unlock()
	}
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
	if c.AllMapTasksDone() && c.AllReduceTasksDone() {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:          make(map[int]Task),
		reduceTasks:       make(map[int]Task),
		inputFiles:        files,
		intermediateFiles: make(map[int][]string),
		nReduce:           nReduce,
		timeout:           10 * time.Second,
	}

	// Initialize task queues
	for i, file := range files {
		fmt.Printf("Creating task with ID: %v\n", i)
		c.mapTasks[i] = Task{ID: i, Filename: file, Status: TaskPending}
	}

	for i := range nReduce {
		c.reduceTasks[i] = Task{ID: i, Status: TaskPending}
	}
	c.server()
	go c.MonitorTasks()
	return &c
}
