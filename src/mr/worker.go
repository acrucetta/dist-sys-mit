package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"

	"6.5840/mr"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	nReduceReply := CallNReduce()
	nReduce := nReduceReply.NReduce
	intermediates := make([][]KeyValue, nReduce)

	for {
		taskReply := CallTask()

		if !taskReply.Valid {
			// No more tasks
			break
		}
		filename := taskReply.Task.Filename
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			reduceTask := ihash(kv.Key) % nReduce
			intermediates[reduceTask] = append(intermediates[reduceTask], kv)
		}

		// Write to an intermediate file
		taskId := taskReply.Task.TaskID
		for i := 0; i < nReduce; i++ {
			iname := fmt.Sprintf("mr-%i-%i", taskId, nReduce)
			ifile, _ := os.Create(iname)
			// Write all the kv pairs inside of the []KeyValue bucket to ifile
			for _, kv := range intermediates[nReduce] {
				fmt.Fprintf(ifile, "%v %v\n", kv.Key, kv.Value)
			}
		}
	}

	// Once they're all finished, we can start with reduce
	// Reduce will grab each of the intermediate files, aggregate all of the keys
	// print them to an output file
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
}

func CallNReduce() GetNReduceReply {
	args := GetNReduceArgs{}
	reply := GetNReduceReply{}

	// Send RPC request, wait for the reply
	ok := call("Coordinator.GetNReduce", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("No nReduce found")
		panic(1)
	}
}

func CallTask() TaskReply {
	// Request a task from the coordinator
	args := TaskArgs{}
	reply := TaskReply{}

	// Send RPC request, wait for the reply
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("The task is good")
		return reply
	} else {
		fmt.Printf("Task is no good")
		panic(1)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
