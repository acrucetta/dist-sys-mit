package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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
	reduceValuess := make([][]KeyValue, nReduce)

	for {
		taskReply := GetMapTask()

		if taskReply.TasksDone {
			break
		}

		filename := taskReply.Task.Filename

		fmt.Printf("Processing file: %v\n", filename)
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
			reduceValuess[reduceTask] = append(reduceValuess[reduceTask], kv)
		}

		// Write to an reduceValues file
		taskId := taskReply.Task.ID
		for i := 0; i < nReduce; i++ {
			iname := fmt.Sprintf("mr-%d-%d", taskId, i)
			fmt.Printf("Creating file %v for the task %v\n", iname, taskId)
			ifile, _ := os.Create(iname)
			// Write all the kv pairs inside of the []KeyValue bucket to ifile
			enc := json.NewEncoder(ifile)
			for _, kv := range reduceValuess[i] {
				if err := enc.Encode(&kv); err != nil {
					log.Fatalf("cannot encode %v", kv)
				}
			}
			ifile.Close()
		}
		ReportTaskCompletion(taskId, MapTask)
	}
	// Once they're all finished, we can start with reduce
	// Reduce will grab each of the reduceValues files, aggregate all of the keys
	// print them to an output file
	for {
		// Read a file from the file system
		taskReply := GetReduceTask()

		if taskReply.TasksDone {
			break
		}

		taskId := taskReply.Task.ID

		// 1. Get all the reduceValues files for the given task id
		// We need to grab all the reduceValues "Y" files by mr-X-Y

		reduceValues := []KeyValue{}
		fmt.Printf("We have %v nreduce tasks\n", nReduce)
		for i := 0; i < nReduce-1; i++ {
			// 	Read file, append all keys to an reduceValues file
			filename := fmt.Sprintf("mr-%d-%d", taskId, i)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				reduceValues = append(reduceValues, kv)
			}
		}
		sort.Sort(ByKey(reduceValues))

		// 3. Create the output file
		oname := fmt.Sprintf("mr-out-%d", taskId)
		ofile, _ := os.Create(oname)

		// 4. Call reducef on each key and write results
		i := 0
		for i < len(reduceValues) {
			j := i + 1
			for j < len(reduceValues) && reduceValues[j].Key == reduceValues[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, reduceValues[k].Value)
			}
			output := reducef(reduceValues[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", reduceValues[i].Key, output)
			i = j
		}

		// 5. Report task completion
		ReportTaskCompletion(taskId, ReduceTask)
		ofile.Close()
	}
}

func ReportTaskCompletion(taskId int, taskType TaskType) {
	args := TaskCompletionArgs{TaskId: taskId, TaskType: taskType}
	reply := TaskCompletionReply{}

	// Send RPC request, wait for the reply
	ok := call("Coordinator.ReportTaskCompletion", &args, &reply)
	if ok {
		fmt.Printf("Reported task completion for %v\n", taskId)
	} else {
		fmt.Printf("call failed!\n")
		panic(1)
	}
}

func CallNReduce() GetNReduceReply {
	args := GetNReduceArgs{}
	reply := GetNReduceReply{}

	// Send RPC request, wait for the reply
	ok := call("Coordinator.GetNReduce", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
		panic(1)
	}
}

func GetMapTask() TaskReply {
	// Request a task from the coordinator
	args := TaskArgs{}
	reply := TaskReply{}

	// Send RPC request, wait for the reply
	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
		panic(1)
	}
}

func GetReduceTask() TaskReply {
	// Request a task from the coordinator
	args := TaskArgs{}
	reply := TaskReply{}

	// Send RPC request, wait for the reply
	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
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
		panic(1)
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
