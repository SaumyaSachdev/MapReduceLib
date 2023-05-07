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
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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

	// Your worker implementation here.
	workerId := os.Getpid()
	for !Done() {
		// time.Sleep(time.Millisecond)
		reqArgs := GetTaskRequest{workerId}
		resArgs := GetTaskResponse{}
		// fmt.Printf("Ask for task------- \n")
		ok := call("Coordinator.GetTask", &reqArgs, &resArgs)

		task := resArgs.Task
		// fmt.Printf("Task response--------------- %+v\n", task)
		if resArgs.Task == nil || task == nil {
			continue
		}
		if ok {
			if task.TaskType == MapType {
				fmt.Printf("Map task assigned: %d\n", task.TaskId)
				PerformMapTask(mapf, task)
			} else if resArgs.Task.TaskType == ReduceType {
				fmt.Printf("Reduce task assigned: %d\n", task.TaskId)
				PerformReduceTask(reducef, task)
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func Done() bool {
	time.Sleep(time.Second)
	req := AllTasksDoneRequest{}
	res := AllTasksDoneResponse{}

	ok := call("Coordinator.IsDone", &req, &res)
	if ok {
		return res.Done
	} else {
		log.Fatal("Done call failed")
		return true
	}
}

func PerformReduceTask(reducef func(string, []string) string, task *Task) {
	intermediate := make([]KeyValue, 0)
	numFiles := len(task.IntermediateFiles)
	for i := 0; i < numFiles; i++ {
		interFile, _ := os.Open(task.IntermediateFiles[i])
		dec := json.NewDecoder(interFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	outputFN := fmt.Sprintf("mr-out-%d", task.TaskId-numFiles)
	outputFile, _ := os.Create(outputFN)
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
			// fmt.Println("intermediate key: ", intermediate[i].Key, "\tvalue: ", intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	outputFile.Close()
	fmt.Printf("Reduce task completed: %d\n", task.TaskId)

	// notify coordinator of map task complete
	req := NotifyChangeRequest{
		TaskId:     task.TaskId,
		TaskStatus: Completed,
		WorkerId:   task.WorkerId,
	}
	res := NotifyChangeResponse{}
	ok := call("Coordinator.ChangeStatus", &req, &res)
	if !ok {
		log.Fatalf("ChangeStatus for Reduce failed")
	}
}

func PerformMapTask(mapf func(string, string) []KeyValue, task *Task) {
	// fmt.Printf("MAP TASK STARTED: %d\n", task.TaskId)
	intermediate := make([]KeyValue, 0)
	task.TaskStatus = InProgress
	filename := task.File
	mapFile, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(mapFile)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	mapFile.Close()
	// fmt.Printf("INSIDE MAP TASK 1111111111: %d\n", task.TaskId)
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	encArr := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		interFN := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		interFile, _ := os.Create(interFN)
		encArr[i] = json.NewEncoder(interFile)
	}
	// fmt.Printf("INSIDE MAP TASK 2222222222: %d\n", task.TaskId)
	// hash the key for each intermediate pair and add to the respective file
	for _, inter := range intermediate {
		mapIdx := ihash(inter.Key) % task.NReduce
		err := encArr[mapIdx].Encode(inter)
		if err != nil {
			log.Fatalf("error encoding")
		}
	}
	fmt.Printf("Map task completed: %d\n", task.TaskId)
	// notify coordinator of map task complete
	req := NotifyChangeRequest{
		TaskId:     task.TaskId,
		TaskStatus: Completed,
		WorkerId:   task.WorkerId,
	}
	res := NotifyChangeResponse{}
	ok := call("Coordinator.ChangeStatus", &req, &res)
	if !ok {
		log.Fatalf("ChangeStatus for Map failed")
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
	// c, err := rpc.DialHTTP("tcp", "128.6.13.146:1234")
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
