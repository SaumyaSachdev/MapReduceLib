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

const (
	MapType    = 0
	ReduceType = 1
)

const (
	Idle       = 0
	InProgress = 1
	Completed  = 2
)

type Task struct {
	TaskId            int
	TaskType          int
	TaskStatus        int
	NReduce           int
	WorkerId          int
	File              string
	IntermediateFiles []string
	timer             *time.Timer
}

type Coordinator struct {
	// Your definitions here.
	Files           []string
	NumMapTasks     int
	NReduce         int
	Tasks           []*Task
	compMapTasks    int
	compReduceTasks int
	Mutex           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

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
	// l, e := net.Listen("tcp", "128.6.13.146:1234")
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) IsDone(req *AllTasksDoneRequest, res *AllTasksDoneResponse) error {
	res.Done = c.Done()
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.compMapTasks+c.compReduceTasks == len(c.Tasks)
}

// create a Coordinator.
// create map task for each file and nReduce reduce tasks
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NReduce = nReduce
	c.Files = files
	c.NumMapTasks = len(files)
	c.compMapTasks = 0
	c.compReduceTasks = 0
	numFiles := len(files)
	// Your code here.
	// initialize map tasks for all input files
	c.Tasks = make([]*Task, numFiles+nReduce)
	for i := 0; i < numFiles; i++ {
		c.Tasks[i] = &Task{
			TaskId:     i,
			TaskType:   MapType,
			TaskStatus: Idle,
			NReduce:    nReduce,
			File:       files[i],
		}
	}
	for i := 0; i < nReduce; i++ {
		temp := &Task{
			TaskId:     i + numFiles,
			TaskType:   ReduceType,
			TaskStatus: Idle,
		}
		interFiles := make([]string, numFiles)
		for j := 0; j < numFiles; j++ {
			interFiles[j] = fmt.Sprintf("mr-%d-%d", j, i)
		}
		temp.IntermediateFiles = interFiles
		c.Tasks[i+numFiles] = temp
	}
	fmt.Printf("Coordinator created.\n")
	fmt.Printf("Tasks created. \n")
	// for i := 0; i < len(c.Tasks); i++ {
	// 	fmt.Printf("%+v\n", c.Tasks[i])
	// }

	c.server()
	return &c
}

func (c *Coordinator) ChangeStatus(req *NotifyChangeRequest, res *NotifyChangeResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	temp := c.Tasks[req.TaskId]
	if temp == nil {
		return nil
	}
	if req.WorkerId == temp.WorkerId {
		temp.TaskStatus = req.TaskStatus
	}
	if temp.TaskType == MapType {
		c.compMapTasks++
		fmt.Printf("# map completed: %d\n", c.compMapTasks)
	} else {
		c.compReduceTasks++
		fmt.Printf("# reduce completed: %d\n", c.compReduceTasks)
	}
	return nil
}

// check the list of pending task and assign either a map or reduce task
// return "map"
func (c *Coordinator) GetTask(req *GetTaskRequest, res *GetTaskResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// assign all Map tasks first
	for _, current := range c.Tasks {

		task := current
		if task.TaskStatus == Idle {
			if task.TaskType == ReduceType && c.compMapTasks != c.NumMapTasks {
				continue
			}
			task.WorkerId = req.WorkerId
			task.TaskStatus = InProgress
			res.Task = task
			if task.timer != nil {
				task.timer.Stop()
				task.timer = nil
			}
			task.timer = time.AfterFunc(30*time.Second, func() {
				c.Mutex.Lock()
				defer c.Mutex.Unlock()
				if task.TaskStatus != Completed {
					task.TaskStatus = Idle
					task.WorkerId = 0
				}
			})
			return nil
		}
	}
	return nil
}
