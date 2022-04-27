package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	idleState       int = 0
	inProgressState int = 1
	completedState  int = 2
	mapTaskType     int = 0
	reduceTaskType  int = 1
	taskTimeOut     int = 10
)

type Task struct {
	fileName    string
	curWorkerId []int
	state       int
	taskType    int
}

type Coordinator struct {
	// Your definitions here.
	mu        sync.Mutex
	waitTasks []Task
}

func (c *Coordinator) lock() {
	c.mu.Lock()
}

func (c *Coordinator) unlock() {
	c.mu.Unlock()
}

// Your code here -- RPC handlers for the worker to call.
type GetTaskArgs struct {
	workerId int
}

type GetTaskReply struct {
	fileName string
	taskType int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.lock()
	defer c.unlock()
	for _, task := range c.waitTasks {
		if task.state == idleState {
			reply.fileName = task.fileName
			reply.taskType = task.taskType

			// prevent from straggler
			go func(task *Task) {
				time.Sleep(time.Duration(taskTimeOut) * time.Second)
				c.lock()
				task.state = idleState
				c.unlock()
			}(&task)
			task.state = inProgressState
			task.curWorkerId = append(task.curWorkerId, args.workerId)
		}
		return nil
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.waitTasks = []Task{}

	// Your code here.
	for _, fileName := range files {
		c.waitTasks = append(c.waitTasks, Task{
			fileName:    fileName,
			curWorkerId: []int{},
			state:       idleState,
			taskType:    mapTaskType,
		})
	}

	c.server()
	return &c
}
