package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	idleState         int = 0
	inProgressState   int = 1
	completedState    int = 2
	allCompletedState int = 3
	mapTaskType       int = 0
	reduceTaskType    int = 1
	taskTimeOut       int = 10
	taskNumNone       int = -1
)

type Task struct {
	fileName    string
	curWorkerId []int
	state       int
	taskType    int
	taskNum     int
}

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	waitTasks         []Task
	nReduce           int
	nMap              int
	cntCompleteMap    int
	cntCompleteReduce int
}

func (c *Coordinator) lock() {
	c.mu.Lock()
}

func (c *Coordinator) unlock() {
	c.mu.Unlock()
}

// Your code here -- RPC handlers for the worker to call.
type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	FileName string
	TaskType int
	TaskNum  int
	NReduce  int
	NMap     int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.lock()
	defer c.unlock()

	if c.cntCompleteReduce == c.nReduce {
		reply.TaskType = allCompletedState
		return nil
	}

	// P1: 这里存在一种情况，有些任务仍在处理中，满足条件c.cntCompleteMap < c.nMap，但是实际上里边已经没有空闲任务。
	if c.cntCompleteMap < c.nMap {
		// map task are done
		for i := 0; i < c.nMap; i++ {
			task := &c.waitTasks[i]
			if task.state == idleState {
				task.state = inProgressState
				task.curWorkerId = append(task.curWorkerId, args.WorkerId)
				c.setReplyInGetTask(args, reply, task)
				return nil
			}
		}
	} else {
		// all map task are done, start reduce
		for i := c.nMap; i < c.nMap+c.nReduce; i++ {
			task := &c.waitTasks[i]
			if task.state == idleState {
				task.state = inProgressState
				task.curWorkerId = append(task.curWorkerId, args.WorkerId)
				c.setReplyInGetTask(args, reply, task)
				return nil
			}
		}
	}

	// P1: 处理P1情况，设置一个默认的值，表示当前没有任务可以被处理。
	reply.TaskNum = taskNumNone

	return nil
}

func (c *Coordinator) setReplyInGetTask(args *GetTaskArgs, reply *GetTaskReply, task *Task) {
	reply.FileName = task.fileName
	reply.TaskType = task.taskType
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap
	reply.TaskNum = task.taskNum

	// P2: 避免掉队的节点，但这样做存在一个问题，掉队节点与新节点都完成后，c.nCompleteMap会被加两次，这显然不符合逻辑。
	go func(task *Task) {
		time.Sleep(time.Duration(taskTimeOut) * time.Second)
		c.lock()
		if task.state == inProgressState {
			log.Printf("task timeout, type %v, number %v, reset this task to idleState\n", task.taskType, task.taskNum)
			task.state = idleState
		}
		c.unlock()
	}(task)
}

type TaskFinishedArgs struct {
	TaskNum int
}
type TaskFinishedReply struct {
}

func (c *Coordinator) TaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.lock()
	defer c.unlock()
	if args.TaskNum >= len(c.waitTasks) {
		return errors.New(fmt.Sprintf("invalid args.TaskNum which is %v larger than %v", args.TaskNum, len(c.waitTasks)))
	}
	task := &c.waitTasks[args.TaskNum]
	// P2: 如果这个任务已经完成，则不做任何动作，处理P2问题。
	if task.state == completedState {
		log.Printf("task type %v, task number %v already compeleted\n", task.taskType, task.taskNum)
		return nil
	}
	task.state = completedState
	if task.taskType == mapTaskType {
		log.Printf("task type MapTask, number %v completed, c.cntCompletedMap++\n", task.taskNum)
		c.cntCompleteMap += 1
		if c.cntCompleteMap == c.nMap {
			fmt.Println("all map task are done, starting reduce task...")
			for i := 0; i < c.nReduce; i++ {
				c.waitTasks = append(c.waitTasks, Task{
					curWorkerId: []int{},
					state:       idleState,
					taskType:    reduceTaskType,
					taskNum:     i,
				})
			}
		}
	} else {
		log.Printf("task type ReduceTask, number %v completed, c.cntCompletedMap++\n", task.taskNum)
		c.cntCompleteReduce += 1
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
	c.lock()
	defer c.unlock()
	ret = c.nReduce == c.cntCompleteReduce
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.waitTasks = []Task{}
	c.nReduce = nReduce
	c.nMap = len(files)
	c.cntCompleteMap = 0
	c.cntCompleteReduce = 0

	// Your code here.
	for taskNum, fileName := range files {
		c.waitTasks = append(c.waitTasks, Task{
			fileName:    fileName,
			curWorkerId: []int{},
			state:       idleState,
			taskType:    mapTaskType,
			taskNum:     taskNum,
		})
	}

	c.server()
	return &c
}
