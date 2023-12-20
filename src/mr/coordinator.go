package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	mapTasks        []MapTaskDef
	nFiles          int
	MapCompleted    map[int]string
	reduceTaskIds   []int
	nReduce         int
	ReduceCompleted map[int]bool
	cond            *sync.Cond
	mutex           sync.Mutex
	once1           sync.Once
	once2           sync.Once
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) IsMapCompleted() bool {
	return len(c.MapCompleted) == c.nFiles
}

func (c *Coordinator) IsReduceCompleted() bool {
	return len(c.ReduceCompleted) == c.nReduce
}

func (c *Coordinator) Dispatch(args *ExampleArgs, reply *TaskReply) error {
	reply.NumReduce = c.nReduce
	reply.NumMap = c.nFiles
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// dispatch a task
	for !c.DispatchImpl(reply) {
		c.cond.Wait()
	}
	return nil
}

func (c *Coordinator) DispatchImpl(reply *TaskReply) bool {
	if !c.IsMapCompleted() {
		if len(c.mapTasks) > 0 {
			c.DispatchMapTask(reply)
			return true
		} else {
			return false
		}
	} else {
		if c.IsReduceCompleted() {
			// log once
			//c.once2.Do(func() { log.Printf("Reduce completed!\n") })
			reply.Type = Terminate
			return true
		} else {
			if len(c.reduceTaskIds) > 0 {
				c.DispatchReduceTask(reply)
				//log.Printf("Dispatched Reduce Task %d\n", reply.ReduceTaskId)
				return true
			} else {
				return false
			}
		}
	}
}

func (c *Coordinator) NotifyComplete(args *TaskReply, reply *TaskReply) error {
	c.mutex.Lock()
	switch args.Type {
	case Map:
		c.MapCompleted[args.MapTask.TaskId] = args.MapTask.FileName
	case Reduce:
		c.ReduceCompleted[args.ReduceTaskId] = true
	}
	c.cond.Broadcast()
	defer c.mutex.Unlock()
	return nil
}

func FileExists(file string) bool {
	_, err := os.Stat(file)
	return err == nil
}

func (c *Coordinator) DispatchMapTask(reply *TaskReply) {
	reply.Type = Map
	reply.MapTask = c.mapTasks[len(c.mapTasks)-1]
	c.mapTasks = c.mapTasks[:len(c.mapTasks)-1]
	go c.WaitForMapSuccess(reply.MapTask)
}

func (c *Coordinator) DispatchReduceTask(reply *TaskReply) {
	//c.once1.Do(func() { log.Printf("Starting Reduce...\n") })
	reply.Type = Reduce
	reply.ReduceTaskId = c.reduceTaskIds[len(c.reduceTaskIds)-1]
	c.reduceTaskIds = c.reduceTaskIds[:len(c.reduceTaskIds)-1]
	go c.WaitForReduceSuccess(reply.ReduceTaskId)
}

func (c *Coordinator) WaitForReduceSuccess(reduceTaskId int) {
	time.Sleep(time.Second * 10)
	failed := !FileExists(fmt.Sprintf("mr-out-%d", reduceTaskId))
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if failed {
		c.reduceTaskIds = append(c.reduceTaskIds, reduceTaskId)
		log.Printf("Reduce task ID %d added back to queue.\n", reduceTaskId)
	} else {
		c.ReduceCompleted[reduceTaskId] = true
	}
	c.cond.Broadcast()
}

func (c *Coordinator) WaitForMapSuccess(task MapTaskDef) {
	time.Sleep(time.Second * 10)
	failed := false
	for i := 0; i < c.nReduce; i++ {
		if !FileExists(fmt.Sprintf("mr-%d-%d", task.TaskId, i)) {
			failed = true
		}
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if failed {
		c.mapTasks = append(c.mapTasks, task)
		log.Printf("Map task ID %d added back to queue.\n", task.TaskId)
	} else {
		c.MapCompleted[task.TaskId] = task.FileName
	}
	c.cond.Broadcast()
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
	c.mutex.Lock()
	ret := c.IsReduceCompleted()
	c.mutex.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:        make([]MapTaskDef, len(files)),
		nFiles:          len(files),
		nReduce:         nReduce,
		MapCompleted:    make(map[int]string, len(files)),
		ReduceCompleted: make(map[int]bool, nReduce),
		reduceTaskIds:   make([]int, nReduce),
	}
	c.cond = sync.NewCond(&c.mutex)

	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = MapTaskDef{TaskId: i, FileName: files[i]}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTaskIds[i] = i
	}
	c.server()
	return &c
}
