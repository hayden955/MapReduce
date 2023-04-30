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

type Coordinator struct {
	// Your definitions here.
	NumReduce      int             // Number of reduce tasks
	Files          []string        // Files for map tasks, len(Files) is number of Map tasks
	MapTasks       chan MapTask    // Channel for uncompleted map tasks
	CompletedTasks map[string]bool // Map to check if task is completed
	Lock           sync.Mutex      // Lock for contolling shared variables
	MapTaskId      int
	BucketNum      chan Buckets
	ReduceTask     chan ReduceTasks
	Files2Reduce   []string
	InterFiles     [][]string
	ReduceDone     bool
	taskpending    int
	maptaskpending int
}

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

// Start Coordinator Logic
func (c *Coordinator) Start() {
	//fmt.Println("Starting Coordinator, adding Map Tasks to channel")

	filenumber := 0
	// Prepare initial MapTasks and add them to the queue
	for _, file := range c.Files {
		mapTask := MapTask{
			Filename:         file,
			NumReducer:       c.NumReduce,
			MapNumAssignment: filenumber,
		}
		//fmt.Print(mapTask.Filename)

		fmt.Println("MapTask", mapTask, "added to channel")
		c.MapTasks <- mapTask
		c.CompletedTasks["map_"+mapTask.Filename] = false

		filenumber++

	}
	c.maptaskpending = len(c.Files)
	c.taskpending = c.NumReduce

	for i := 0; i < c.NumReduce; i++ {
		Bucket := Buckets{
			Bucket: i,
		}
		c.BucketNum <- Bucket
	}

	// adds buckets to channel
	// each worker requests a bucketnumber
	// each worker requests a task with that bucketnumber
	// if nil, then the worker requests a new bucketnumber

	c.server()
}

// intermediate files handler
func (c *Coordinator) IntermediateFileHandler(args *InterFiles, reply *ReduceTasks) error {
	filename := args.Filename
	bucketnums := args.ReduceBucketNum

	reduceTask := ReduceTasks{
		Filename:        filename,
		ReduceBucketNum: bucketnums,
	}

	c.ReduceTask <- reduceTask

	return nil
}

func (c *Coordinator) RequestBA(args *EmptyArs, reply *Buckets) error {
	assigned, _ := <-c.BucketNum
	*reply = assigned

	return nil
}

// RPC that worker calls when idle (worker requests a map task)
func (c *Coordinator) RequestMapTask(a *EmptyArs, reply *MapTask) error {
	//fmt.Println("Map task requested")

	task, _ := <-c.MapTasks // check if there are uncompleted map tasks. Keep in mind, if MapTasks is empty, this will halt

	//fmt.Println("Map task requested and found,", task.Filename)
	*reply = task

	go c.WaitingWorker(task)

	return nil
}

func (c *Coordinator) WaitingWorker(task MapTask) {
	time.Sleep(time.Second * 5)
	c.Lock.Lock()
	if c.CompletedTasks["map_"+task.Filename] == false {
		//fmt.Println("Timer expired, task", task.Filename, "is not finished. Putting back in queue.")
		c.MapTasks <- task
	}
	c.Lock.Unlock()
}

func (c *Coordinator) MTaskCompleted(args *MapTask, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedTasks["map_"+args.Filename] = true
	fmt.Print("map_"+args.Filename, " Completed")

	if len(c.MapTasks) == 0 {
		time.Sleep(time.Second * 2)

		reply.MapCompleted = true
	}

	return nil
}

// RPC for reporting a completion of a task
func (c *Coordinator) RTaskCompleted(args *AllBuckets, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if len(c.BucketNum) == 0 {
		time.Sleep(time.Second * 2)
		fmt.Print("reduce complete")
		reply.ReduceCompleted = true
		c.ReduceDone = true

	}
	c.Done()

	//fmt.Print("Reduce Completed = False?")

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

	if c.ReduceDone {
		time.Sleep(1 * time.Second)
		ret = true
	}

	//Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		NumReduce:      nReduce,
		Files:          files,
		Files2Reduce:   make([]string, 0),
		MapTaskId:      0,
		MapTasks:       make(chan MapTask, 100),
		ReduceTask:     make(chan ReduceTasks, 100),
		CompletedTasks: make(map[string]bool),
		BucketNum:      make(chan Buckets, nReduce),
		ReduceDone:     false,
	}

	// Your code here.

	//fmt.Println("Starting coordinator")
	//fmt.Println(files)

	c.Start()
	return &c
}
