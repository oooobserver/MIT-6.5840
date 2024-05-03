package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	files         []string
	nReduce       int
	index         int // The worker index
	map_status    []state
	map_finish    int
	reduce_status []state
	reduce_finish int
	mu            sync.Mutex
}

type state struct {
	state      string // Ready, Processing, Finish
	start_time time.Time
	index      int // The worker index
}

// (Worker, task, file)
func (c *Coordinator) assign_map_task(i int) {
	c.map_status[i].state = "Processing"
	c.map_status[i].start_time = time.Now()
	c.map_status[i].index = c.index
	c.index++
}

func (c *Coordinator) assign_reduce_task(i int) {
	c.reduce_status[i].state = "Processing"
	c.reduce_status[i].start_time = time.Now()
	c.reduce_status[i].index = c.index
	c.index++
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.reduce_finish == 0 {
		reply.Shutdown = true
		return nil
	}

	if c.map_finish == 0 {
		reply.Cat = "reduce"
		c.GetReduceTask(reply)
		return nil
	}

	reply.Cat = "map"
	c.GetMapTask(reply)
	return nil
}

func (c *Coordinator) GetReduceTask(reply *GetTaskReply) {
	c.mu.Lock()
	var reduce_task int
	var worker_index int
	for i, s := range c.reduce_status {
		if s.state == "Ready" {
			reduce_task = i
			worker_index = c.index

			c.assign_reduce_task(i)
			break
		} else if c.reduce_status[i].state == "Processing" { // Over 10 second, retry
			if time.Since(c.reduce_status[i].start_time) > 10*time.Second {
				reduce_task = i
				worker_index = c.index

				c.assign_reduce_task(i)
				break
			}
		}
	}
	c.mu.Unlock()

	reply.TaskIndex = reduce_task
	reply.WorkerIndex = worker_index
	reply.Shutdown = false
}

func (c *Coordinator) GetMapTask(reply *GetTaskReply) {
	c.mu.Lock()
	var file string
	var task_index int
	var worker_index int
	for i, f := range c.files {
		if c.map_status[i].state == "Ready" {
			file = f
			task_index = i
			worker_index = c.index

			c.assign_map_task(i)
			break
		} else if c.map_status[i].state == "Processing" { // Over 10 second, retry
			if time.Since(c.map_status[i].start_time) > 10*time.Second {
				file = f
				task_index = i
				worker_index = c.index

				c.assign_map_task(i)
				break
			}
		}
	}
	c.mu.Unlock()
	reply.NReduce = c.nReduce

	if file == "" {
		reply.Filename = "shye"
	} else {
		reply.Filename = file
	}
	reply.TaskIndex = task_index
	reply.WorkerIndex = worker_index
	reply.Shutdown = false
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	if args.Cat == "map" {
		c.mu.Lock()
		if c.map_status[args.TaskIndex].state == "Processing" &&
			c.map_status[args.TaskIndex].index == args.WorkerIndex {
			reply.Rename = true
			c.map_status[args.TaskIndex].state = "Finish"
			c.map_finish -= 1
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
	} else {
		c.mu.Lock()
		if c.reduce_status[args.TaskIndex].state == "Processing" &&
			c.reduce_status[args.TaskIndex].index == args.WorkerIndex {
			reply.Rename = true
			c.reduce_status[args.TaskIndex].state = "Finish"
			c.reduce_finish -= 1
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
	}

	reply.Rename = false
	return nil
}

// Start a thread that listens for RPCs from worker.go
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

	if c.reduce_finish == 0 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.server()
	c.files = files
	c.nReduce = nReduce
	c.nReduce = nReduce
	c.map_status = []state{}
	c.map_finish = len(files)
	c.reduce_status = []state{}
	c.reduce_finish = nReduce
	for i := 0; i < len(files); i++ {
		c.map_status = append(c.map_status, state{"Ready", time.Unix(0, 0), 0})
	}

	for i := 0; i < nReduce; i++ {
		c.reduce_status = append(c.reduce_status, state{"Ready", time.Unix(0, 0), 0})
	}

	return &c
}
