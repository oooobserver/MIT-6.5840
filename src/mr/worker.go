package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"time"
)

var cwd string

func init() {
	var err error
	cwd, err = os.Getwd()
	if err != nil {
		log.Fatal("Error: ", err)
	}
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Task struct {
	ok           bool
	filename     string
	task_index   int
	worker_index int
	nReduce      int
	shutdown     bool
	cat          string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {

	for {
		t := CallGetTask()
		// fmt.Println(t)
		if t.shutdown {
			return
		}

		if !t.ok || t.filename == "shye" {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		if t.cat == "map" {
			map_worker(t, mapf)
		} else {
			reduce_worker(t, reducef)
		}

	}
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func reduce_worker(t Task, reducef func(string, []string) string) {
	index := fmt.Sprintf("%d", t.task_index)
	pattern := `^mr-\d+-` + index + `$`

	// Compile the regular expression
	r, err := regexp.Compile(pattern)
	if err != nil {
		log.Fatalf("Error compiling regex: %v", err)
	}

	files, err := os.ReadDir(cwd)
	if err != nil {
		log.Fatalf("Error reading directory: %v", err)
	}

	// Read all file
	intermediate := []KeyValue{}
	for _, file := range files {
		if r.MatchString(file.Name()) {
			file, err := os.Open(file.Name())
			if err != nil {
				log.Fatalf("cannot open %v", file.Name())
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}
	}

	sort.Sort(ByKey(intermediate))

	file, err := os.CreateTemp(".", "msy")
	if err != nil {
		log.Fatalf("Reduce: error creating file %v", err)
	}

	// If crash, just delete
	// filePath := cwd + "/" + file.Name()
	// defer os.Remove(filePath)

	write_output(intermediate, file, reducef)
	CallFinishTaskReduce(t.task_index, t.worker_index, file)
}

func write_output(intermediate []KeyValue, file *os.File, reducef func(string, []string) string) {

	res := []KeyValue{}

	// call Reduce on each distinct key in intermediate[],
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		res = append(res, KeyValue{intermediate[i].Key, output})
		i = j
	}

	sort.Sort(ByKey(intermediate))

	for _, kv := range res {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
}

func map_worker(t Task, mapf func(string, string) []KeyValue) {
	// Read the file content
	file, err := os.Open(t.filename)
	if err != nil {
		log.Fatalf("Map: cannot open %v", t.filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Map: cannot read %v", t.filename)
	}
	file.Close()

	files := []*os.File{}

	// Create the intermidate file
	for i := 0; i < 10; i++ {
		file, err := os.CreateTemp(".", "mr-")
		if err != nil {
			log.Fatalf("Map: error creating file %v", err)
		}
		files = append(files, file)
	}

	kva := mapf(t.filename, string(content))
	write_intermediate(kva, files, t.nReduce)
	CallFinishTaskMap(t.task_index, t.worker_index, files)
}

func write_intermediate(kva []KeyValue, files []*os.File, NReduce int) {
	encs := []*json.Encoder{}
	for _, file := range files {
		tmp_enc := json.NewEncoder(file)
		encs = append(encs, tmp_enc)
	}
	for _, kv := range kva {
		tmp := ihash(kv.Key) % NReduce
		err := encs[tmp].Encode(&kv)
		if err != nil {
			log.Fatalf("Error: (encoding) %v", err)
		}
	}
}

func CallGetTask() Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return Task{
			ok:           ok,
			filename:     reply.Filename,
			task_index:   reply.TaskIndex,
			worker_index: reply.WorkerIndex,
			nReduce:      reply.NReduce,
			shutdown:     reply.Shutdown,
			cat:          reply.Cat,
		}
	}

	fmt.Printf("call failed!\n")
	return Task{ok: ok}
}

func CallFinishTaskMap(task_index, worker_index int, files []*os.File) {
	args := FinishTaskArgs{TaskIndex: task_index, WorkerIndex: worker_index, Cat: "map"}
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}

	// Only reply is true to let is persistent
	if reply.Rename {
		for i, file := range files {
			tmp_file_name := fmt.Sprintf("mr-%d-%d", task_index, i)
			newpath := cwd + "/" + tmp_file_name
			oldpath := cwd + "/" + file.Name()
			os.Rename(oldpath, newpath)
			file.Close()
		}
	} else {
		for _, file := range files {
			filePath := cwd + "/" + file.Name()
			err := os.Remove(filePath)
			if err != nil {
				log.Fatalf("Error: %v", err)
			}
		}
	}
}

func CallFinishTaskReduce(task_index, worker_index int, file *os.File) {
	args := FinishTaskArgs{TaskIndex: task_index, WorkerIndex: worker_index, Cat: "reduce"}
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}

	if reply.Rename {
		// Rename the file
		tmp_file_name := fmt.Sprintf("mr-out-%d", task_index)
		newpath := cwd + "/" + tmp_file_name
		oldpath := cwd + "/" + file.Name()
		os.Rename(oldpath, newpath)
		file.Close()
	} else {
		filePath := cwd + "/" + file.Name()
		err := os.Remove(filePath)
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
	}
}

// Send an RPC request to the coordinator, wait for the response.
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
