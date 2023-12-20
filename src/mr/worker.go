package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
	for {
		reply := TaskReply{}
		args := ExampleArgs{}
		ok := call("Coordinator.Dispatch", &args, &reply)
		if !ok {
			reply.Type = Terminate
		}
		switch reply.Type {
		case Terminate:
			return
		case Map:
			ExecuteMap(reply.MapTask, mapf, reply.NumReduce)
			call("Coordinator.NotifyComplete", &reply, &reply)
		case Reduce:
			ExecuteReduce(reply.ReduceTaskId, reducef, reply.NumMap)
			call("Coordinator.NotifyComplete", &reply, &reply)
		}
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

	// send the RPC request, cond for the reply.
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

func ExecuteMap(task MapTaskDef, mapf func(string, string) []KeyValue, nReduce int) {
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))
	oFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		oFiles[i], err = ioutil.TempFile("", "temp")
		if err != nil {
			panic("Failed to create temp file.")
		}
		encoders[i] = json.NewEncoder(oFiles[i])
	}
	defer func() {
		for idx, fid := range oFiles {
			fid.Close()
			if err := os.Rename(fid.Name(), fmt.Sprintf("mr-%d-%d", task.TaskId, idx)); err != nil {
				panic("Failed to rename file")
			}
		}
	}()
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % nReduce
		encoders[idx].Encode(&kv)
	}
}

func ExecuteReduce(taskId int, reducef func(string, []string) string, nMapTask int) {
	intermediate := []KeyValue{}
	for i := 0; i < nMapTask; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, taskId)
		fd, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", iname)
		}
		dec := json.NewDecoder(fd)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		fd.Close()
	}

	sort.Sort(ByKey(intermediate))

	ofile, _ := ioutil.TempFile("", "temp")

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", taskId))
}

// send an RPC request to the coordinator, cond for the response.
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
