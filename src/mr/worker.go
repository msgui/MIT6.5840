package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KeyValues []KeyValue

func (kva KeyValues) Len() int {
	return len(kva)
}
func (kva KeyValues) Less(i int, j int) bool {
	return kva[i].Key < kva[j].Key
}

func (kva KeyValues) Swap(i int, j int) {
	kva[i], kva[j] = kva[j], kva[i]
}

var mapFunc func(string, string) []KeyValue
var reduceFunc func(string, []string) string

// use ihash(Key) % NReduce to choose the reduce
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
	mapFunc = mapf
	reduceFunc = reducef
	nReduce = getNReduce()
	mapExecute()
	reduceExecute()
	fmt.Println("================ Done ================")
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func Done() bool {
	ags := Args{}
	res := Result{}
	call("Coordinator.DoneRPC", &ags, &res)
	var done bool
	json.Unmarshal([]byte(res.Json), &done)
	return done
}

var nReduce int

func getNReduce() int {
	ags := Args{}
	res := Result{}
	call("Coordinator.GetNReduce", &ags, &res)
	var n int
	parse2Obj(res.Json, &n)
	return n
}

func getReduceTasks() []FileTask {
	args := Args{}
	res := Result{}
	for {
		call("Coordinator.GetReduces", &args, &res)
		if len(res.Json) > 0 {
			var reduceTasks []FileTask
			parse2Obj(res.Json, &reduceTasks)
			return reduceTasks
		}
	}
}

func mapDone() bool {
	res := Result{}
	ags := Args{}
	call("Coordinator.MapDone", &ags, &res)
	var f bool
	parse2Obj(res.Json, &f)
	return f
}

func reduceDone() bool {
	res := Result{}
	ags := Args{}
	call("Coordinator.ReduceDone", &ags, &res)
	var f bool
	parse2Obj(res.Json, &f)
	return f
}

func mapExecute() {
	// send the RPC request, wait for the reply.
	res := Result{}
	ags := Args{}
	pid := os.Getpid()
	for !mapDone() {
		ags.Json = parse2Json(pid)
		call("Coordinator.MapFetch", &ags, &res)
		if len(res.Json) == 0 {
			continue
		}
		var fileTask FileTask
		parse2Obj(res.Json, &fileTask)
		fileTask.Pid = pid
		file, _ := os.Open(fileTask.FileName)
		content, _ := ioutil.ReadAll(file)
		kva := mapFunc(fileTask.FileName, string(content))
		ags.Json = parse2Json(fileTask)
		var f bool
		call("Coordinator.FinishMap", &ags, &res)
		parse2Obj(res.Json, &f)
		if f {
			reduceTasks := getReduceTasks()
			encoders := make([]*json.Encoder, len(reduceTasks))
			files := make([]*os.File, len(reduceTasks))
			for k, reduceTask := range reduceTasks {
				outFile, _ := os.OpenFile(reduceTask.FileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
				files[k] = outFile
				encoders[k] = json.NewEncoder(outFile)
			}
			for _, kv := range kva {
				index := ihash(kv.Key) % nReduce
				encoders[index].Encode(&kv)
			}
			for _, file := range files {
				file.Close()
			}
			call("Coordinator.FinishFileTask", &ags, &res)
		}
		res.Json = ""
		ags.Json = ""
		file.Close()
	}
}

func reduceExecute() {
	// send the RPC request, wait for the reply.
	res := Result{}
	ags := Args{}
	pid := os.Getpid()
	for !reduceDone() {
		ags.Json = parse2Json(pid)
		call("Coordinator.ReduceFetch", &ags, &res)
		if len(res.Json) == 0 {
			continue
		}
		var reduceTask FileTask
		parse2Obj(res.Json, &reduceTask)
		fileName := reduceTask.FileName
		inFile, _ := os.OpenFile(fileName, os.O_RDONLY, 0644)
		kva := make([]KeyValue, 0)
		dec := json.NewDecoder(inFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		inFile.Close()
		sort.Sort(KeyValues(kva))
		outputs := make([]KeyValue, 0)
		i := 0
		for i < len(kva) {
			kv := &kva[i]
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			outputs = append(outputs, KeyValue{
				Key:   kv.Key,
				Value: reduceFunc(kv.Key, values),
			})
			i = j
		}
		ags.Json = res.Json
		call("Coordinator.FinishReduce", &ags, &res)
		var f bool
		parse2Obj(res.Json, &f)
		if f {
			oname := "mr-out-" + strconv.Itoa(reduceTask.I+1)
			ofile, _ := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			for _, output := range outputs {
				fmt.Fprintf(ofile, "%v %v\n", output.Key, output.Value)
			}
			ofile.Close()
			call("Coordinator.ReduceEsc", &ags, &res)
		}
		res.Json = ""
		ags.Json = ""
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
