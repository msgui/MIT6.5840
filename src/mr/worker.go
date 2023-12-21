package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
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

	go mapExecute()
	go reduceExecute()

	for Done() == false {
		fmt.Printf("=== 发送成功: %d, 发送失败：%d, 任务总数 %d\n", done, sum-done, sum)
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
	fmt.Println("================ Done ================")
	fmt.Printf("=== 发送成功: %d, 发送失败：%d, 任务总数 %d\n", done, sum-done, sum)
	defer func() {
		ofile, _ := os.OpenFile("failTasks", os.O_WRONLY|os.O_CREATE|os.O_APPEND,
			0644)
		fmt.Fprintf(ofile, "==== 开始打印发送失败的KV, len(fmap): %d \n")
		fmap.Range(func(key, value any) bool {
			fmt.Printf("%s %s \n", key, value)
			_, err := fmt.Fprintf(ofile, "%v %v \n", key, value)
			return err == nil
		})
		ofile.Close()
	}()
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
func Done() bool {
	ags := Args{}
	res := Result{}
	call("Coordinator.DoneRPC", &ags, &res)
	var done bool
	json.Unmarshal([]byte(res.Json), &done)
	return done
}

var done int
var sum int
var dlock sync.Mutex = sync.Mutex{}
var fmap sync.Map = sync.Map{}

func mapExecute() {
	// send the RPC request, wait for the reply.
	res := Result{}
	ags := Args{}
	pid := os.Getpid()
	for {
		ags.Json = parse2Json(pid)
		call("Coordinator.MapFetch", &ags, &res)
		if len(res.Json) == 0 {
			continue
		}
		var fileTask FileTask
		parse2Obj(res.Json, &fileTask)
		fileTask.Pid = pid
		file, err := os.Open(fileTask.FileName)
		if err != nil {
			fmt.Errorf("FileName open error: %s", err)
			break
		}
		content, _ := ioutil.ReadAll(file)
		kva := mapFunc(fileTask.FileName, string(content))
		sort.Sort(KeyValues(kva))
		//fmt.Printf("%s \n", kva)
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
			reduceTask := ReduceTask{
				I:         -1,
				Pid:       pid,
				Key:       kv.Key,
				Values:    values,
				StartTime: -1,
			}
			ags.Json = parse2Json(reduceTask)
			sum++
			go func(a Args, reduceTask ReduceTask) {
				r := Result{}
				parse2Obj(a.Json, &r)
				for {
					call("Coordinator.MapPut", &a, &r)
					var f bool
					parse2Obj(r.Json, &f)
					if f {
						dlock.Lock()
						done++
						dlock.Unlock()
						//fmap.Delete(reduceTask.Key)
						return
					}
					time.Sleep(time.Millisecond)
					//v, _ := fmap.Load(reduceTask.Key)
					//if v == nil {
					//	fmap.Store(reduceTask.Key, len(reduceTask.Values))
					//}
				}
			}(ags, reduceTask)
			i = j
			time.Sleep(10)
		}
		ags.Json = parse2Json(fileTask)
		call("Coordinator.FinishFileTask", &ags, &res)
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
	oname := "workerOut/mr-out-" + strconv.Itoa(pid)
	for {
		ags.Json = parse2Json(pid)
		call("Coordinator.ReduceFetch", &ags, &res)
		if len(res.Json) == 0 {
			fmt.Errorf("Coordinator.ReduceFetch后 res: %s", res)
			continue
		}
		var reduceTask ReduceTask
		parse2Obj(res.Json, &reduceTask)
		output := reduceFunc(reduceTask.Key, reduceTask.Values)
		ags.Json = res.Json
		call("Coordinator.ReduceEsc", &ags, &res)
		var f bool
		parse2Obj(res.Json, &f)
		if f {
			ofile, _ := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			fmt.Fprintf(ofile, "%v %v\n", reduceTask.Key, output)
		} else {
			fmt.Errorf("Pid:%s 当前worker")
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
