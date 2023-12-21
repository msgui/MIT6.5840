package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/json"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Result struct {
	Json string
}

type Args struct {
	Json string
}
type ReduceTask struct {
	I         int
	Pid       int
	Key       string
	Values    []string
	StartTime int64 // -2 代表 任务已完成，可以替换， -1代表任务未开始，等待被执行。>0代表 开始的时间辍
}

type FileTask struct {
	I         int
	Pid       int
	FileName  string
	StartTime int64
}

func parse2Json(p interface{}) string {
	bytes, _ := json.Marshal(p)
	return string(bytes)
}

func parse2Obj(s string, p interface{}) {
	json.Unmarshal([]byte(s), p)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
