package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	Count       Count
	Reduces     []ReduceTask
	FileTasks   []FileTask
	FileLocks   []sync.Mutex
	ReduceLocks []sync.Mutex
	Cinc        sync.Mutex
	Cdec        sync.Mutex
}

type Count struct {
	Inc     int
	Dec     int
	Timemap int64
}

// Your code here -- RPC handlers for the worker to call.
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) ReduceEsc(ags *Args, res *Result) error {
	var reduceTask ReduceTask
	parse2Obj(ags.Json, &reduceTask)
	i := reduceTask.I
	lock := &c.ReduceLocks[i]
	rt := &c.Reduces[i]
	lock.Lock()
	b := rt.Pid == reduceTask.Pid && rt.StartTime != -2
	if b {
		rt.StartTime = -2
		c.Dec() //Count--*
	}
	lock.Unlock()
	res.Json = parse2Json(b)
	return nil
}

func (c *Coordinator) ReduceFetch(ags *Args, res *Result) error {
	var pid int
	parse2Obj(ags.Json, &pid)
	for i := range c.Reduces {
		reduceTask := &c.Reduces[i]
		lock := &c.ReduceLocks[i]

		if reduceTask.StartTime == -1 {
			if !lock.TryLock() {
				continue
			}
			if reduceTask.StartTime == -1 {
				reduceTask.Pid = pid
				reduceTask.StartTime = time.Now().UnixNano()
				lock.Unlock()
				res.Json = parse2Json(reduceTask)
				break
			}
			lock.Unlock()
		}
	}
	return nil
}

func (c *Coordinator) MapPut(ags *Args, res *Result) error {
	var reduceTask ReduceTask
	err := json.Unmarshal([]byte(ags.Json), &reduceTask)
	index := ihash(reduceTask.Key) % len(c.Reduces)
	lock := &c.ReduceLocks[index]
	task := &c.Reduces[index]

	if task.StartTime != -2 {
		res.Json = parse2Json(false)
		return err
	}
	lock.Lock()
	defer lock.Unlock()
	if task.StartTime == -2 {
		task.I = index
		task.Pid = -1
		task.Key = reduceTask.Key
		task.Values = reduceTask.Values
		task.StartTime = -1
		c.Inc() //Count++
		res.Json = parse2Json(true)
	} else {
		res.Json = parse2Json(false)
	}
	return err
}

func (c *Coordinator) MapFetch(ags *Args, res *Result) error {
	var pid int
	parse2Obj(ags.Json, &pid)
	for i := range c.FileTasks {
		fileTask := &c.FileTasks[i]
		lock := &c.FileLocks[i]
		if fileTask.StartTime == -1 {
			if !lock.TryLock() {
				continue
			}
			if fileTask.StartTime == -1 {
				fileTask.I = i
				fileTask.Pid = pid
				fileTask.StartTime = time.Now().UnixNano()
				bytes, err := json.Marshal(fileTask)
				if err != nil {
					lock.Unlock()
					return err
				}
				res.Json = string(bytes)
				lock.Unlock()
				break
			}
			lock.Unlock()
		}
	}
	return nil
}

func (c *Coordinator) DoneRPC(ags *Args, res *Result) error {
	done := c.Done()
	bytes, err := json.Marshal(done)
	res.Json = string(bytes)
	return err
}

func (c *Coordinator) FinishFileTask(ags *Args, res *Result) error {
	var task FileTask
	parse2Obj(ags.Json, &task)
	fileIndex := task.I
	fileTask := &c.FileTasks[fileIndex]
	flock := &c.FileLocks[fileIndex]
	flock.Lock()
	defer flock.Unlock()
	if task.Pid != fileTask.Pid {
		return nil
	}
	fileTask.StartTime = -2
	return nil
}

/*
*
Timer
*/
func (c *Coordinator) ReduceTimer() {
	for { //死循环，当master的主线程退出时，協程会自动退出
		for i := range c.Reduces {
			reduceTask := &c.Reduces[i]
			lock := &c.ReduceLocks[i]
			if timeOut(reduceTask.StartTime) {
				go func() {
					lock.Lock()
					reduceTask.Pid = -1
					reduceTask.StartTime = -1
					lock.Unlock()
				}()
			}
		}
	}
}

func (c *Coordinator) MapTimer() {
	for { //死循环，当master的主线程退出时，協程会自动退出
		for i := range c.FileTasks {
			fileTask := &c.FileTasks[i]
			lock := &c.FileLocks[i]
			if timeOut(fileTask.StartTime) {
				go func() {
					lock.Lock()
					fileTask.Pid = -1
					fileTask.StartTime = -1
					lock.Unlock()
				}()
			}
		}
	}
}

func (c *Coordinator) Inc() {
	c.Cinc.Lock()
	c.Count.Inc++
	c.Count.Timemap = time.Now().UnixNano()
	c.Cinc.Unlock()
}

func (c *Coordinator) Dec() {
	c.Cdec.Lock()
	c.Count.Dec++
	c.Count.Timemap = time.Now().UnixNano()
	c.Cdec.Unlock()
}

func timeOut(startTime int64) bool {
	//return false
	return startTime > 0 && time.Now().UnixNano()-startTime >= 10*time.Second.Nanoseconds()
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//return false
	count := &c.Count
	timep := count.Timemap
	isDone := count.Inc == count.Dec
	fmt.Printf("待处理任务数：%d, 已处理任务数：%d, 总任务数：%d \n", count.Inc-count.Dec, count.Dec, count.Inc)

	for i := range c.FileTasks {
		fileTask := &c.FileTasks[i]
		isDone = isDone && fileTask.StartTime == -2
		if !isDone {
			return false
		}
	}
	time.Sleep(5 * time.Second)
	isDone = timep == count.Timemap
	return isDone
}

// create a Coordinator.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	fileTasks := make([]FileTask, len(files))
	for i, fileName := range files {
		fileTasks[i] = FileTask{
			I:         i,
			Pid:       -1,
			FileName:  fileName,
			StartTime: -1,
		}
	}

	reduceTasks := make([]ReduceTask, nReduce)
	for i := range reduceTasks {
		reduceTasks[i] = ReduceTask{
			I:         i,
			Pid:       -1,
			StartTime: -2,
		}
	}

	reduceLocks := make([]sync.Mutex, nReduce)
	fileLocks := make([]sync.Mutex, len(fileTasks))
	m := Coordinator{Count{
		Inc:     0,
		Dec:     0,
		Timemap: 0,
	},
		reduceTasks, fileTasks,
		fileLocks, reduceLocks,
		sync.Mutex{}, sync.Mutex{}}
	m.server()
	go m.ReduceTimer()
	go m.MapTimer()
	return &m
}
