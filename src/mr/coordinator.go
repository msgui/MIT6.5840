package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	Sum         int
	mapDone     bool
	reduceDone  bool
	Count       Count
	Reduces     []FileTask
	FileTasks   []FileTask
	FileLocks   []sync.Mutex
	ReduceLocks []sync.Mutex
	Cinc        sync.Mutex
	Cdec        sync.Mutex
	Slock       sync.Mutex
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

func (c *Coordinator) GetReduces(ags *Args, res *Result) error {
	var i int
	parse2Obj(ags.Json, &i)
	res.Json = parse2Json(c.Reduces)
	return nil
}

func (c *Coordinator) ReduceUnLock(ags *Args, res *Result) error {
	var i int
	parse2Obj(ags.Json, &i)
	lock := &c.ReduceLocks[i]
	lock.Unlock()
	return nil
}

func (c *Coordinator) GetNReduce(ags *Args, res *Result) error {
	res.Json = parse2Json(len(c.Reduces))
	return nil
}

func (c *Coordinator) ReduceEsc(ags *Args, res *Result) error {
	var reduceTask ReduceTask
	parse2Obj(ags.Json, &reduceTask)
	i := reduceTask.I
	lock := &c.ReduceLocks[i]
	rt := &c.Reduces[i]
	lock.Lock()
	b := rt.Pid == reduceTask.Pid
	if b {
		rt.StartTime = -3
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
				res.Json = parse2Json(fileTask)
				lock.Unlock()
				break
			}
			lock.Unlock()
		}
	}
	return nil
}

func (c *Coordinator) MapDone(ags *Args, res *Result) error {
	res.Json = parse2Json(c.mapDone)
	return nil
}

func (c *Coordinator) ReduceDone(ags *Args, res *Result) error {
	res.Json = parse2Json(c.reduceDone)
	return nil
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
		res.Json = parse2Json(false)
		return nil
	}
	fileTask.StartTime = -3
	res.Json = parse2Json(true)
	return nil
}

func (c *Coordinator) FinishMap(ags *Args, res *Result) error {
	var task FileTask
	parse2Obj(ags.Json, &task)
	fileIndex := task.I
	fileTask := &c.FileTasks[fileIndex]
	flock := &c.FileLocks[fileIndex]
	flock.Lock()
	defer flock.Unlock()
	if task.Pid != fileTask.Pid {
		res.Json = parse2Json(false)
		return nil
	}
	fileTask.StartTime = -2
	res.Json = parse2Json(true)
	return nil
}

func (c *Coordinator) FinishReduce(ags *Args, res *Result) error {
	var task FileTask
	parse2Obj(ags.Json, &task)
	fileIndex := task.I
	fileTask := &c.Reduces[fileIndex]
	flock := &c.ReduceLocks[fileIndex]
	flock.Lock()
	defer flock.Unlock()
	if task.Pid != fileTask.Pid {
		res.Json = parse2Json(false)
		return nil
	}
	fileTask.StartTime = -2
	res.Json = parse2Json(true)
	return nil
}

/*
Timer
*/
func (c *Coordinator) ReduceTimer() {
	for { //死循环，当master的主线程退出时，協程会自动退出
		for i := range c.Reduces {
			reduceTask := &c.Reduces[i]
			lock := &c.ReduceLocks[i]
			if timeOut(reduceTask.StartTime) {
				go func(reduceTask *FileTask, lock *sync.Mutex) {
					lock.Lock()
					if timeOut(reduceTask.StartTime) {
						reduceTask.Pid = -1
						reduceTask.StartTime = -1
					}
					lock.Unlock()
				}(reduceTask, lock)
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
				go func(fileTask *FileTask) {
					lock.Lock()
					if fileTask.StartTime != -2 {
						fileTask.Pid = -1
						fileTask.StartTime = -1
					}
					lock.Unlock()
				}(fileTask)
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
	fileTasks := &c.FileTasks
	for _, fileTask := range *fileTasks {
		if fileTask.StartTime != -3 {
			return false
		}
	}
	c.mapDone = true
	reduces := &c.Reduces
	for _, reduceTask := range *reduces {
		if reduceTask.StartTime != -3 {
			return false
		}
	}
	c.reduceDone = true
	return true
}

func (c *Coordinator) DoneRPC(ags *Args, res *Result) error {
	res.Json = parse2Json(c.reduceDone && c.mapDone)
	return nil
}

func (c *Coordinator) PrintTimer() bool {
	for {
		count := &c.Count
		fmt.Printf("待处理任务数：%d, 已处理任务数：%d, 总任务数：%d , Sum: %d \n",
			count.Inc-count.Dec, count.Dec, count.Inc, c.Sum)
		time.Sleep(time.Second)
	}
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

	reduceTasks := make([]FileTask, nReduce)
	for i := range reduceTasks {
		reduceTasks[i] = FileTask{
			I:         i,
			Pid:       -1,
			FileName:  "reduce-" + strconv.Itoa(i) + ".txt",
			StartTime: -1,
		}
	}

	reduceLocks := make([]sync.Mutex, nReduce)
	fileLocks := make([]sync.Mutex, len(fileTasks))
	m := Coordinator{
		0,
		false,
		false,
		Count{
			Inc:     0,
			Dec:     0,
			Timemap: 0},
		reduceTasks, fileTasks,
		fileLocks, reduceLocks,
		sync.Mutex{}, sync.Mutex{}, sync.Mutex{}}
	m.server()
	go m.MapTimer()
	go m.ReduceTimer()
	return &m
}
