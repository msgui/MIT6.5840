package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.5840/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	count := &m.Count
	fmt.Printf("=== 待处理任务数：%d, 已处理任务数：%d, 总任务数：%d \n", count.Inc-count.Dec, count.Dec, count.Inc)
	fmt.Println("================ Done ================")

	time.Sleep(time.Second)
}
