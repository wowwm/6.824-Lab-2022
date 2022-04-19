package main

//
// 启动已经实现的Coordinator进程
// 路径 ../mr/coordinator.go
// 启动命令如下：
// go run mrcoordinator.go pg*.txt
//
// 此文件不要修改
//

import "6.824/mr"
import "time"
import "os"
import "fmt"

func main() {
	// 检查命令行参数
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	// 启用 Coordinator，传入命令行参数，第一个为所有待Map文件名的切片，nReduce 是 Reduce 任务数量
	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
}
