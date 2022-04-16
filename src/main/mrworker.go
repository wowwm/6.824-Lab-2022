package main

//
// 启动一个已经实现的 worker 进程
// 路径 ../mr/worker.go. typically there will be
// 多个 worker 进程与一个 Coordinator 通信
// 启动命令如下：
// go run mrworker.go wc.so
//
// 不要修改此文件
//

import "6.824/mr"
import "plugin"
import "os"
import "fmt"
import "log"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	// 加载插件
	mapf, reducef := loadPlugin(os.Args[1])
	// 启动 worker
	mr.Worker(mapf, reducef)
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
// 传入 .so文件名，返回两个函数
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	// 打开 .so 文件
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	// 找 Map 函数
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue) // 从指针中取出函数
	// 找 reduce 函数
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string) // 从指针中取出函数

	return mapf, reducef
}
