package mr

//
// RPC definitions.
//
// 记住所有名称需要首字母大写。
//

import "os"
import "strconv"

//
// 声明参数和应答RPC的例子
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type MapJob struct {
	MapName string
	MapID   int
}

type ReduceJob struct {
	ReduceName []string
	ReduceID   int
}

type WorkerReply struct {
	MapJobs    []MapJob
	ReduceJobs []ReduceJob
	//Mapchan    chan MapJob
	//ReduceChan chan ReduceJob
	//UncommitFiles []string // 未提交的待 Map 文件名切片
	NMap       int // Mapper 数量
	NReduce    int // Reducer 数量
	FinishFlag int // 1为完成了Map，2为完成了Reduce
}

// 制作一个唯一的 UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
