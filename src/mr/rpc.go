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

type WorkerReply struct {
	UncommitFiles []string // 未提交的待 Map 文件名切片
	NReduce       int      // Reducer 数量
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
