package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

// Example
// 一个 RPC 处理器的示例
// the RPC 参数 and reply 在 rpc.go 中定义.
// 绑定 Coordinator 的方法
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// 开启一个线程监听 RPCs from worker.go
// 绑定 Coordinator 的方法
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

// Done
// main/mrcoordinator.go 定期调用 Done() to find out
// 如果全部的工作完成后
// 绑定 Coordinator 的方法
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeCoordinator
// 创建一个 Coordinator.
// main/mrcoordinator.go 调用这个函数.
// nReduce 是 reduce 任务数
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
