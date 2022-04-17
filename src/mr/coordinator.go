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
// 服务器对外暴露的方法需要满足几个条件，详细看 RPC 笔记
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// 开启一个线程监听 RPCs from worker.go
// 绑定 Coordinator 的方法
func (c *Coordinator) server() {
	rpc.Register(c)  // 将服务对象进行注册
	rpc.HandleHTTP() // 提供的服务注册到HTTP协议上，方便调用者可以利用http的方式进行数据传递
	//l, e := net.Listen("tcp", ":1234")	// 在特定的 tcp 端口进行监听
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname) // 在特定的 unix 端口进行监听
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // 监听等待请求
}

// Done
// main/mrcoordinator.go 定期调用 Done() 判断任务是否结束
// 如果全部的工作完成后应该返回 true
// 绑定 Coordinator 的方法
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeCoordinator
// 创建一个 Coordinator.
// main/mrcoordinator.go 调用这个函数.
// files 为所有待 Map 文件名的切片
// nReduce 是 reduce 任务数
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
