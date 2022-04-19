package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files      []string // 未完成的待 Map 文件名切片
	mapJobs    []MapJob
	reduceJobs []ReduceJob
	nMap       int
	nReduce    int // Reducer 数量
	finishFlag chan bool
	lock       locker
}

type locker struct {
	mapl    *sync.Mutex
	reducel *sync.Mutex
}

// 制作 MapJobs
func (c *Coordinator) makeMapJobs(files []string) []MapJob {
	var mapJobs []MapJob
	for num, filename := range files {
		mapJob := MapJob{
			filename,
			num,
		}
		mapJobs = append(mapJobs, mapJob)
	}
	return mapJobs
}

// 制作 ReduceJobs
func (c *Coordinator) makeReduceJobs() []ReduceJob {
	var rdJobs []ReduceJob
	for i := 0; i < c.nReduce; i++ {
		var rdNmaes ReduceJob // 一个 Reduce Job
		var rdFiles []string  // 每一个 Reduce 任务的文件名切片
		for j := 0; j < c.nMap; j++ {
			filename := "mr-" + strconv.Itoa(j) + strconv.Itoa(i)
			rdFiles = append(rdFiles, filename)
		}
		rdNmaes.ReduceID = i
		rdNmaes.ReduceName = rdFiles
		rdJobs = append(rdJobs, rdNmaes)
	}
	//fmt.Println("rdjobs: ", rdJobs)
	return rdJobs
}

// Your code here -- RPC handlers for the worker to call.

// WorkerArgsReply RPC 暴露方法，获取所有 Worker 参数
func (c *Coordinator) WorkerArgsReply(fflag int, workerArgs *WorkerReply) error {
	workerArgs.FinishFlag = fflag
	//fmt.Println("111 workerArgs get: ", workerArgs)
	if workerArgs.FinishFlag == 0 { // Map 未完成
		workerArgs.NMap = len(c.mapJobs)
		workerArgs.NReduce = c.nReduce
		workerArgs.MapJobs = c.mapJobs
		//fmt.Println("workerArgs: ", workerArgs)

	} else if workerArgs.FinishFlag == 1 { // Map 完成，Reduce 未完成
		//fmt.Println("Map 结束！！！！！")
		workerArgs.ReduceJobs = c.reduceJobs
	} else { // MapReduce 完成
		fmt.Println("--------- all down 退出 coordinator ---------")
		c.finishFlag <- true
	}

	return nil
}

// MapJobOK 处理 mapjob 完成后的通知
func (c *Coordinator) MapJobOK(mapjob MapJob, reply *string) error {
	var jobs []MapJob
	c.lock.mapl.Lock()
	defer c.lock.mapl.Unlock()
	// 在 c.mapJobs 去除已经完成的 job
	for _, job := range c.mapJobs {
		if job.MapID != mapjob.MapID {
			jobs = append(jobs, job)
		}
	}
	c.mapJobs = jobs
	fmt.Println("-- del mapjob: ", mapjob.MapID)
	return nil
}

// ReduceJobOK 处理 ReduceJob 完成后的通知
func (c *Coordinator) ReduceJobOK(reducejob ReduceJob, reply *string) error {
	c.lock.reducel.Lock()
	defer c.lock.reducel.Unlock()
	var jobs []ReduceJob
	// 在 c.reduceJobs 去除已经完成的 job
	for _, job := range c.reduceJobs {
		if job.ReduceID != reducejob.ReduceID {
			jobs = append(jobs, job)
		}
	}
	c.reduceJobs = jobs
	//fmt.Println("-- after del reduceJobs: ", c.reduceJobs)
	return nil
}

// Example
// 一个 RPC 处理器的示例
// the RPC 参数 and reply 在 rpc.go 中定义.
// 服务器对外暴露的方法需要满足几个条件，详细看 RPC 笔记
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

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
	if <-c.finishFlag {
		return true
	}
	c.finishFlag <- false
	return false
}

// MakeCoordinator
// 创建一个 Coordinator.
// main/mrcoordinator.go 调用这个函数.
// files 为所有待 Map 文件名的切片
// nReduce 是 reduce 任务数
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapJobs = c.makeMapJobs(files) // 转为 MapJobs 类型
	c.reduceJobs = c.makeReduceJobs()
	c.finishFlag = make(chan bool, 2)
	c.finishFlag <- false
	c.lock.mapl = new(sync.Mutex)
	c.lock.reducel = new(sync.Mutex)
	//// 初始化 channel
	//c.workerArgs.Mapchan = make(chan MapJob, c.nMap)
	//c.workerArgs.ReduceChan = make(chan ReduceJob, c.nReduce)

	c.server()
	return &c
}
