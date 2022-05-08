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
	//files      []string // 未完成的待 Map 文件名切片
	mapJobs    []MapJob
	reduceJobs []ReduceJob
	nMap       int
	nReduce    int // Reducer 数量
	mapChan    chan MapJob
	reduceChan chan ReduceJob
	finishFlag chan bool
	Fin        int
	lock       locker
}

type locker struct {
	mapl    *sync.Mutex
	reducel *sync.Mutex
	workerl *sync.Mutex
}

// 制作 MapJobs
func (c *Coordinator) makeMapJobs(files []string) {
	c.mapChan = make(chan MapJob, c.nMap)
	for num, filename := range files {
		mapJob := MapJob{
			filename,
			num,
		}
		c.mapChan <- mapJob
		c.mapJobs = append(c.mapJobs, mapJob)
	}
}

// 制作 ReduceJobs
func (c *Coordinator) makeReduceJobs() {
	//var rdJobs []ReduceJob
	c.reduceChan = make(chan ReduceJob, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		var rdNmaes ReduceJob // 一个 Reduce Job
		var rdFiles []string  // 每一个 Reduce 任务的文件名切片
		for j := 0; j < c.nMap; j++ {
			filename := "mr-" + strconv.Itoa(j) + strconv.Itoa(i)
			rdFiles = append(rdFiles, filename)
		}
		rdNmaes.ReduceID = i
		rdNmaes.ReduceName = rdFiles
		c.reduceChan <- rdNmaes
		c.reduceJobs = append(c.reduceJobs, rdNmaes)
	}
}

// Your code here -- RPC handlers for the worker to call.

// WorkerArgsReply RPC 暴露方法，获取所有 Worker 参数
func (c *Coordinator) WorkerArgsReply(Fin int, workerArgs *WorkerReply) error {
	//fmt.Println("call reply ==================")
	c.lock.mapl.Lock()
	c.lock.reducel.Lock()
	Fin = c.Fin
	c.lock.reducel.Unlock()
	c.lock.mapl.Unlock()
	workerArgs.Fin = Fin
	if Fin == 0 { // Map
		workerArgs.MapJob = <-c.mapChan
		workerArgs.NReduce = c.nReduce
		//fmt.Println("gei Map ----------", workerArgs.MapJob.MapID)
		go func() {
			//后台跑一个监控超时，超时就重新加入 channel
			time.Sleep(time.Second * 10)
			c.lock.mapl.Lock()
			jobs := c.mapJobs[:] // 拷贝切片
			c.lock.mapl.Unlock()
			for _, job := range jobs {
				if workerArgs.MapJob.MapID == job.MapID {
					c.mapChan <- job
					//fmt.Println("MapDown ", job.MapID)
				}
			}
		}()
		//time.Sleep(time.Second * 10)
	} else if Fin == 1 { // Reduce
		workerArgs.ReduceJob = <-c.reduceChan
		//fmt.Println("gei Reduce ----------", workerArgs.ReduceJob.ReduceID)
		go func() {
			//后台跑一个监控超时，超时就重新加入 channel
			time.Sleep(time.Second * 10)
			c.lock.reducel.Lock()
			jobs := c.reduceJobs[:] // 拷贝切片
			c.lock.reducel.Unlock()
			for _, job := range jobs {
				if workerArgs.ReduceJob.ReduceID == job.ReduceID {
					c.reduceChan <- job
					//fmt.Println("ReduceDown ", job.ReduceID)
				}
			}
		}()
	} else {
		//time.Sleep(time.Second * 10)
		//c.Fin++
		//c.finishFlag <- true

		// 让 Worker 正常退出后再退出 Coordinator
		//workerArgs.Fin = 2
		//Fin++
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
	// Map 完成
	if len(jobs) == 0 && len(c.mapChan) == 0 {
		//fmt.Println(" map fininsh -----")
		c.Fin = 1
	}
	//fmt.Println("-- del mapjob: ", mapjob.MapID)
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
	// Reduce 完成
	if len(jobs) == 0 && len(c.reduceChan) == 0 {
		//fmt.Println("reduce fininsh -----")
		c.Fin = 2
	}
	//fmt.Println("-- after del reduceJobs: ", c.reduceJobs)
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
	//fmt.Println("done!!!!!")

	if <-c.finishFlag {
		fmt.Println("-------- exit coordinator --------")
		return true
	}
	//fmt.Println(len(c.finishFlag))
	//c.finishFlag <- false
	//fmt.Println(len(c.finishFlag))
	return false
}

// MakeCoordinator  创建一个 Coordinator.
// main/mrcoordinator.go 调用这个函数.
// files 为所有待 Map 文件名的切片，nReduce 是 reduce 任务数
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.Fin = 0
	// 将所有任务做成任务类，放入 channel
	c.makeMapJobs(files)
	c.makeReduceJobs()

	c.finishFlag = make(chan bool)
	//c.finishFlag <- false

	// 初始化全局锁
	c.lock.mapl = new(sync.Mutex)
	c.lock.reducel = new(sync.Mutex)
	c.lock.workerl = new(sync.Mutex)

	c.server()

	// 监控结束
	//go func() {
	//	for c.Fin < 2 {
	//		time.Sleep(time.Second)
	//	}
	//	c.finishFlag <- true
	//}()

	return &c
}
