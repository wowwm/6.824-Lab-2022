package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
// wc.go 中使用了这个 struct
type KeyValue struct {
	Key   string
	Value string
}

// ByKey 用于按 key 排序.
// worker.go 中的 KeyValue 的切片类型
type ByKey []KeyValue

// 此类型要使用 Sort 方法需要实现其接口，包含这几个函数

func (a ByKey) Len() int           { return len(a) }              // 获取长度
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }    // 交换 a[i], a[j] 值
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key } // 判断 key 大小

// use ihash(key) % NReduce to 用哈希选择一个 Reduce
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func MapWorker(mapf func(string, string) []KeyValue, mapjob MapJob, nReduce int,
	lock *sync.Mutex) {
	//defer wg.Done()
	//fmt.Println("----- start map -----")
	file, err := os.Open(mapjob.MapName)
	if err != nil {
		log.Fatalf("cannot open %v", mapjob.MapName)
	}
	content, err := ioutil.ReadAll(file) // 读取文件，后期尝试下用 ReadFile
	if err != nil {
		log.Fatalf("cannot read %v", mapjob.MapName)
	}
	file.Close()
	lock.Lock()
	kva := mapf(mapjob.MapName, string(content))
	lock.Unlock()
	sort.Sort(ByKey(kva)) // 排序
	// 输出到中间文件
	for i := 0; i < nReduce; i++ {
		iname := "mr-" + strconv.Itoa(mapjob.MapID) + strconv.Itoa(i)
		//fmt.Println(iname)
		tmpfile, _ := ioutil.TempFile("./", "tmpmr-")
		//ofile, _ := os.Create(iname)
		enc := json.NewEncoder(tmpfile)
		for _, kv := range kva {
			if ihash(kv.Key)%10 == i {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println("encode failed! ", err.Error())
				}
			}
		}
		err := tmpfile.Close()
		if err != nil {
			fmt.Println("tmpfile close err")
		}
		err2 := os.Rename(tmpfile.Name(), "./"+iname)
		if err2 != nil {
			fmt.Println("tmpfile rename erro ", iname)
		}
	}
	//lock.Lock()
	//*mapNum--            // 一次循环减一
	CallMapJobOK(mapjob) // 通知完成了此次 MapJob
	//lock.Unlock()
}

func ReduceWorker(reducef func(string, []string) string, reducejob ReduceJob,
	lock *sync.Mutex) {
	//defer wg.Done()
	//fmt.Println("----- Reduce start -----")
	var kva []KeyValue
	for _, fileName := range reducejob.ReduceName {
		ofile, err := os.Open(fileName)
		if err != nil {
			fmt.Println("cannot open %v", fileName, " ", err)
		}
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		ofile.Close()
		//os.Remove(fileName) // 移除打开过文件
	}
	sort.Sort(ByKey(kva)) // 排序
	//fmt.Println("ReduceTask len: ", len(kva))
	iname := "mr-out-" + strconv.Itoa(reducejob.ReduceID)
	tmpfile, _ := ioutil.TempFile("./", "tmpmr-")
	//outfile, _ := os.Create(iname)
	i := 0
	for i < len(kva) { // 对每一个中间文件值循环
		j := i + 1
		// 对第 i 之后的所有 Key 与第 i 相等的中间文件，由于已经经过排序，所以 j 即为同一个 key 的数量
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ { // 从 i 到 j 每个的 Value 放入切片
			values = append(values, kva[k].Value)
		}
		lock.Lock()
		output := reducef(kva[i].Key, values) // 调用 reduce 函数，每次一个 key
		lock.Unlock()
		// 一行一行地写入文件
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	err := tmpfile.Close()
	if err != nil {
		fmt.Println("tmpfile close err")
	}
	err2 := os.Rename(tmpfile.Name(), "./"+iname)
	if err2 != nil {
		fmt.Println("tmpfile rename erro ", iname)
	}
	//lock.Lock()
	//*reduceNum--               // 一次循环减一
	CallReduceJobOK(reducejob) // 通知完成了此次 reducejob
	//fmt.Println("reduce finish ---- ", reducejob.ReduceID)
	//lock.Unlock()
}

// Worker
// main/mrworker.go 调用这个函数
// 传入 map reduce 两个函数
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		workerArgs := &WorkerReply{} // 传指针
		//timeChannel := time.After(5 * time.Second) //计时器
		ok := CallWorkerArgsReply(workerArgs)
		//fmt.Println("Fin:---------", workerArgs.Fin)
		if !ok { // call失败
			fmt.Println("---------- exit worker ---------- call fail")
			break
		}
		//if workerArgs.Fin == 0 {
		//	fmt.Println(workerArgs)
		//}
		lock := sync.Mutex{} // 锁
		if workerArgs.MapJob.MapName != "" {
			//fmt.Println("MapID:", workerArgs.MapJob.MapID)
			MapWorker(mapf, workerArgs.MapJob, workerArgs.NReduce, &lock)
		} else if workerArgs.Fin == 1 {
			//fmt.Println("ReduceID:", workerArgs.ReduceJob.ReduceID)
			ReduceWorker(reducef, workerArgs.ReduceJob, &lock)
		} else {
			fmt.Println("---------- exit worker ----------")
			break
		}
		// 超时break，但是可能会导致无worker可用
		//select {
		//case <-timeChannel:
		//	fmt.Println("timeout")
		//	timeoutSign = 1
		//default:
		//}
		//if timeoutSign == 1 {
		//	break
		//}
	}
}

// CallMapJobOK RPC 通知某一个 Map 任务已经完成
func CallMapJobOK(mapjob MapJob) {
	reply := ""
	ok := call("Coordinator.MapJobOK", mapjob, &reply)
	if ok {
		//fmt.Println("---== success! CallMapJobOK")
	} else {
		fmt.Printf("MapJobOK failed!\n")
		panic("MapJobOK failed!")
	}
}

// CallReduceJobOK RPC 通知某一个 Reduce 任务已经完成
func CallReduceJobOK(reducejob ReduceJob) {
	reply := ""
	ok := call("Coordinator.ReduceJobOK", reducejob, &reply)
	if ok {
		//fmt.Println("---== success! CallReduceJobOK")
	} else {
		fmt.Printf("CallReduceJobOK failed!\n")
		panic("CallReduceJobOK failed!")
	}
}

// CallWorkerArgsReply  RPC 向协调器获取 Worker 所需参数
func CallWorkerArgsReply(workerArgs *WorkerReply) bool {
	//fflag := workerArgs.FinishFlag // 此次的 FinishFlag 作为参数传给 coordinator
	ok := call("Coordinator.WorkerArgsReply", 0, workerArgs)

	if ok {
		//fmt.Printf("-------- Call success!\n")

		return true
	} else {
		//fmt.Printf("CallWorkerArgsReply failed!\n")
		return false
	}
}

// 发送一个 RPC request 给 coordinator, 等待回应.
// 通常返回 true.
// 发生错误返回 false
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname) // 客户端连接服务端
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
