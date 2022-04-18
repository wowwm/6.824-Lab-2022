package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
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
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func MapWorker(mapf func(string, string) []KeyValue, MapNum int, fileName string, nReduce int) {
	fmt.Println("----- start map -----")
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file) // 读取文件，后期尝试下用 ReadFile
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	sort.Sort(ByKey(kva)) // 排序
	for i := 0; i < nReduce; i++ {
		iname := "mr-" + strconv.Itoa(MapNum) + strconv.Itoa(i)
		fmt.Println(iname)
		ofile, _ := os.Create(iname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			if ihash(kv.Key)%10 == i {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println("encode failed! ", err.Error())
				}
			}
		}
	}
}

func ReduceWorker(reducef func(string, []string) string, ReduceNum int, rdFiles []string) {
	fmt.Println("----- Reduce start -----")
	var kva []KeyValue
	for _, fileName := range rdFiles {
		ofile, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
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
		os.Remove(fileName)
	}
	sort.Sort(ByKey(kva)) // 排序
	fmt.Println("ReduceTask len: ", len(kva))
	outfile, _ := os.Create("mr-out-" + strconv.Itoa(ReduceNum))
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
		output := reducef(kva[i].Key, values) // 调用 reduce 函数，每次一个 key
		// 一行一行地写入文件
		fmt.Fprintf(outfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outfile.Close()
}

// Worker
// main/mrworker.go 调用这个函数
// 传入 map reduce 两个函数
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		workerArgs := CallWorkerArgsReply()
		nMap := len(workerArgs.UncommitFiles)
		nReduce := workerArgs.NReduce
		//fmt.Println("get workerArgs success!  ", nMap)
		if nMap > 0 { // Map 任务未结束
			for MapNum, fileName := range workerArgs.UncommitFiles {
				if fileName != "" { // 文件名非空（空说明已经完成了）
					go MapWorker(mapf, MapNum, fileName, nReduce) // MapNum 为文件编号也为任务编号
				}
			}
		} else { //Map 结束，开始 Reduce
			time.Sleep(time.Second * 5)
			for i := 0; i < nReduce; i++ {
				var rdFiles []string // 每一个 Reduce 任务的文件名切片
				for j := 0; j < workerArgs.NMap; j++ {
					filename := "mr-" + strconv.Itoa(j) + strconv.Itoa(i)
					rdFiles = append(rdFiles, filename)
				}
				fmt.Println(rdFiles)
				go ReduceWorker(reducef, i, rdFiles)
			}
		}
		time.Sleep(time.Second * 5)
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// CallWorkerArgsReply  RPC 向协调器获取所需参数
func CallWorkerArgsReply() *WorkerReply {
	workerArgs := &WorkerReply{} // 传指针
	ok := call("Coordinator.WorkerArgsReply", 0, workerArgs)
	if ok {
		fmt.Println("--- success! WorkerArgsReply: ", workerArgs)
		return workerArgs
	} else {
		fmt.Printf("CallWorkerArgsReply failed!\n")
		panic("CallWorkerArgsReply failed!")
	}
}

// CallExample
// 示例函数 对 coordinator 进行一次 RPC 调用.
// the RPC 参数 在 rpc.go 中定义.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}
	// fill in the argument(s).
	args.X = 99
	// declare a reply structure.
	reply := ExampleReply{}
	// 发送 RPC request, 等待 reply.
	// the "Coordinator.Example" 告诉接受服务器 想要调用 struct Coordinator 的 Example() 方法
	ok := call("Coordinator.Example", &args, &reply)
	if ok { // 返回true
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// 发送一个 RPC request 给 coordinator, 等待回应.
// 通常返回 true.
// 发生错误返回 false
//
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

	fmt.Println(err)
	return false
}
