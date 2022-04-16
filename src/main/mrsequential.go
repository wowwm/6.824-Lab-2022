package main

//
// 简单的顺序 MapReduce 示例.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.824/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// ByKey 用于按 key 排序.
// worker.go 中的 KeyValue 的切片类型
type ByKey []mr.KeyValue

// 此类型要使用 Sort 方法需要实现其接口，包含这几个函数

func (a ByKey) Len() int           { return len(a) }              // 获取长度
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }    // 交换 a[i], a[j] 值
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key } // 判断 key 大小

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}
	// 加载插件，得到 map 和 reduce 函数
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// 读取每个输入文件,
	// 交给 Map,
	// 计算中间 Map 输出.
	//
	intermediate := []mr.KeyValue{}        // 中间文件，为 KeyValue 的切片
	for _, filename := range os.Args[2:] { // 对传入的所有文件进行循环
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file) // 读取文件，后期尝试下用 ReadFile
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))      // 进行 map 操作
		intermediate = append(intermediate, kva...) // 加入中间文件
	}

	//
	// 与真实的 MapReduce 的最大不同就在这里，
	// 所有的中间数据都在一个地方：intermediate[],
	// 而不是分散在不同地方的 NxM buckets.
	//

	sort.Sort(ByKey(intermediate)) // 将中间文件转为 ByKey 类型并使用 Sort 方法
	// 创建输出文件
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// 在 intermediate[] 中每一个不同的 key 上调用 Reduce,
	// 打印结果到输出文件 mr-out-0.
	//
	i := 0
	for i < len(intermediate) { // 对每一个中间文件值循环
		j := i + 1
		// 对第 i 之后的所有 Key 与第 i 相等的中间文件，由于已经经过排序，所以 j 即为同一个 key 的数量
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		// 从 i 到 j 每个的 Value 放入切片
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 调用 reduce 函数
		output := reducef(intermediate[i].Key, values)

		// 注意 Reduce 输出每一行正确的格式.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// 从插件中加载 Map 和 Reduce函数, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
