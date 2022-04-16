package main

//
// a word-count application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc.go
//

import "6.824/mr"
import "unicode"
import "strings"
import "strconv"

//
// Map 每个输入文件调用一次，第一个参数文件名，第二个参数文件完整内容。返回值是个 key/value pairs 切片。
// You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func Map(filename string, contents string) []mr.KeyValue {
	// 探测词分隔符函数
	ff := func(r rune) bool { return !unicode.IsLetter(r) } // 传入unicode字符，判断是否是字母的函数

	// 内容分割为词数组
	words := strings.FieldsFunc(contents, ff) // 通过传入的判断函数作为分割点分割内容，返回数组

	kva := []mr.KeyValue{} // 在 worker.go 中定义的 struct 的切片
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

// Reduce
// 对于 map 任务生成的每个键，reduce 函数都会调用一次，其中包含任何 map 任务为该键创建的所有值的列表。
//
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values)) // 将数字转换成对应的字符串类型的数字
}
