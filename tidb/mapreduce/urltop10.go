package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 .
// 通过pprof cpuprofile&memprofile&trace profile观测性能消耗情况
// 优化从文件I/O、GC、调度三个方向出发
// 改写了首轮统计词频的MapReduce任务，若唯一的URLs数量少，将减少首轮输出的中间文件体积
// 改写了了第二轮TOP10的MarReduce任务，并发的对多个中间文件进行排序，仅保留TOP10的URLs
// PS：以上都是通过减小中间文件的体检对文件I/O的调优
// 减小CreateFileAndBuf()方法返回的buffer size，slice内存申请从1MiB降至256KiB
// PS：以上是对GC的调优（会增加写文件的Syscall调用次数）
// 增加了执行Reduce时的并发（2*nWorkers)
// PS：此实验下Map任务数量在大文件case下I/O阻塞时间较长（主要是第一轮MapReduce），为防止过度调度故未在worker方法中对Map任务使用goroutine
// 提升了执行Reduce任务时产生的goroutine数量，稍稍提升了在处理小文件testcases时的速度
func URLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    nWorkers,
	})
	// rond 3: combine every 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    URLCombineMap,
		ReduceFunc: URLCombineReduce,
		NReduce:    1,
	})
	return args
}

// URLCountMap 统计不同URL出现的频次
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	kvs := make([]KeyValue, 0)
	cnts := make(map[string]int)
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		cnts[l]++
	}
	for k, v := range cnts {
		kvs = append(kvs, KeyValue{Key: k, Value: strconv.Itoa(v)})
	}
	return kvs
}

// URLCountReduce 合计中间文件中不同URL出现的频次
func URLCountReduce(key string, values []string) string {
	sum := 0
	for _, v := range values {
		iv, _ := strconv.Atoi(v)
		sum += iv
	}
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(sum))
}

// URLTop10Map 对分片文件进行排序，仅使用出现频次前10的URL产生产生中间文件
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0)
	cnts := make(map[string]int)
	for _, l := range lines {
		v := strings.TrimSpace(l)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			continue
		}
		cnts[tmp[0]] = n
	}
	ks, cs := TopN(cnts, 10)
	for i, k := range ks {
		kvs = append(kvs, KeyValue{k, strconv.Itoa(cs[i])})
	}
	return kvs
}

// URLTop10Reduce 合并中间文件中的URLs
func URLTop10Reduce(key string, values []string) string {
	return fmt.Sprintf("%s %s\n", key, values[0])
}

// URLCombineMap 合并分片文件的URLs
func URLCombineMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0)
	for _, l := range lines {
		kvs = append(kvs, KeyValue{"", l})
	}
	return kvs
}

// URLCombineReduce 对合并后的目标文件URLs排序
func URLCombineReduce(key string, values []string) string {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}

	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}
