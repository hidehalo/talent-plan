package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 .
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
	args = append(args, RoundArgs{
		MapFunc:    URLCombineMap,
		ReduceFunc: URLCombineReduce,
		NReduce:    1,
	})
	return args
}

func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	kvs := make([]KeyValue, 0, len(lines))
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

func URLCountReduce(key string, values []string) string {
	sum := 0
	for _, v := range values {
		iv, _ := strconv.Atoi(v)
		sum += iv
	}
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(sum))
}

func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))
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

func URLTop10Reduce(key string, values []string) string {
	return fmt.Sprintf("%s %s\n", key, values[0])
}

func URLCombineMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		kvs = append(kvs, KeyValue{"", l})
	}
	return kvs
}

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
