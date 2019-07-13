package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"unsafe"

	"github.com/pingcap/tidb/util/mvmap"
)

type probeWorker struct {
	rangeCh chan []int
	closeCh chan bool
	result  uint64
}

type hashIndex struct {
	h []*mvmap.MVMap
}

type chunk struct {
	data   [][][]byte
	offset int
}

func (hi *hashIndex) Get(k []byte) [][]byte {
	var v [][]byte
	for _, m := range hi.h {
		v = m.Get(k, v)
	}
	return v
}

// Join accepts a join query of two relations, and returns the sum of
// relation0.col0 in the final result.
// Input arguments:
//   f0: file name of the given relation0
//   f1: file name of the given relation1
//   offset0: offsets of which columns the given relation0 should be joined
//   offset1: offsets of which columns the given relation1 should be joined
// Output arguments:
//   sum: sum of relation0.col0 in the final result
func Join(f0, f1 string, offset0, offset1 []int) (sum uint64) {
	var (
		probeTable  *[][][]byte
		buildTable  *[][][]byte
		probeOffset *[]int
		buildOffset *[]int
	)
	// 读取关系表
	tbl0, tbl1 := readTables(f0, f1)
	if len(*tbl0) > len(*tbl1) {
		probeTable = tbl0
		probeOffset = &offset0
		buildTable = tbl1
		buildOffset = &offset1
	} else {
		probeTable = tbl1
		probeOffset = &offset1
		buildTable = tbl0
		buildOffset = &offset0
	}
	// 构建散列索引结构
	// hashtable := _build(*buildTable, *buildOffset)
	hashtable := _concurrentBuild(*buildTable, *buildOffset)
	// 探测并返回求和结果
	return probeTblWithSum(hashtable, buildTable, probeTable, probeOffset)
}

// 探测关系元组并返回col0求和结果
func probeTblWithSum(hashtable *hashIndex, buildTable *[][][]byte, probeTable *[][][]byte, probeOffset *[]int) uint64 {
	concurrency := runtime.NumCPU()
	probeWorkers := make([]*probeWorker, concurrency)
	var wg sync.WaitGroup
	ch := make(chan []int, 2)
	for i := 0; i < concurrency; i++ {
		probeWorkers[i] = &probeWorker{
			rangeCh: ch,
			closeCh: make(chan bool),
			result:  uint64(0),
		}
		go func(w *probeWorker) {
			for {
				select {
				case ran := <-w.rangeCh:
					for _, row := range (*probeTable)[ran[0]:ran[1]] {
						rowIDs := _probe(hashtable, row, *probeOffset)
						for _, id := range rowIDs {
							v, err := strconv.ParseUint(string((*buildTable)[id][0]), 10, 64)
							if err != nil {
								panic("Join panic\n" + err.Error())
							}
							w.result += v
						}
					}
					wg.Done()
				case <-w.closeCh:
					return
				}
			}
		}(probeWorkers[i])
	}
	// 增加探测任务数量，提升探测任务执行快情景下的速度，使多核上的调度更加均衡
	threshold := len(*probeTable) / concurrency / 4
	if threshold < 1 {
		threshold = 1
	}
	var end int
	for j := 0; j < len(*probeTable); {
		end = j + threshold
		if len(*probeTable) < end {
			end = len(*probeTable)
		}
		wg.Add(1)
		ch <- []int{j, end}
		j = end
	}
	wg.Wait()

	sum := uint64(0)
	for _, w := range probeWorkers {
		sum += w.result
		w.closeCh <- true
	}

	return sum
}

// 读取CSV文件内容并解析
func readCSV(filepath string) [][][]byte {
	csvFile, _ := os.Open(filepath)
	defer csvFile.Close()
	reader := bufio.NewReader(csvFile)
	comma := []byte{','}
	var tbl [][][]byte
	var line []byte
	var err error

	for {
		line, err = reader.ReadBytes('\n')
		if err == io.EOF {
			if len(line) > 0 {
				tbl = append(tbl, bytes.Split(line, comma))
			}
			break
		}
		tbl = append(tbl, bytes.Split(line[:len(line)-1], comma))
	}

	return tbl
}

//
func readTables(tbl0Path, tbl1Path string) (*[][][]byte, *[][][]byte) {
	if tbl0Path == tbl1Path {
		tbl0 := readCSV(tbl0Path)
		return &tbl0, &tbl0
	}

	var wg sync.WaitGroup
	wg.Add(2)
	var tbl0, tbl1 [][][]byte
	go func() {
		defer wg.Done()
		tbl0 = readCSV(tbl0Path)
	}()
	go func() {
		defer wg.Done()
		tbl1 = readCSV(tbl1Path)
	}()
	wg.Wait()

	return &tbl0, &tbl1
}

// （并发）构建散列索引结构
func _concurrentBuild(data [][][]byte, offset []int) *hashIndex {
	wg := new(sync.WaitGroup)
	concurrency := runtime.NumCPU()
	hashIndex := &hashIndex{
		h: make([]*mvmap.MVMap, concurrency),
	}
	for n := range hashIndex.h {
		hashIndex.h[n] = mvmap.NewMVMap()
	}
	chunkCh := make(chan *chunk, concurrency)
	closeCh := make(chan bool)
	// 创建workers
	for i := 0; i < concurrency; i++ {
		go func(id int) {
			var keyBuffer []byte
			valBuffer := make([]byte, 8)
			for {
				select {
				case chunk := <-chunkCh:
					for i, row := range chunk.data {
						for _, off := range offset {
							keyBuffer = append(keyBuffer, row[off]...)
						}
						*(*int64)(unsafe.Pointer(&valBuffer[0])) = int64(chunk.offset + i)
						hashIndex.h[id].Put(keyBuffer, valBuffer)
						keyBuffer = keyBuffer[:0]
					}
					wg.Done()
				case <-closeCh:
					return
				}
			}
		}(i)
	}
	// 产生任务
	chunkSize := len(data) / concurrency
	if chunkSize < 1 {
		chunkSize = 1
	}
	end := 0
	for j := 0; j < len(data); {
		end += chunkSize
		if end > len(data) {
			end = len(data)
		}
		wg.Add(1)
		go func(s, e int) {
			chunkCh <- &chunk{
				data:   data[s:e],
				offset: s,
			}
		}(j, end)
		j = end
	}
	wg.Wait()
	// 关闭workers
	for i := 0; i < concurrency; i++ {
		closeCh <- true
	}

	return hashIndex
}

// 构建散列索引结构
func _build(data [][][]byte, offset []int) *hashIndex {
	var keyBuffer []byte
	valBuffer := make([]byte, 8)
	hashtable := mvmap.NewMVMap()
	for i, row := range data {
		for _, off := range offset {
			keyBuffer = append(keyBuffer, row[off]...)
		}
		*(*int64)(unsafe.Pointer(&valBuffer[0])) = int64(i)
		hashtable.Put(keyBuffer, valBuffer)
		keyBuffer = keyBuffer[:0]
	}
	return &hashIndex{[]*mvmap.MVMap{hashtable}}
}

// 探测一行元组
func _probe(hashtable *hashIndex, row [][]byte, offset []int) (rowIDs []int64) {
	var keyHash []byte
	// var vals [][]byte
	for _, off := range offset {
		keyHash = append(keyHash, row[off]...)
	}
	vals := hashtable.Get(keyHash)
	for _, val := range vals {
		rowIDs = append(rowIDs, *(*int64)(unsafe.Pointer(&val[0])))
	}
	return rowIDs
}
