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

// probeWorker
type probeWorker struct {
	chunkCh     chan [][][]byte
	closeCh     chan bool
	buildWorker *buildWorker
	result      uint64
}

// buildWorker
type buildWorker struct {
	id          int
	chunks      [][][]byte
	hashtable   chan *mvmap.MVMap
	regionStart int //用于构建正确的索引
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
	var wg sync.WaitGroup
	var tbl0, tbl1 [][][]byte
	wg.Add(2)
	go func() {
		defer wg.Done()
		tbl0 = readCSV(f0)
	}()
	go func() {
		defer wg.Done()
		tbl1 = readCSV(f1)
	}()
	wg.Wait()
	// tbl0, tbl1 := readCSV(f0), readCSV(f1)
	var (
		probeTable  *[][][]byte
		buildTable  *[][][]byte
		probeOffset *[]int
		buildOffset *[]int
	)
	// 采用更小的关系作为构建关系
	if len(tbl0) > len(tbl1) {
		probeTable = &tbl0
		probeOffset = &offset0
		buildTable = &tbl1
		buildOffset = &offset1
	} else {
		probeTable = &tbl1
		probeOffset = &offset1
		buildTable = &tbl0
		buildOffset = &offset0
	}

	buildWorkers := conBuild(*buildTable, *buildOffset)
	concurrency := len(buildWorkers)
	probeWorkers := make([]*probeWorker, 0, concurrency)
	closeCh := make(chan bool, concurrency)

	for j := 0; j < concurrency; j++ {
		w := &probeWorker{
			chunkCh:     make(chan [][][]byte),
			closeCh:     closeCh,
			buildWorker: buildWorkers[j],
			result:      uint64(0),
		}
		probeWorkers = append(probeWorkers, w)
		go func(w *probeWorker) {
			hashtable := <-w.buildWorker.hashtable
			for {
				select {
				case chunk := <-w.chunkCh:
					for _, row := range chunk {
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
				case <-closeCh:
					return
				}
			}
		}(w)
	}
	// 数据分片的阈值
	threshold := len(*probeTable) / 2
	if threshold < 1 {
		threshold = 1
	}
	var end int
	for i := 0; i < len(*probeTable); {
		end = i + threshold
		if len(*probeTable) < end {
			end = len(*probeTable)
		}
		for k := 0; k < concurrency; k++ {
			wg.Add(1)
			go func(id, s, e int) {
				probeWorkers[id].chunkCh <- (*probeTable)[s:e]
			}(k, i, end)
		}
		i = end
	}

	wg.Wait()

	for _, w := range probeWorkers {
		sum += w.result
		closeCh <- true
	}

	return sum
}

// readCSV 从CSV文件中读取内容并解析
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

// build 根据数据分片构建散列索引
func (w *buildWorker) build(offset []int) {
	var keyBuffer []byte
	valBuffer := make([]byte, 8)
	hashtable := mvmap.NewMVMap()
	for i, row := range w.chunks {
		for _, off := range offset {
			keyBuffer = append(keyBuffer, row[off]...)
		}
		*(*int64)(unsafe.Pointer(&valBuffer[0])) = int64(w.regionStart + i)
		hashtable.Put(keyBuffer, valBuffer)
		keyBuffer = keyBuffer[:0]
	}
	w.hashtable <- hashtable
}

// conBuild 根据表数据产生并发构建散列索引的任务
func conBuild(data [][][]byte, offset []int) []*buildWorker {
	concurrency := runtime.NumCPU()
	chunkSize := len(data) / concurrency
	buildWorkers := make([]*buildWorker, 0)

	for i := 0; i < concurrency; i++ {
		down := i * chunkSize
		up := down + chunkSize
		if up > len(data) {
			up = len(data)
		}
		w := &buildWorker{
			id:          i,
			chunks:      data[down:up],
			hashtable:   make(chan *mvmap.MVMap),
			regionStart: down,
		}
		buildWorkers = append(buildWorkers, w)
		go w.build(offset)
	}

	return buildWorkers
}

// _probe 探测一行数据
func _probe(hashtable *mvmap.MVMap, row [][]byte, offset []int) (rowIDs []int64) {
	var keyHash []byte
	var vals [][]byte
	for _, off := range offset {
		keyHash = append(keyHash, row[off]...)
	}
	vals = hashtable.Get(keyHash, vals)
	for _, val := range vals {
		rowIDs = append(rowIDs, *(*int64)(unsafe.Pointer(&val[0])))
	}
	return rowIDs
}
