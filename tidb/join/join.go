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
	chunk       [][][]byte
	offset      []int
	buildWorker *buildWorker
	result      uint64
}

type buildWorker struct {
	id          int
	chunks      [][][]byte
	hashtable   chan *mvmap.MVMap
	regionStart int
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
	tbl0, tbl1 := readCSV(f0), readCSV(f1)
	var (
		probeTable  *[][][]byte
		buildTable  *[][][]byte
		probeOffset *[]int
		buildOffset *[]int
	)

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
	probeWorkers := make([]*probeWorker, 0)

	var wg sync.WaitGroup
	for j := 0; j < len(buildWorkers); j++ {
		w := &probeWorker{
			chunk:       (*probeTable),
			buildWorker: buildWorkers[j],
			offset:      *probeOffset,
			result:      uint64(0),
		}
		probeWorkers = append(probeWorkers, w)
		wg.Add(1)
		go func(w *probeWorker) {
			defer wg.Done()
			sum := uint64(0)
			hashtable := <-w.buildWorker.hashtable
			for _, row := range w.chunk {
				rowIDs := _probe(hashtable, row, *probeOffset)
				for _, id := range rowIDs {
					v, err := strconv.ParseUint(string((*buildTable)[id][0]), 10, 64)
					if err != nil {
						panic("Join panic\n" + err.Error())
					}
					sum += v
				}
			}
			w.result = sum
		}(w)
	}

	wg.Wait()

	for _, w := range probeWorkers {
		sum += w.result
	}

	return sum
}

// readCSV 从CSV文件中读取内容并解析
func readCSV(filepath string) [][][]byte {
	csvFile, _ := os.Open(filepath)
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
