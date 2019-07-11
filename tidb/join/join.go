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
		hashtable   *hashTable
		probeTable  *[][][]byte
		buildTable  *[][][]byte
		probeOffset *[]int
	)

	if len(tbl0) > len(tbl1) {
		probeTable = &tbl0
		probeOffset = &offset0
		buildTable = &tbl1
		hashtable = build(tbl1, offset1)
	} else {
		probeTable = &tbl1
		probeOffset = &offset1
		buildTable = &tbl0
		hashtable = build(tbl0, offset0)
	}

	concurrency := runtime.NumCPU()
	chunkSize := len(*probeTable) / concurrency
	workers := make([]*joinWorker, 0)

	for i := 0; i < concurrency; i++ {
		up := i * chunkSize
		down := up + chunkSize
		if down > len(*probeTable) {
			down = len(*probeTable)
		}
		w := &joinWorker{
			chunk:  (*probeTable)[up:down],
			offset: *probeOffset,
			result: make(chan uint64),
		}
		workers = append(workers, w)
		go func(w *joinWorker) {
			sum := uint64(0)
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
			w.result <- sum
		}(w)
	}

	for _, w := range workers {
		sum += <-w.result
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
		var tmp [][]byte

		line, err = reader.ReadBytes('\n')
		// line, err = reader.ReadSlice('\n')
		if err == io.EOF {
			if len(line) > 0 {
				tmp = bytes.Split(line, comma)
				tbl = append(tbl, tmp)
			}
			break
		}
		tmp = bytes.Split(line[:len(line)-1], comma)
		tbl = append(tbl, tmp)
	}

	return tbl
}

type joinWorker struct {
	chunk  [][][]byte
	offset []int
	result chan uint64
}

type buildWorker struct {
	id          int
	chunks      [][][]byte
	hashtable   *mvmap.MVMap
	regionStart int
}

type hashTable struct {
	hashtables []*mvmap.MVMap
}

func (ht *hashTable) Get(keyHash []byte, vals [][]byte) [][]byte {
	for _, hashtable := range ht.hashtables {
		vals = hashtable.Get(keyHash, vals)
	}
	return vals
}

func (ht *hashTable) Len() int {
	len := 0
	for _, hashtable := range ht.hashtables {
		len += hashtable.Len()
	}
	return len
}

func (w *buildWorker) build(offset []int, ch chan *buildWorker) {
	var keyBuffer []byte
	valBuffer := make([]byte, 8)
	for i, row := range w.chunks {
		for _, off := range offset {
			keyBuffer = append(keyBuffer, row[off]...)
		}
		*(*int64)(unsafe.Pointer(&valBuffer[0])) = int64(w.regionStart + i)
		w.hashtable.Put(keyBuffer, valBuffer)
		keyBuffer = keyBuffer[:0]
	}
	ch <- w
}

func build(data [][][]byte, offset []int) *hashTable {
	concurrency := runtime.NumCPU()
	chunkSize := len(data) / concurrency
	ch := make(chan *buildWorker, concurrency)

	for i := 0; i < concurrency; i++ {
		down := i * chunkSize
		up := down + chunkSize
		if up > len(data) {
			up = len(data)
		}
		w := &buildWorker{
			id:          i,
			chunks:      data[down:up],
			hashtable:   mvmap.NewMVMap(),
			regionStart: down,
		}
		go w.build(offset, ch)
	}

	var wg sync.WaitGroup
	wg.Add(concurrency)
	hashtable := &hashTable{
		hashtables: make([]*mvmap.MVMap, concurrency),
	}

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			w := <-ch
			hashtable.hashtables[w.id] = w.hashtable
		}()
	}

	wg.Wait()

	return hashtable
}

func _probe(hashtable *hashTable, row [][]byte, offset []int) (rowIDs []int64) {
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
