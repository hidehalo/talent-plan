package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"unsafe"

	"github.com/pingcap/tidb/util/mvmap"
)

type probeWorker struct {
	ctx context.Context
	sum uint64
}

// 探测一行元组
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

func (worker *probeWorker) probeWithSum(hashtable *mvmap.MVMap, innerTable [][][]byte, outerTable [][][]byte, offset []int) {
	for _, row := range outerTable {
		rowIDs := _probe(hashtable, row, offset)
		for _, id := range rowIDs {
			v, err := strconv.ParseUint(string(innerTable[id][0]), 10, 64)
			if err != nil {
				panic("Join panic\n" + err.Error())
			}
			worker.sum += v
		}
	}
}

type chunk struct {
	data   [][][]byte
	cursor int
}

func newChunk(cap int) *chunk {
	return &chunk{
		data:   make([][][]byte, 0, cap),
		cursor: 0,
	}
}

func (chunk *chunk) appendRow(row [][]byte) {
	if len(chunk.data) <= cap(chunk.data) {
		chunk.data = append(chunk.data, row)
	} else {
		chunk.data[chunk.cursor] = row
	}
	chunk.cursor++
}

func (chunk *chunk) reset() {
	chunk.data = chunk.data[:0]
	chunk.cursor = 0
}

func (chunk *chunk) isFull() bool {
	return chunk.cursor >= cap(chunk.data)
}

type tableReader struct {
	ctx      context.Context
	close    context.CancelFunc
	path     string
	chunkIn  chan *chunk
	chunkOut chan *chunk
}

func newTableReader(ctx context.Context, path string, chunkNum, chunkCap int) *tableReader {
	tableReader := &tableReader{path: path}
	tableReader.ctx, tableReader.close = context.WithCancel(ctx)
	tableReader.chunkIn = make(chan *chunk, chunkNum)
	tableReader.chunkOut = make(chan *chunk, chunkNum)
	for i := 0; i < chunkNum; i++ {
		tableReader.chunkIn <- newChunk(chunkCap)
	}
	return tableReader
}

func (tableReader *tableReader) read(colsNeed []int) chan *chunk {
	go func() {
		defer func() {
			close(tableReader.chunkOut)
			tableReader.close()
		}()
		csvFile, err := os.Open(tableReader.path)
		if err != nil {
			panic(err)
		}
		defer csvFile.Close()
		reader := bufio.NewReader(csvFile)
		comma := []byte{','}
		var line []byte
		chunk := tableReader.borrowChunk()
		for {
			select {
			case <-tableReader.ctx.Done():
				return
			default:
				if chunk.isFull() {
					tableReader.chunkOut <- chunk
					chunk = tableReader.borrowChunk()
				}
				line, err = reader.ReadBytes('\n')
				if err == io.EOF {
					if len(line) > 0 {
						chunk.appendRow(bytes.Split(line, comma))
					}
					tableReader.chunkOut <- chunk
					return
				}
				chunk.appendRow(bytes.Split(line[:len(line)-1], comma))
			}
		}
	}()
	return tableReader.chunkOut
}

func (tableReader *tableReader) borrowChunk() *chunk {
	chunk := <-tableReader.chunkIn
	chunk.reset()
	return chunk
}

func (tableReader *tableReader) releaseChunk(chunk *chunk) {
	tableReader.chunkIn <- chunk
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
	ctx := context.Background()
	joiner := newJoiner(ctx)
	sum = joiner.join(f0, f1, offset0, offset1)
	return
}

type hashJoiner struct {
	ctx              context.Context
	innerTableReader *tableReader
	outerTableReader *tableReader
}

// 探测关系元组并返回col0求和结果
func (joiner *hashJoiner) probeStreamWithSum(hashtable *mvmap.MVMap, innerTable [][][]byte, probeOffset []int) uint64 {
	chunkCh := joiner.outerTableReader.read(nil)
	concurrency := runtime.NumCPU()
	probeWorkers := make([]*probeWorker, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		probeWorkers[i] = &probeWorker{
			ctx: joiner.ctx,
			sum: uint64(0),
		}
		go func(w *probeWorker) {
			defer wg.Done()
			for {
				select {
				case <-w.ctx.Done():
					return
				case chunk := <-chunkCh:
					if chunk == nil {
						return
					}
					w.probeWithSum(hashtable, innerTable, chunk.data, probeOffset)
					joiner.outerTableReader.releaseChunk(chunk)
				}
			}
		}(probeWorkers[i])
	}
	wg.Wait()
	sum := uint64(0)
	for _, w := range probeWorkers {
		sum += w.sum
	}
	return sum
}

func (joiner *hashJoiner) probeTblWithSum(hashtable *mvmap.MVMap, innerTable, outerTable [][][]byte, probeOffset []int) uint64 {
	worker := &probeWorker{
		ctx: joiner.ctx,
		sum: uint64(0),
	}
	worker.probeWithSum(hashtable, innerTable, outerTable, probeOffset)
	return worker.sum
}

// 构建散列索引结构
func (joiner *hashJoiner) build(offset []int) ([][][]byte, *mvmap.MVMap, error) {
	chunkCh := joiner.innerTableReader.read(nil)
	innerTable := make([][][]byte, 0)
	hashIndex := mvmap.NewMVMap()
	var keyBuffer []byte
	valBuffer := make([]byte, 8)
	var id int
	var err error
loop:
	for {
		select {
		case <-joiner.ctx.Done():
			break loop
		case chunk := <-chunkCh:
			if chunk == nil {
				break loop
			}
			innerTable = append(innerTable, chunk.data...)
			for _, row := range chunk.data {
				for _, off := range offset {
					keyBuffer = append(keyBuffer, row[off]...)
				}
				*(*int64)(unsafe.Pointer(&valBuffer[0])) = int64(id)
				hashIndex.Put(keyBuffer, valBuffer)
				keyBuffer = keyBuffer[:0]
				id++
			}
			joiner.innerTableReader.releaseChunk(chunk)
		}
	}
	return innerTable, hashIndex, err
}

func (joiner *hashJoiner) join(innerTablePath, outerTablePath string, innerOffset, outerOffset []int) uint64 {
	joiner.innerTableReader = newTableReader(joiner.ctx, innerTablePath, 10, 1000)
	innerTable, hashtable, err := joiner.build(innerOffset)
	if err != nil {
		panic(err)
	}
	if innerTablePath != outerTablePath {
		joiner.outerTableReader = newTableReader(joiner.ctx, outerTablePath, 10*runtime.NumCPU(), 1000)
		return joiner.probeStreamWithSum(hashtable, innerTable, outerOffset)
	}
	return joiner.probeTblWithSum(hashtable, innerTable, innerTable, outerOffset)
}

func newJoiner(ctx context.Context) *hashJoiner {
	return &hashJoiner{ctx: ctx}
}
