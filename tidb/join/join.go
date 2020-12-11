package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"unsafe"

	"github.com/pingcap/tidb/util/mvmap"
)

func _mergeCols(parentNeed, selfNeed []int) []int {
	colsNeed := make([]int, len(parentNeed), len(parentNeed)+len(selfNeed))
	copy(colsNeed, parentNeed)
	contain := func(i int, arr []int) bool {
		for j := 0; j < len(arr); j++ {
			if i == arr[j] {
				return true
			}
		}
		return false
	}
	for _, selfCol := range selfNeed {
		if !contain(selfCol, parentNeed) {
			colsNeed = append(colsNeed, selfCol)
		}
	}
	return colsNeed
}

type probeWorker struct {
	ctx        context.Context
	outerTable *tableReader
	sumTable   *tableReader
	sum        uint64
}

// 探测一行元组
func (worker *probeWorker) probeOneRow(hashtable *mvmap.MVMap, row [][]byte, offset []int) (rowIDs []int64) {
	var keyHash []byte
	var vals [][]byte
	for _, off := range offset {
		keyHash = append(keyHash, worker.outerTable.getColumn(row, off)...)
	}
	vals = hashtable.Get(keyHash, vals)
	for _, val := range vals {
		rowIDs = append(rowIDs, *(*int64)(unsafe.Pointer(&val[0])))
	}
	return rowIDs
}

func (worker *probeWorker) probeWithSum(hashtable *mvmap.MVMap, innerTable [][][]byte, outerTable [][][]byte, offset []int) {
	for _, row := range outerTable {
		rowIDs := worker.probeOneRow(hashtable, row, offset)
		for _, id := range rowIDs {
			v, err := strconv.ParseUint(string(worker.sumTable.getColumn(innerTable[id], 0)), 10, 64)
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
	colsMap  []int
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

func (tableReader *tableReader) read() chan *chunk {
	go tableReader.fetchData()
	return tableReader.chunkOut
}

func (tableReader *tableReader) readRow(rawRow []byte, sep byte, chunk *chunk) {
	if len(rawRow) > 0 {
		row := bytes.Split(rawRow, []byte{sep})
		if tableReader.colsMap != nil {
			for i := 0; i < len(row); i++ {
				for newIdx, oldIdx := range tableReader.colsMap {
					if newIdx != oldIdx {
						row[newIdx], row[oldIdx] = row[oldIdx], row[newIdx]
					}
				}
				row = row[:len(tableReader.colsMap)]
			}
		}
		chunk.appendRow(row)
	}
}

func (tableReader *tableReader) fetchData() {
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
				tableReader.readRow(line, ',', chunk)
				tableReader.chunkOut <- chunk
				return
			} else if err == nil {
				tableReader.readRow(line, ',', chunk)
			} else {
				panic(err)
			}
		}
	}
}

func (tableReader *tableReader) borrowChunk() *chunk {
	chunk := <-tableReader.chunkIn
	return chunk
}

func (tableReader *tableReader) releaseChunk(chunk *chunk) {
	chunk.reset()
	tableReader.chunkIn <- chunk
}

func (tableReader *tableReader) getColumn(row [][]byte, colIdx int) []byte {
	if tableReader.colsMap == nil {
		return row[colIdx]
	}
	find := -1
	for i := 0; i < len(tableReader.colsMap); i++ {
		if tableReader.colsMap[i] == colIdx {
			find = i
			break
		}
	}
	if find == -1 || find >= len(row) {
		panic(fmt.Sprintf("Can't find column[%d] in map %v", colIdx, tableReader.colsMap))
	}
	return row[find]
}

type hashJoiner struct {
	ctx              context.Context
	innerTableReader *tableReader
	outerTableReader *tableReader
	swap             bool
}

// 探测关系元组并返回col0求和结果
func (joiner *hashJoiner) probeStreamWithSum(hashtable *mvmap.MVMap, innerTable [][][]byte, probeOffset []int) uint64 {
	chunkCh := joiner.outerTableReader.read()
	concurrency := runtime.NumCPU()
	probeWorkers := make([]*probeWorker, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		probeWorkers[i] = &probeWorker{
			ctx:        joiner.ctx,
			outerTable: joiner.outerTableReader,
			sum:        uint64(0),
		}
		if joiner.swap {
			probeWorkers[i].sumTable = joiner.outerTableReader
		} else {
			probeWorkers[i].sumTable = joiner.innerTableReader
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
		ctx:        joiner.ctx,
		outerTable: joiner.outerTableReader,
		sum:        uint64(0),
	}
	if joiner.swap {
		worker.sumTable = joiner.outerTableReader
	} else {
		worker.sumTable = joiner.innerTableReader
	}
	worker.probeWithSum(hashtable, innerTable, outerTable, probeOffset)
	return worker.sum
}

// 构建散列索引结构
func (joiner *hashJoiner) build(offset []int) ([][][]byte, *mvmap.MVMap, error) {
	chunkCh := joiner.innerTableReader.read()
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
					keyBuffer = append(keyBuffer, joiner.innerTableReader.getColumn(row, off)...)
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

func (joiner *hashJoiner) joinSelf(innerTablePath string, innerOffset, outerOffset []int) uint64 {
	joiner.innerTableReader.colsMap = _mergeCols(innerOffset, outerOffset)
	joiner.outerTableReader = joiner.innerTableReader
	innerTable, hashtable, err := joiner.build(innerOffset)
	if err != nil {
		panic(err)
	}
	return joiner.probeTblWithSum(hashtable, innerTable, innerTable, outerOffset)
}

func (joiner *hashJoiner) join(innerTablePath, outerTablePath string, innerOffset, outerOffset []int) uint64 {
	innerStat, err := os.Stat(innerTablePath)
	if err != nil {
		panic(err)
	}
	outerStat, err := os.Stat(outerTablePath)
	if err != nil {
		panic(err)
	}
	innerOffset = _mergeCols([]int{0}, innerOffset)
	// inner表用与build hashtable，越小越好
	if innerStat.Size() > outerStat.Size() {
		innerTablePath, outerTablePath = outerTablePath, innerTablePath
		innerOffset, outerOffset = outerOffset, innerOffset
		joiner.swap = true
	}
	joiner.innerTableReader = newTableReader(joiner.ctx, innerTablePath, 1, runtime.NumCPU()*1000)
	// 相同表不重复读
	if innerTablePath == outerTablePath {
		return joiner.joinSelf(innerTablePath, innerOffset, outerOffset)
	}
	joiner.innerTableReader.colsMap = innerOffset
	innerTable, hashtable, err := joiner.build(innerOffset)
	if err != nil {
		panic(err)
	}
	joiner.outerTableReader = newTableReader(joiner.ctx, outerTablePath, runtime.NumCPU(), 1000)
	joiner.outerTableReader.colsMap = outerOffset
	return joiner.probeStreamWithSum(hashtable, innerTable, outerOffset)
}

func newJoiner(ctx context.Context) *hashJoiner {
	return &hashJoiner{ctx: ctx}
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
