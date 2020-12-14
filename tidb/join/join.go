package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
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

// sum via inner table col0
func _sumFunc1(innerIDs []int64, innerTableRows [][][]byte, outerTableRow [][]byte, columnResolver *columnResolver) uint64 {
	var sum uint64
	for _, id := range innerIDs {
		v, err := strconv.ParseUint(string(columnResolver.resolve(innerTableRows[id], 0)), 10, 64)
		if err != nil {
			panic("Join panic\n" + err.Error())
		}
		sum += v
	}
	return sum
}

// sum via outer table col0
func _sumFunc2(innerIDs []int64, innerTableRows [][][]byte, outerTableRow [][]byte, columnResolver *columnResolver) uint64 {
	v, err := strconv.ParseUint(string(columnResolver.resolve(outerTableRow, 0)), 10, 64)
	if err != nil {
		panic("Join panic\n" + err.Error())
	}
	return uint64(len(innerIDs)) * v
}

type probeWorker struct {
	ctx                 context.Context
	hashtable           *mvmap.MVMap
	innerColumnResolver *columnResolver
	outerColumnResolver *columnResolver
	probeCols           []int
	sumInner            bool
	sumFunc             func(innerIDs []int64, innerTableRows [][][]byte, outerTableRow [][]byte, columnResolver *columnResolver) uint64
	sum                 uint64
	quit                chan struct{}
}

// 探测一行元组，返回inner table行ID集
func (worker *probeWorker) probeOneRow(row [][]byte) (rowIDs []int64) {
	var keyHash []byte
	var vals [][]byte
	for _, off := range worker.probeCols {
		keyHash = append(keyHash, worker.outerColumnResolver.resolve(row, off)...)
	}
	fmt.Println("Probe use key", string(keyHash))
	vals = worker.hashtable.Get(keyHash, vals)
	if len(vals) > 0 {
		fmt.Println("Hitted")
	}
	for _, val := range vals {
		rowIDs = append(rowIDs, *(*int64)(unsafe.Pointer(&val[0])))
	}
	return rowIDs
}

// 探测并进行求和
func (worker *probeWorker) probeWithSum(innerTable [][][]byte, outerTable [][][]byte) {
	var sum uint64
	for _, row := range outerTable {
		rowIDs := worker.probeOneRow(row)
		if worker.sumInner {
			sum = worker.sumFunc(rowIDs, innerTable, row, worker.innerColumnResolver)
		} else {
			sum = worker.sumFunc(rowIDs, innerTable, row, worker.outerColumnResolver)
		}
		worker.sum += sum
	}
}

func (worker *probeWorker) runStream(innerTableRows [][][]byte, outerTableReader *tableReader, chunkCh chan *chunk) {
	defer worker.close()
	for {
		select {
		case <-worker.ctx.Done():
			return
		case chunk := <-chunkCh:
			if chunk == nil {
				return
			}
			worker.probeWithSum(innerTableRows, chunk.data[:chunk.cursor])
			outerTableReader.releaseChunk(chunk)
		}
	}
}

func (worker *probeWorker) runSerial(innerTableRows, outerTableRows [][][]byte) {
	defer worker.close()
	select {
	case <-worker.ctx.Done():
		return
	default:
		worker.probeWithSum(innerTableRows, outerTableRows)
		return
	}
}

func (worker *probeWorker) close() {
	worker.quit <- struct{}{}
}

func (worker *probeWorker) getSum() uint64 {
	<-worker.quit
	return worker.sum
}

func newProbeWorker(ctx context.Context, hashtable *mvmap.MVMap, innerColResolver, outerColResolver *columnResolver, probeCols []int) *probeWorker {
	return &probeWorker{
		ctx:                 ctx,
		hashtable:           hashtable,
		innerColumnResolver: innerColResolver,
		outerColumnResolver: outerColResolver,
		probeCols:           probeCols,
		sum:                 uint64(0),
		quit:                make(chan struct{}),
	}
}

type hashJoiner struct {
	ctx              context.Context
	innerTableReader *tableReader
	outerTableReader *tableReader
	innerOn          []int
	outerOn          []int
	optimized        bool
	selfJoin         bool
}

// 探测关系数据流并返回r0.col0求和结果
func (joiner *hashJoiner) concurrentProbeStreamWithSum(hashtable *mvmap.MVMap, innerTable [][][]byte) uint64 {
	concurrency := runtime.NumCPU()
	probeWorkers := make([]*probeWorker, 0, concurrency)
	chunkCh := joiner.outerTableReader.read()
	for i := 0; i < concurrency; i++ {
		worker := newProbeWorker(
			joiner.ctx, hashtable,
			joiner.innerTableReader.columnResolver,
			joiner.outerTableReader.columnResolver,
			joiner.outerOn,
		)
		if joiner.optimized {
			worker.sumInner = false
			worker.sumFunc = _sumFunc2
		} else {
			worker.sumInner = true
			worker.sumFunc = _sumFunc1
		}
		probeWorkers = append(probeWorkers, worker)
		go worker.runStream(innerTable, joiner.outerTableReader, chunkCh)
	}
	var sum uint64
	for _, w := range probeWorkers {
		sum += w.getSum()
	}
	return sum
}

// 探测关系元组并返回r0.col0求和结果
func (joiner *hashJoiner) concurrentProbeSerialWithSum(hashtable *mvmap.MVMap, innerTable, outerTable [][][]byte) uint64 {
	concurrency := runtime.NumCPU()
	probeWorkers := make([]*probeWorker, 0, concurrency)
	chunkSize := len(outerTable) / concurrency
	for i := 0; i < concurrency; i++ {
		worker := newProbeWorker(
			joiner.ctx, hashtable,
			joiner.innerTableReader.columnResolver,
			joiner.outerTableReader.columnResolver,
			joiner.outerOn,
		)
		if joiner.selfJoin {
			worker.sumInner = true
			worker.sumFunc = _sumFunc2
		} else if joiner.optimized {
			worker.sumInner = false
			worker.sumFunc = _sumFunc2
		} else {
			worker.sumInner = true
			worker.sumFunc = _sumFunc1
		}
		probeWorkers = append(probeWorkers, worker)
		start := i * chunkSize
		end := start + chunkSize
		if end > len(outerTable) {
			end = len(outerTable)
		}
		go worker.runSerial(innerTable, outerTable[start:end])
	}
	var sum uint64
	for _, w := range probeWorkers {
		sum += w.getSum()
	}
	return sum
}

// 构建散列索引结构
func (joiner *hashJoiner) build() ([][][]byte, *mvmap.MVMap, error) {
	chunkCh := joiner.innerTableReader.read()
	innerTable := make([][][]byte, 0)
	hashIndex := mvmap.NewMVMap()
	var keyBuffer []byte
	valBuffer := make([]byte, 8)
	var id int64
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
			innerTable = append(innerTable, chunk.data[:chunk.cursor]...)
			for _, row := range chunk.data[:chunk.cursor] {
				for _, off := range joiner.innerOn {
					keyBuffer = append(keyBuffer, joiner.innerTableReader.getColumn(row, off)...)
				}
				*(*int64)(unsafe.Pointer(&valBuffer[0])) = id
				fmt.Println("Put key=", string(keyBuffer), "Val=", id)
				hashIndex.Put(keyBuffer, valBuffer)
				keyBuffer = keyBuffer[:0]
				id++
			}
			joiner.innerTableReader.releaseChunk(chunk)
		}
	}
	return innerTable, hashIndex, err
}

// 优化join执行
func (joiner *hashJoiner) optimize() {
	// 自连接不重复磁盘IO
	if joiner.innerTableReader.path == joiner.outerTableReader.path {
		colsMap := _mergeCols(_mergeCols([]int{0}, joiner.innerOn), joiner.outerOn)
		joiner.innerTableReader.columnResolver = newColumnResolver(colsMap)
		joiner.outerTableReader = joiner.innerTableReader
		joiner.optimized = true
		joiner.selfJoin = true
		return
	}
	innerStat, err := os.Stat(joiner.innerTableReader.path)
	if err != nil {
		panic(err)
	}
	outerStat, err := os.Stat(joiner.outerTableReader.path)
	if err != nil {
		panic(err)
	}
	// inner表用与build hashtable，越小越好
	if innerStat.Size() > outerStat.Size() {
		joiner.innerTableReader.columnResolver = newColumnResolver(_mergeCols([]int{0}, joiner.innerOn))
		joiner.outerTableReader.columnResolver = newColumnResolver(joiner.outerOn)
		joiner.innerTableReader, joiner.outerTableReader = joiner.outerTableReader, joiner.innerTableReader
		joiner.innerOn, joiner.outerOn = joiner.outerOn, joiner.innerOn
		joiner.optimized = true
	}
}

func (joiner *hashJoiner) join() uint64 {
	joiner.optimize()
	innerTable, hashtable, err := joiner.build()
	if err != nil {
		panic(err)
	}
	if joiner.selfJoin {
		return joiner.concurrentProbeSerialWithSum(hashtable, innerTable, innerTable)
	}
	return joiner.concurrentProbeStreamWithSum(hashtable, innerTable)
}

func newJoiner(ctx context.Context, innerTablePath, outerTablePath string, innerOffset, outerOffset []int) *hashJoiner {
	return &hashJoiner{
		ctx:              ctx,
		innerTableReader: newTableReader(ctx, innerTablePath, runtime.NumCPU()+1, 500),
		innerOn:          innerOffset,
		outerTableReader: newTableReader(ctx, outerTablePath, runtime.NumCPU()+1, 500),
		outerOn:          outerOffset,
	}
}
