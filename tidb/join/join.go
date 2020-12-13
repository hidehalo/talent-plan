package main

import (
	"context"
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

type probeWorker struct {
	ctx            context.Context
	hashtable      *mvmap.MVMap
	innerTableData [][][]byte
	outerTable     *tableReader
	resolver       *columnResolver
	probeCols      []int
	sum            uint64
	quit           chan struct{}
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
			v, err := strconv.ParseUint(string(worker.resolver.resolve(innerTable[id], 0)), 10, 64)
			if err != nil {
				panic("Join panic\n" + err.Error())
			}
			worker.sum += v
		}

		// v, err := strconv.ParseUint(string(worker.sumTable.getColumn(row, 0)), 10, 64)
		// if err != nil {
		// 	panic("Join panic\n" + err.Error())
		// }
		// worker.sum += uint64(len(rowIDs)) * v

	}
}

func (worker *probeWorker) run(chunkCh chan *chunk) {
	defer worker.close()
	for {
		select {
		case <-worker.ctx.Done():
			return
		case chunk := <-chunkCh:
			if chunk == nil {
				return
			}
			worker.probeWithSum(worker.hashtable, worker.innerTableData, chunk.data, worker.probeCols)
			worker.outerTable.releaseChunk(chunk)
		}
	}
}

func (worker *probeWorker) runSerial(outerTableData [][][]byte) {
	defer worker.close()
	select {
	case <-worker.ctx.Done():
		return
	default:
		worker.probeWithSum(worker.hashtable, worker.innerTableData, outerTableData, worker.probeCols)
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

func newProbeWorker(ctx context.Context, hashtable *mvmap.MVMap, innerTableData [][][]byte, resolver *columnResolver, probeCols []int) *probeWorker {
	return &probeWorker{
		ctx:            ctx,
		hashtable:      hashtable,
		innerTableData: innerTableData,
		resolver:       resolver,
		probeCols:      probeCols,
		sum:            uint64(0),
		quit:           make(chan struct{}),
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

// 探测关系元组并返回col0求和结果
func (joiner *hashJoiner) probeStreamWithSum(hashtable *mvmap.MVMap, innerTable [][][]byte) uint64 {
	chunkCh := joiner.outerTableReader.read()
	concurrency := runtime.NumCPU()
	probeWorkers := make([]*probeWorker, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		var resolver *columnResolver
		if joiner.optimized {
			resolver = joiner.outerTableReader.columnResolver
		} else {
			resolver = joiner.innerTableReader.columnResolver
		}
		worker := newProbeWorker(joiner.ctx, hashtable, innerTable, resolver, joiner.outerOn)
		probeWorkers = append(probeWorkers, worker)
		go worker.run(chunkCh)
	}
	var sum uint64
	for _, w := range probeWorkers {
		sum += w.getSum()
	}
	return sum
}

func (joiner *hashJoiner) probeTblWithSum(hashtable *mvmap.MVMap, innerTable, outerTable [][][]byte) uint64 {
	concurrency := runtime.NumCPU()
	probeWorkers := make([]*probeWorker, 0, concurrency)
	chunkSize := len(outerTable) / concurrency
	for i := 0; i < concurrency; i++ {
		var resolver *columnResolver
		if joiner.selfJoin {
			resolver = joiner.innerTableReader.columnResolver
		} else if joiner.optimized {
			resolver = joiner.outerTableReader.columnResolver
		} else {
			resolver = joiner.innerTableReader.columnResolver
		}
		worker := newProbeWorker(joiner.ctx, hashtable, innerTable, resolver, joiner.outerOn)
		probeWorkers = append(probeWorkers, worker)
		start := i * chunkSize
		end := start + chunkSize
		if end > len(outerTable) {
			end = len(outerTable)
		}
		go worker.runSerial(outerTable[start:end])
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
				for _, off := range joiner.innerOn {
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

func (joiner *hashJoiner) optimize() {
	if joiner.innerTableReader.path == joiner.outerTableReader.path {
		colsMap := _mergeCols(_mergeCols([]int{0}, joiner.innerOn), joiner.outerOn)
		joiner.innerTableReader.columnResolver = newColumnResolver(colsMap)
		joiner.outerTableReader = nil
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

func (joiner *hashJoiner) joinSelf() uint64 {
	innerTable, hashtable, err := joiner.build()
	if err != nil {
		panic(err)
	}
	return joiner.probeTblWithSum(hashtable, innerTable, innerTable)
}

func (joiner *hashJoiner) join() uint64 {
	joiner.optimize()
	if joiner.selfJoin {
		return joiner.joinSelf()
	}
	innerTable, hashtable, err := joiner.build()
	if err != nil {
		panic(err)
	}
	return joiner.probeStreamWithSum(hashtable, innerTable)
}

func newJoiner(ctx context.Context, innerTablePath, outerTablePath string, innerOffset, outerOffset []int) *hashJoiner {
	return &hashJoiner{
		ctx:              ctx,
		innerTableReader: newTableReader(ctx, innerTablePath, 1, runtime.NumCPU()*1000),
		innerOn:          innerOffset,
		outerTableReader: newTableReader(ctx, outerTablePath, runtime.NumCPU(), 1000),
		outerOn:          outerOffset,
	}
}
