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
	probeCols      []int
	sumTable       *tableReader
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
			v, err := strconv.ParseUint(string(worker.sumTable.getColumn(innerTable[id], 0)), 10, 64)
			if err != nil {
				panic("Join panic\n" + err.Error())
			}
			worker.sum += v
		}
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

func newProbeWorker(ctx context.Context, hashtable *mvmap.MVMap, innerTableData [][][]byte, outerTable *tableReader, probeCols []int) *probeWorker {
	return &probeWorker{
		ctx:            ctx,
		hashtable:      hashtable,
		innerTableData: innerTableData,
		outerTable:     outerTable,
		probeCols:      probeCols,
		sum:            uint64(0),
		quit:           make(chan struct{}),
	}
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
	probeWorkers := make([]*probeWorker, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		worker := newProbeWorker(joiner.ctx, hashtable, innerTable, joiner.outerTableReader, probeOffset)
		// 这个设计不是很好
		if joiner.swap {
			worker.sumTable = joiner.outerTableReader
		} else {
			worker.sumTable = joiner.innerTableReader
		}
		probeWorkers = append(probeWorkers, worker)
		go worker.run(chunkCh)
	}
	var sum uint64
	for _, w := range probeWorkers {
		sum += w.getSum()
	}
	return sum
}

func (joiner *hashJoiner) probeTblWithSum(hashtable *mvmap.MVMap, innerTable, outerTable [][][]byte, probeOffset []int) uint64 {
	// worker := &probeWorker{
	// 	ctx:        joiner.ctx,
	// 	outerTable: joiner.outerTableReader,
	// 	sum:        uint64(0),
	// }
	// if joiner.swap {
	// 	worker.sumTable = joiner.outerTableReader
	// } else {
	// 	worker.sumTable = joiner.innerTableReader
	// }
	// worker.probeWithSum(hashtable, innerTable, outerTable, probeOffset)
	// return worker.sum
	concurrency := runtime.NumCPU()
	probeWorkers := make([]*probeWorker, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		worker := newProbeWorker(joiner.ctx, hashtable, innerTable, joiner.outerTableReader, probeOffset)
		// 这个设计不是很好，原因是需要outerTableReader resolve column index, 也许可以再写一个resolver
		if joiner.swap {
			worker.sumTable = joiner.outerTableReader
		} else {
			worker.sumTable = joiner.innerTableReader
		}
		probeWorkers = append(probeWorkers, worker)
		go worker.runSerial(outerTable)
	}
	var sum uint64
	for _, w := range probeWorkers {
		sum += w.getSum()
	}
	return sum
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
