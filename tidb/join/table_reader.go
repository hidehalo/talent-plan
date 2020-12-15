package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
)

type columnResolver struct {
	colsMap []int
}

func (resolver *columnResolver) resolve(row [][]byte, colIdx int) []byte {
	if resolver.colsMap == nil {
		return row[colIdx]
	}
	find := -1
	for i := 0; i < len(resolver.colsMap); i++ {
		if resolver.colsMap[i] == colIdx {
			find = i
			break
		}
	}
	if find == -1 || find >= len(row) {
		panic(fmt.Sprintf("Can't find column[%d] in map %v", colIdx, resolver.colsMap))
	}
	return row[find]
}

func newColumnResolver(colsMap []int) *columnResolver {
	return &columnResolver{colsMap: colsMap}
}

type tableReader struct {
	ctx            context.Context
	close          context.CancelFunc
	path           string
	columnResolver *columnResolver
	chunkIn        chan *chunk
	chunkOut       chan *chunk
}

func newTableReader(ctx context.Context, path string, chunkNum, chunkCap int) *tableReader {
	tableReader := &tableReader{path: path}
	tableReader.ctx, tableReader.close = context.WithCancel(ctx)
	tableReader.columnResolver = newColumnResolver(nil)
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
		// 不知道为什么bufio#Reader.ReadBytes('\n')会把'\n'给读进来...
		if rawRow[len(rawRow)-1] == '\n' {
			rawRow = rawRow[:len(rawRow)-1]
		}
		row := bytes.Split(rawRow, []byte{sep})
		if tableReader.columnResolver.colsMap != nil {
			for i := 0; i < len(row); i++ {
				for newIdx, oldIdx := range tableReader.columnResolver.colsMap {
					if newIdx != oldIdx {
						row[newIdx], row[oldIdx] = row[oldIdx], row[newIdx]
					}
				}
				row = row[:len(tableReader.columnResolver.colsMap)]
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
			line, err := reader.ReadBytes('\n')
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
	return <-tableReader.chunkIn
}

func (tableReader *tableReader) releaseChunk(chunk *chunk) {
	chunk.reset()
	tableReader.chunkIn <- chunk
}

func (tableReader *tableReader) getColumn(row [][]byte, colIdx int) []byte {
	return tableReader.columnResolver.resolve(row, colIdx)
}
