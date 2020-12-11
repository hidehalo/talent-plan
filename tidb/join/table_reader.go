package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
)

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
