package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"sync"
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
	return sum
}

type ParalleCSVReader struct {
	readChan    []chan []byte // Read file channel,line by line
	contents    [][][]byte    // Contents of file,split via EOL
	concurrency uint          // Concurrency
}

func NewParalleCSVReader(concurrency uint) *ParalleCSVReader {
	pcr := &ParalleCSVReader{
		readChan:    make([]chan []byte, concurrency),
		contents:    make([][][]byte, 0),
		concurrency: concurrency,
	}
	for i := uint(0); i < concurrency; i++ {
		pcr.readChan[i] = make(chan []byte, 500)
	}

	return pcr
}

func (pcr *ParalleCSVReader) ReadCSV(filepath string) {
	csvFile, _ := os.Open(filepath)
	reader := bufio.NewReader(csvFile)
	var wg sync.WaitGroup

	for i := uint(0); i < pcr.concurrency; i++ {
		go func(id uint) {
			for record := range pcr.readChan[id] {
				recordBytes := bytes.Split([]byte{','}, record)
				pcr.contents = append(pcr.contents, recordBytes)
				wg.Done()
			}
		}(i)
	}

	c := uint(0)
	for {
		c %= pcr.concurrency
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		wg.Add(1)
		pcr.readChan[c] <- line
		c++
	}

	wg.Wait()
}

// func buildHashTable(data [][]string, offset []int) (hashtable *mvmap.MVMap) {

// }

// func probe(hashtable *mvmap.MVMap, row []string, offset []int) (rowIDs []int64) {

// }
