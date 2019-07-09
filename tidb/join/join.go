package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
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

func ReadCSV(filepath string) []*[][]byte {
	contents := make([]*[][]byte, 0)
	csvFile, _ := os.Open(filepath)
	reader := bufio.NewReader(csvFile)
	comma := []byte{','}

	for {
		line, _, err := reader.ReadLine()
		// if line[len(line)-1] == '\n' {
		// 	tmp := bytes.Split(line[:len(line)-1], comma)
		// 	contents = append(contents, &tmp)
		// } else {
		// 	tmp := bytes.Split(line, comma)
		// 	contents = append(contents, &tmp)
		// }
		if err == io.EOF {
			break
		}
		tmp := bytes.Split(line, comma)
		contents = append(contents, &tmp)
	}

	return contents
}

// func buildHashTable(data [][]string, offset []int) (hashtable *mvmap.MVMap) {

// }

// func probe(hashtable *mvmap.MVMap, row []string, offset []int) (rowIDs []int64) {

// }
