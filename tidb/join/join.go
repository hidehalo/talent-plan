package main

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
	pcr := new(ParalleCSVReader)
	panic(pcr)
	return sum
}

type ParalleCSVReader struct {
	readChann []byte	// Read file channel,line by line
	contents [][]byte	// Contents of file,split via EOL
	concurrency uint	// Concurrency
	scaner bufio.Scanner
	wg sync.WaitGroup
}

func (pcr *ParalleCSVReader) readCSV(filepath string) {
	for i:=0;i<pcr.concurrency;i++ {
		pcr.wg.Add(1)
		go func () {
			defer pcr.wg.Done()
			pcr.contents = append(pcr.contents, <-pcr.readChann)
		}
	}
	for pcr.scaner.Scan() {
		pcr.readChann <- pcr.scaner.Bytes()
	}
	pcr.wg.Wait();
}

func buildHashTable(data [][]string, offset []int) (hashtable *mvmap.MVMap) {

}

func probe(hashtable *mvmap.MVMap, row []string, offset []int) (rowIDs []int64) {

}
