package main

import "testing"

// var tbl0, tbl1 = readCSVFileIntoTbl("./t/r2.tbl"), readCSV("./t/r2.tbl")
// var h0, h1 = buildHashTable(tbl0, []int{0}), conBuild(tbl1, []int{0})

// func BenchmarkReadCSVIntoTbl(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		readCSVFileIntoTbl("./t/r2.tbl")
// 	}
// }

// func BenchmarkReadCSV(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		readCSV("./t/r2.tbl")
// 	}
// }

// func BenchmarkBuildHashTable(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		buildHashTable(tbl0, []int{0})
// 	}
// }

// func BenchmarkConBuild(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		workers := conBuild(tbl1, []int{0})
// 		for _, w := range workers {
// 			<-w.hashtable
// 		}
// 	}
// }

// func BenchmarkProbe(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		for j := 0; j < len(tbl0); j++ {
// 			probe(h0, tbl0[j], []int{1})
// 		}
// 	}
// }

// func BenchmarkSlashProbe(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		for j := 0; j < len(tbl0); j++ {
// 			_probe(h0, tbl1[j], []int{1})
// 		}
// 	}
// }

func BenchmarkJoinExample(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./t/r2.tbl", "./bt/r2.tbl", []int{0}, []int{1})
	}
}

func BenchmarkJoin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./t/r2.tbl", "./bt/r2.tbl", []int{0}, []int{1})
	}
}
