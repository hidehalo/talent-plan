package main

import "testing"

// func BenchmarkReadCSVIntoTbl(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		readCSVFileIntoTbl("./bt/r2.tbl")
// 	}
// }

// func BenchmarkReadCSV(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		readCSV("./bt/r2.tbl")
// 	}
// }

// func BenchmarkBuildHashTable(b *testing.B) {
// 	data := readCSVFileIntoTbl("./bt/r2.tbl")
// 	for i := 0; i < b.N; i++ {
// 		buildHashTable(data, []int{0})
// 	}
// }

// func BenchmarkBuild(b *testing.B) {
// 	data := readCSV("./bt/r2.tbl")
// 	for i := 0; i < b.N; i++ {
// 		build(data, []int{0})
// 	}
// }

func BenchmarkJoinExample(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./bt/r8.tbl", "./bt/r7.tbl", []int{0}, []int{1})
	}
}

func BenchmarkJoin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./bt/r8.tbl", "./bt/r7.tbl", []int{0}, []int{1})
	}
}
