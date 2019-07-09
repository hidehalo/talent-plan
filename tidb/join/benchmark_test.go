package main

import "testing"

// func BenchmarkJoin(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		Join("./t/r0.tbl", "./t/r2.tbl", []int{0}, []int{1})
// 	}
// }

// func BenchmarkJoinExample(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		JoinExample("./t/r0.tbl", "./t/r2.tbl", []int{0}, []int{1})
// 	}
// }

func BenchmarkReadCSV(b *testing.B) {
	for i := 0; i < b.N; i++ {
		readCSVFileIntoTbl("./t/r0.tbl")
	}
}

func BenchmarkParalleReadCSV(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ReadCSV("./t/r0.tbl")
	}
}
