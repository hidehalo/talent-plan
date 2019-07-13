package main

import "testing"

// 小表连接小表
func BenchmarkJoinExample1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./t/r2.tbl", "./t/r2.tbl", []int{0}, []int{1})
	}
}

// 小表连接小表
func BenchmarkJoin1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./t/r2.tbl", "./t/r2.tbl", []int{0}, []int{1})
	}
}

// 小表连接大表
func BenchmarkJoinExample2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./t/r2.tbl", "./t/r1.tbl", []int{0}, []int{1})
	}
}

// 小表连接大表
func BenchmarkJoin2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./t/r2.tbl", "./t/r1.tbl", []int{0}, []int{1})
	}
}

// 大表连接小表
func BenchmarkJoinExample3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./t/r1.tbl", "./t/r2.tbl", []int{0}, []int{1})
	}
}

// 大表连接小表
func BenchmarkJoin3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./t/r1.tbl", "./t/r2.tbl", []int{0}, []int{1})
	}
}

// 大表连接小表
func BenchmarkJoinExample4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./t/r1.tbl", "./t/r2.tbl", []int{0}, []int{1})
	}
}

// 大表连接小表
func BenchmarkJoin4(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./t/r1.tbl", "./t/r2.tbl", []int{0}, []int{1})
	}
}

// 自连接
func BenchmarkJoinExample5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./t/r0.tbl", "./t/r0.tbl", []int{0}, []int{0})
	}
}

// 自连接
func BenchmarkJoin5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./t/r0.tbl", "./t/r0.tbl", []int{0}, []int{0})
	}
}

// func BenchmarkForTrace(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		Join("./bt/r8.tbl", "./bt/r7.tbl", []int{0}, []int{1})
// 	}
// }
