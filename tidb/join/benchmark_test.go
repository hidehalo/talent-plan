package main

import "testing"

// 小表连接大表
func BenchmarkExample_R2R0(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./t/r2.tbl", "./t/r0.tbl", []int{0}, []int{1})
	}
}

// 小表连接大表
func BenchmarkJoin_R2R0(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./t/r2.tbl", "./t/r0.tbl", []int{0}, []int{1})
	}
}

// 大表连接小表
func BenchmarkExample_R0R2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./t/r0.tbl", "./t/r2.tbl", []int{0}, []int{1})
	}
}

// 大表连接小表
func BenchmarkJoin_R0R2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./t/r0.tbl", "./t/r2.tbl", []int{0}, []int{1})
	}
}

// 自连接
func BenchmarkExample_R0R0(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./t/r0.tbl", "./t/r0.tbl", []int{0}, []int{0})
	}
}

// 自连接
func BenchmarkJoin_R0R0(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./t/r0.tbl", "./t/r0.tbl", []int{0}, []int{0})
	}
}
