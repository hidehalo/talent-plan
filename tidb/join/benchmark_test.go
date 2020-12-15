package main

import "testing"

var DIR = "t"

// 空表连接，性能极限测试
func BenchmarkExample_R_R_(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./"+DIR+"/r_.tbl", "./"+DIR+"/r_.tbl", []int{0}, []int{0})
	}
}

// 空表连接，性能极限测试
func BenchmarkJoin_R_R_(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./"+DIR+"/r_.tbl", "./"+DIR+"/r_.tbl", []int{0}, []int{0})
	}
}

// 小表连接大表
func BenchmarkExample_R2R0(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./"+DIR+"/r2.tbl", "./"+DIR+"/r0.tbl", []int{0}, []int{1})
	}
}

// 小表连接大表
func BenchmarkJoin_R2R0(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./"+DIR+"/r2.tbl", "./"+DIR+"/r0.tbl", []int{0}, []int{1})
	}
}

// 大表连接小表
func BenchmarkExample_R0R2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./"+DIR+"/r0.tbl", "./"+DIR+"/r2.tbl", []int{0}, []int{1})
	}
}

// 大表连接小表
func BenchmarkJoin_R0R2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./"+DIR+"/r0.tbl", "./"+DIR+"/r2.tbl", []int{0}, []int{1})
	}
}

// 自连接
func BenchmarkExample_R0R0(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JoinExample("./"+DIR+"/r0.tbl", "./"+DIR+"/r0.tbl", []int{0}, []int{0})
	}
}

// 自连接
func BenchmarkJoin_R0R0(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join("./"+DIR+"/r0.tbl", "./"+DIR+"/r0.tbl", []int{0}, []int{0})
	}
}
