package main

import "context"

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
	ctx := context.Background()
	joiner := newJoiner(ctx, f0, f1, offset0, offset1)
	sum = joiner.join()
	return
}

// go test -bench Benchmark -run xx -count 1 -benchmem
// goos: darwin
// goarch: amd64
// pkg: join
// BenchmarkExample_R_R_-4   	   40974	     28654 ns/op	   11408 B/op	      18 allocs/op
// BenchmarkJoin_R_R_-4      	   15584	     77969 ns/op	  166006 B/op	     109 allocs/op
// BenchmarkExample_R2R0-4   	     385	   3048995 ns/op	 1769033 B/op	   31112 allocs/op
// BenchmarkJoin_R2R0-4      	     499	   2261830 ns/op	 1070473 B/op	   31165 allocs/op
// BenchmarkExample_R0R2-4   	     261	   4158887 ns/op	 3677037 B/op	   21899 allocs/op
// BenchmarkJoin_R0R2-4      	     516	   2256378 ns/op	 1153943 B/op	   41199 allocs/op
// BenchmarkExample_R0R0-4   	     132	   8776079 ns/op	 5622107 B/op	   70374 allocs/op
// BenchmarkJoin_R0R0-4      	     312	   3797139 ns/op	 4465205 B/op	   60426 allocs/op
// PASS
// ok  	join	14.292s

// chunk的缓存命中比较差
// IO没办法优化了
// 空转开销有点高
// build phase使用的数据结构不是线程安全的
