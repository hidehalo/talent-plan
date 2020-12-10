.PHONY: all

all: test bench

test:
	go test

bench:
	go test -bench Benchmark -run xx -count 5 -benchmem

trace:
	go test -bench BenchmarkJoin -run xx -count 3 -benchmem --cpuprofile=cpu --memprofile=mem --blockprofile=block --trace=trace.out