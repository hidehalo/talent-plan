.PHONY: all

all: test_example test_homework cleanup gendata

test_example:
	go test -v -run=TestExampleURLTop

pprof_test_example:
	go test -timeout=30m --trace=trace.out --cpuprofile=cpu.out --blockprofile=block.out -v -run=TestExampleURLTop

test_homework:
	go test -v -run=TestURLTop

cleanup:
	go test -v -run=TestCleanData

gendata:
	go test -v -run=TestGenData
