.PHONY: all

all: test_example test_homework cleanup gendata

test_example:
	go test -timeout=120m -v -run=TestExampleURLTop

pprof_test_example:
	go test -timeout=120m --trace=trace.out --cpuprofile=cpu.out --blockprofile=block.out -v -run=TestExampleURLTop

test_homework:
	go test -timeout=120m -v -run=TestURLTop

cleanup:
	go test -v -run=TestCleanData

gendata:
	go test -v -run=TestGenData
