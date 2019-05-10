package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"
)

var cpuprofile = flag.String("cpu", "", "write cpu profile to `file`")
var memprofile = flag.String("mem", "", "write memory profile to `file`")
var tracefile = flag.String("trace", "", "write trace to `file`")

var threshold = 1 << 11
var poolSize = 1

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
func MergeSort(src []int64) {
	len := len(src)
	if len > 1 {
		if len <= threshold {
			// Sequential
			mergesortNormal(src)
		} else {
			// Parallel
			middle := len / 2
			var wg sync.WaitGroup
			wg.Add(poolSize)
			go func() {
				defer wg.Done()
				MergeSort(src[:middle])
			}()
			if poolSize == 2 {
				go func() {
					defer wg.Done()
					MergeSort(src[middle:])
				}()
			} else {
				MergeSort(src[middle:])
			}
			wg.Wait()
			copy(src, merge(src[:middle], src[middle:]))
		}
	}
}

func merge(left []int64, right []int64) []int64 {
	i, j := 0, 0
	outPlace := make([]int64, 0, len(left)+len(right))
	for i < len(left) && j < len(right) {
		if left[i] < right[j] {
			outPlace = append(outPlace, left[i])
			i++
		} else {
			outPlace = append(outPlace, right[j])
			j++
		}
	}
	for i < len(left) {
		outPlace = append(outPlace, left[i])
		i++
	}
	for j < len(right) {
		outPlace = append(outPlace, right[j])
		j++
	}
	return outPlace
}

func mergesortNormal(src []int64) {
	if len(src) > 1 {
		middle := len(src) / 2
		mergesortNormal(src[:middle])
		mergesortNormal(src[middle:])
		copy(src, merge(src[:middle], src[middle:]))
	}
}

func internalPrepare(src []int64) {
	rand.Seed(time.Now().Unix())
	for i := range src {
		src[i] = rand.Int63()
	}
}

func main() {
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
	if *tracefile != "" {
		f, err := os.Create(*tracefile)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		err = trace.Start(f)
		if err != nil {
			panic(err)
		}
		defer trace.Stop()
	}

	src := make([]int64, 1<<20)
	internalPrepare(src)

	MergeSort(src)
}
