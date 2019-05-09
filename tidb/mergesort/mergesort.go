package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
func MergeSort(src []int64) {
	len := len(src)
	if len > 1 {
		if len <= 2048 {
			// Sequential
			mergesort(src)
		} else {
			// Parallel
			middle := len / 2

			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				defer wg.Done()
				MergeSort(src[:middle])
			}()

			MergeSort(src[middle:])

			wg.Wait()
			copy(src, merge(src[:middle], src[middle:]))
		}
	}
}

func merge(left []int64, right []int64) []int64 {
	i, j := 0, 0
	sLeft, sRight := len(left), len(right)
	outPlace := make([]int64, 0, sLeft+sRight)
	for i < sLeft && j < sRight {
		if left[i] < right[j] {
			outPlace = append(outPlace, left[i])
			i++
		} else {
			outPlace = append(outPlace, right[j])
			j++
		}
	}
	for i < sLeft {
		outPlace = append(outPlace, left[i])
		i++
	}
	for j < sRight {
		outPlace = append(outPlace, right[j])
		j++
	}
	return outPlace
}

func mergesort(src []int64) {
	if len(src) > 1 {
		middle := len(src) / 2
		mergesort(src[:middle])
		mergesort(src[middle:])
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
	// ... rest of the program ...
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
	src := make([]int64, 1<<20)
	internalPrepare(src)
	MergeSort(src)
}
