package main

import "sync"
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
			merge(src[:middle], src[middle:])
		}
	}
}

func merge(left []int64, right []int64) {
	

}


func mergesort(src []int64) {
	if len(src) > 1 {
		middle := len(src) / 2
		mergesort(src[:middle])
		mergesort(src[middle:])
		merge(src[:middle], src[middle:])
	}
}
