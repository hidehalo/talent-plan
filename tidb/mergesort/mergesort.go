package main

import (
	"sync"
	// "fmt"
)
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
			copyi64arr(src, merge(src[:middle], src[middle:]))
		}
	}
}

func merge(left []int64, right []int64)  []int64 {
	i,j := 0,0
	sLeft,sRight := len(left),len(right)
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
		copyi64arr(src, merge(src[:middle], src[middle:]))
	}
}

func copyi64arr(a []int64, b []int64) {
	for i,v := range b {
		a[i] = v
	}
}

// func main() {
// 	src := []int64{11,10,9,8,7,6,5,4,3,2,1}
// 	MergeSort(src)
// 	fmt.Println(src)
// }