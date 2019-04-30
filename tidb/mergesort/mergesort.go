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
			merge(src, middle)
		}
	}
}

func merge(src []int64, middle int, maxele int64) {
	size := len(src)
	i := 0
	j := middle + 1
	k := 0
	
    for i <= middle && j < size {
        if (src[i] % maxele <= src[j] % maxele) { 
            src[k] = src[k] + (src[i] % maxele) * maxele; 
            k++; 
            i++; 
        } else { 
            src[k] = src[k] + (src[j] % maxele) * maxele; 
            k++; 
            j++; 
        } 
    } 
    for i <= middle { 
        src[k] = src[k] + (src[i] % maxele) * maxele; 
        k++; 
        i++; 
    } 
    for j < size { 
        src[k] = src[k] + (src[j] % maxele) * maxele; 
        k++; 
        j++; 
    } 
  
    // Obtaining actual values 
    for i := 0; i < size; i++ {
        src[i] = src[i] / maxele; 
	}
}


func mergesort(src []int64) {
	if len(src) > 1 {
		middle := len(src) / 2
		mergesort(src[:middle])
		mergesort(src[middle:])
		merge(src, middle)
	}
}
