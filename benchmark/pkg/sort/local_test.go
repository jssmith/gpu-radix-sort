package sort

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func RandomInputs(len int) []uint32 {
	rand.Seed(0)
	out := make([]uint32, len)
	for i := 0; i < len; i++ {
		out[i] = rand.Uint32()
	}
	return out
}

func CheckSort(orig []uint32, new []uint32) error {
	if len(orig) != len(new) {
		return fmt.Errorf("Lengths do not match: Expected %v, Got %v\n", len(orig), len(new))
	}

	origCpy := make([]uint32, len(orig))
	copy(origCpy, orig)
	sort.Slice(origCpy, func(i, j int) bool { return origCpy[i] < origCpy[j] })
	for i := 0; i < len(orig); i++ {
		if origCpy[i] != new[i] {
			return fmt.Errorf("Response doesn't match reference at %v\n: Expected %v, Got %v\n", i, origCpy[i], new[i])
		}
	}
	return nil
}

func TestLocal(t *testing.T) {
	var err error

	test := RandomInputs(1024)

	ref := make([]uint32, len(test))
	copy(ref, test)

	if err = localSort(test); err != nil {
		t.Fatalf("Error while sorting: %v", err)
	}

	if err := CheckSort(ref, test); err != nil {
		t.Fatalf("Sorted Wrong: %v", err)
	}
}

// Test the Go wrapper for libsort.gpuPartial(), we don't go out of our way to
// test gpuPartial itself (we assume libsort is correct)
func TestLocalPartial(t *testing.T) {
	var err error

	size := 1024
	width := 4
	nbucket := 1 << width

	test := RandomInputs(size)
	boundaries := make([]uint32, nbucket)

	ref := make([]uint32, len(test))
	copy(ref, test)

	if err = localSortPartial(test, boundaries, 0, width); err != nil {
		t.Fatalf("Error while sorting: %v", err)
	}

	// Make sure the partial sort worked and set the boundaries correctly
	// Start at uint32_max to detect bucket 0 (will roll over when we increment)
	curBucket := ^(uint32)(0)
	for i := 0; i < size; i++ {
		bucket := test[i] & ((1 << width) - 1)
		if bucket != curBucket {
			if bucket != curBucket+1 {
				t.Fatalf("Buckets not in order: expected %v, got %v", curBucket+1, bucket)
			}
			if boundaries[bucket] != (uint32)(i) {
				t.Fatalf("Boundary for end of bucket %v is wrong: expected %v, got %v", curBucket, i, boundaries[bucket])
			}
			curBucket = bucket
		}
	}

	// Make sure all the right values are in the output (e.g. if
	// localSortPartial erroneously set everything to 0 the above check would
	// still pass)
	sort.Slice(ref, func(i, j int) bool { return ref[i] < ref[j] })
	sort.Slice(test, func(i, j int) bool { return test[i] < test[j] })
	for i := 0; i < size; i++ {
		if ref[i] != test[i] {
			t.Fatalf("Output does not contain the same values as the input")
		}
	}
}
