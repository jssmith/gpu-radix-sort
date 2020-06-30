package sort

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func checkPartial(t *testing.T, test []uint32, boundaries []uint32, orig []uint32) {
	// Make sure the partial sort worked and set the boundaries correctly
	// Start at uint32_max to detect bucket 0 (will roll over when we increment)

	// len(boundaries) is 2^radixWidth, -1 gives us ones for the first width bits
	mask := (uint32)(len(boundaries) - 1)

	require.Equal(t, len(orig), len(test), "Test array has the wrong length")
	size := len(test)

	curBucket := ^(uint32)(0)
	for i := 0; i < size; i++ {
		bucket := test[i] & mask
		if bucket != curBucket {
			require.Equal(t, curBucket+1, bucket, "Buckets not in order")
			require.Equalf(t, boundaries[bucket], (uint32)(i), "Boundary for end of bucket %v is wrong", i)

			curBucket = bucket
		}
	}

	// Make sure all the right values are in the output, the sort here is just
	// to compare set membership.
	sort.Slice(orig, func(i, j int) bool { return orig[i] < orig[j] })
	sort.Slice(test, func(i, j int) bool { return test[i] < test[j] })
	for i := 0; i < size; i++ {
		require.Equal(t, orig[i], test[i], "Output does not contain all the same values as the input")
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

	checkPartial(t, test, boundaries, ref)
}