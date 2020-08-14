package sort

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocal(t *testing.T) {
	var err error

	err = InitLibSort()
	require.Nil(t, err, "Failed to initialize libsort")

	test, err := GenerateInputs((uint64)(4099))
	require.Nil(t, err, "Failed to generate inputs")

	ref := make([]byte, len(test))
	copy(ref, test)

	if err = GpuFull(test); err != nil {
		t.Fatalf("Error while sorting: %v", err)
	}

	if err := CheckSort(ref, test); err != nil {
		t.Fatalf("Sorted Wrong: %v", err)
	}
}

func TestParallel(t *testing.T) {
	var err error
	nparallel := 16

	err = InitLibSort()
	require.Nil(t, err, "Failed to initialize libsort")

	t.Run("Complete Sort", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(nparallel)
		for i := 0; i < nparallel; i++ {
			go func() {
				TestLocal(t)
				wg.Done()
			}()
		}

		wgChan := make(chan struct{})
		go func() {
			defer close(wgChan)
			wg.Wait()
		}()

		select {
		case <-wgChan:
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout")
		}
	})

	t.Run("Partial Sort", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(nparallel)
		for i := 0; i < nparallel; i++ {
			go func() {
				defer wg.Done()
				TestLocalPartial(t)
			}()
		}
		wgChan := make(chan struct{})
		go func() {
			defer close(wgChan)
			wg.Wait()
		}()

		select {
		case <-wgChan:
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout")
		}
	})

}

// test the go wrapper for libsort.gpupartial(), we don't go out of our way to
// test gpupartial itself (we assume libsort is correct)
func TestLocalPartial(t *testing.T) {
	var err error

	err = InitLibSort()
	require.Nil(t, err, "failed to initialize libsort")

	tLen := 1021
	// tLen := 1024 * 1024
	width := 8
	nbucket := 1 << width

	test, err := GenerateInputs((uint64)(tLen))
	require.Nil(t, err, "failed to generate test inputs")

	boundaries := make([]int64, nbucket)

	ref := make([]byte, len(test))
	copy(ref, test)

	err = GpuPartial(test, boundaries, 0, width)
	require.Nil(t, err, "error while sorting")

	checkPartial(t, test, boundaries, ref)
}

func TestGenerate(t *testing.T) {
	// this has to big enough for the law of large numbers to kick in
	tLen := 1024 * 1024

	tdat, err := GenerateInputs((uint64)(tLen))
	require.Nil(t, err, "failed to generate inputs")

	tints := make([]uint32, tLen)
	err = binary.Read(bytes.NewReader(tdat), binary.LittleEndian, tints)
	require.Nil(t, err, "Couldn't interpret generated data")

	min := (uint32)(math.MaxUint32)
	max := (uint32)(0)
	sum := (uint64)(0)
	for _, v := range tints {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}

		sum += (uint64)(v)
	}
	mean := sum / (uint64)(len(tints))

	q25 := (uint64)(math.MaxUint32 / 4)
	q75 := (uint64)(3 * (math.MaxUint32 / 4))

	// these checks are super forgiving, we just want to make sure nothing
	// stupid happened
	t.Logf("mean is: %v", mean)
	require.Greater(t, mean, q25)
	require.Less(t, mean, q75)

	t.Logf("min is: %v", min)
	t.Logf("max is: %v", max)
	require.Less(t, (uint64)(min), q25)
	require.Greater(t, (uint64)(max), q75)
}
