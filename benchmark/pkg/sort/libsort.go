package sort

// These are go wrappers for libsort so I don't have to
// repeat all the cgo nonsense everywhere.
// For full documentation, see libsort

// #cgo CFLAGS: -O3 -I../../../libsort --std=gnu99
// #cgo LDFLAGS: -L../../../libsort -lsort
// #include "libsort.h"
import "C"
import "errors"

// Perform one-time initialization of libsort, this must be called at least once
// per process (calls after the first do nothing)
var libSortInitialized bool = false

func InitLibSort() error {
	if !libSortInitialized {
		success, _ := C.initLibSort()
		if !success {
			return errors.New("Failed to initialize libsort")
		}
		libSortInitialized = true
	}
	return nil
}

func GpuFull(in []uint32) error {
	success, _ := C.providedGpu((*C.uint32_t)(&in[0]), (C.size_t)(len(in)))
	if !success {
		return errors.New("libsort providedGpu failed\n")
	}

	return nil
}

func GpuPartial(in []uint32, boundaries []uint32, offset int, width int) error {
	success, _ := C.gpuPartial((*C.uint32_t)(&in[0]),
		(*C.uint32_t)(&boundaries[0]),
		(C.size_t)(len(in)),
		(C.uint32_t)(offset),
		(C.uint32_t)(width),
	)

	if !success {
		return errors.New("libsort gpuPartial failed\n")
	}

	return nil
}

func GenerateInputs(size uint64) ([]uint32, error) {
	arr := make([]uint32, size)

	C.populateInput((*C.uint32_t)(&arr[0]), (C.size_t)(size))

	return arr, nil
}
