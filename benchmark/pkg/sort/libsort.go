package sort

// These are go wrappers for libsort so I don't have to
// repeat all the cgo nonsense everywhere.
// For full documentation, see libsort
//
// Because of Go's inflexible type system, everything must be in terms of
// []byte to avoid uneccesary copying and converting. By convention, 'len'
// refers to the number of uint32s and 'size' refers to the number of bytes.

// #cgo CFLAGS: -O3 -I../../../libsort --std=gnu99
// #cgo LDFLAGS: -L../../../libsort -lsort
// #include "libsort.h"
import "C"
import (
	"errors"
	"unsafe"
)

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

func GpuFull(in []byte) error {
	cints := (*C.uint32_t)(unsafe.Pointer(&in[0]))
	success, _ := C.providedGpu(cints, (C.size_t)(len(in)/4))
	if !success {
		return errors.New("libsort providedGpu failed\n")
	}

	return nil
}

// Interpret in as uint32s and sort by the radix of width bits starting at bit 'offset'
// boundaries will contain the byte offset of each radix group after sorting
func GpuPartial(in []byte, boundaries []int64, offset int, width int) error {
	boundaries32 := make([]uint32, len(boundaries))

	cints := (*C.uint32_t)(unsafe.Pointer(&in[0]))
	success, _ := C.gpuPartial(cints,
		(*C.uint32_t)(&boundaries32[0]),
		(C.size_t)(len(in)/4),
		(C.uint32_t)(offset),
		(C.uint32_t)(width),
	)

	for i := 0; i < len(boundaries32); i++ {
		boundaries[i] = (int64)(boundaries32[i]) * (int64)(4)
	}

	if !success {
		return errors.New("libsort gpuPartial failed\n")
	}

	return nil
}

// Generate 'len' uint32's and return the array as a byte slice (total bytes will be 4*len)
func GenerateInputs(len uint64) ([]byte, error) {
	arr := make([]byte, len*4)

	cints := (*C.uint32_t)(unsafe.Pointer(&arr[0]))
	C.populateInput(cints, (C.size_t)(len))

	return arr, nil
}
