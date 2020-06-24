package main

// These are mostly convenient go wrappers for libsort so I don't have to
// repeat all the cgo nonsense everywhere

// #cgo CFLAGS: -O3 -I../libsort --std=gnu99
// #cgo LDFLAGS: -L../libsort -lsort
// #include "libsort.h"
import "C"
import "errors"

// Sort in in-place using only process-local resources (no distribution or
// external storage). Uses libsort.
func localSort(in []uint32) error {
	success, _ := C.providedGpu((*C.uint32_t)(&in[0]), (C.size_t)(len(in)))
	if !success {
		return errors.New("libsort providedGpu failed\n")
	}

	return nil
}

// Sort in in-place using only process-local resources (no distribution or
// external storage). Uses libsort.
func localSortPartial(in []uint32, boundaries []uint32, offset int, width int) error {
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
