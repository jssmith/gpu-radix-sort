package data

import (
	"io"
)

// Represents a partition of a DistribArray
type DistribPart interface {
	// Returns a reader that will return bytes from the partition in the given
	// contiguous range. End may be negative to index backwards from the end. A
	// zero end will read until the end of the partition.
	GetRangeReader(start, end int) (io.ReadCloser, error)

	// Returns a reader that will return bytes from the entire partition.
	GetReader() (io.ReadCloser, error)

	// Returns a writer that will append to the partition
	GetWriter() (io.WriteCloser, error)

	// Return the number of bytes currently in this partition
	Len() (int, error)
}

// Represents an array of bytes that is suitable for a distributed sort.
type DistribArray interface {
	GetParts() ([]DistribPart, error)
}

// A reference to an input partition
type PartRef struct {
	Arr     DistribArray // DistribArray to read from
	PartIdx int          // Partition to read from
	Start   int          // Offset to start reading
	NByte   int          // Number of bytes to read
}

// A generic interface to creating new DistribArrays, users must provide their
// own implementations of this.
type ArrayFactory func(name string, nbucket int) (DistribArray, error)
