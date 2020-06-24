package data

import (
	"io"
)

// Represents a partition of a DistribArray
type DistribPart interface {
	// Returns a reader that will return bytes from the partition in the given
	// contiguous range. End may be negative to index backwards from the end. A
	// zero end will read until the end of the partition.
	GetRangeReader(start, end int64) io.ReadCloser

	// Returns a reader that will return bytes from the entire partition.
	GetReader() io.ReadCloser

	// Returns a writer that will append to the partition
	GetWriter() io.WriteCloser

	// Return the number of bytes currently in this partition
	Len() int64
}

// Represents an array of bytes that is suitable for a distributed sort.
type DistribArray interface {
	GetParts() ([]DistribPart, error)
}
