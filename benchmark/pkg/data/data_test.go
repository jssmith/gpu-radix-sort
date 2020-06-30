package data

import (
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func checkPart(part DistribPart, expected []uint32) error {
	var err error

	partReader, err := part.GetReader()
	if err != nil {
		return err
	}

	retInts := make([]uint32, len(expected))
	err = binary.Read(partReader, binary.LittleEndian, retInts)
	if err != nil {
		return errors.Wrap(err, "Failed to read from partition")
	}
	partReader.Close()

	if len(retInts) != len(expected) {
		return fmt.Errorf("Read wrong number of values: expected %v, got %v", len(expected), len(retInts))
	}

	for i := 0; i < len(expected); i++ {
		if retInts[i] != expected[i] {
			return fmt.Errorf("Partition returned wrong value at %v: expected %v, got %v", i, expected[i], retInts[i])
		}
	}
	return nil
}

func writePart(t *testing.T, part DistribPart, src []byte) {
	partWriter, err := part.GetWriter()
	require.Nilf(t, err, "Failed to get writer: %v", err)

	n, err := partWriter.Write(src)
	require.Nilf(t, err, "Writing failed: %v", err)
	require.Equalf(t, n, len(src), "Failed to write all bytes but didn't receive error")

	err = partWriter.Close()
	require.Nilf(t, err, "Closing partWriter failed: %v", err)
}

func readPart(t *testing.T, part DistribPart, dst []byte) {
	var err error
	partReader, err := part.GetReader()
	require.Nil(t, err, "Failed to get reader for partition")

	ntotal := 0
	for ntotal < len(dst) {
		n, err := partReader.Read(dst[ntotal:])
		ntotal += n
		if err == io.EOF {
			require.Equalf(t, len(dst), ntotal, "Didn't read enough bytes")
			break
		}
		require.Nil(t, err, "Error returned after reading %v bytes: %v", ntotal, err)
		require.NotZerof(t, n, "Reader didn't return any data")
	}
	require.Equalf(t, len(dst), ntotal, "Read the wrong number of bytes")

	// ReadCloser must return io.EOF either with the last bytes or with a zero
	// length read after the last bytes
	if err != io.EOF {
		overflow := make([]byte, 1)
		n, err := partReader.Read(overflow)
		require.Zerof(t, n, "Read extra bytes")
		require.Equalf(t, err, io.EOF, "partReader failed to return io.EOF")
	}
	err = partReader.Close()
	require.Nilf(t, err, "Failed to close partReader: %v", err)
}

// The simplest test for DistribArray writes raw bytes and uses only
// DistribArray features (no binary decoding or anything)
func testDistribArrBytes(t *testing.T, arr DistribArray, npart int, partLen int) {
	parts, err := arr.GetParts()
	require.Nilf(t, err, "Failed to get partitions: %v", err)

	rawParts := make([]([]byte), npart)
	for partIdx := 0; partIdx < npart; partIdx++ {
		t.Logf("Processing partition %v\n", partIdx)
		part := parts[partIdx]

		// Generate Inputs
		rawParts[partIdx] = make([]byte, partLen)
		for i := 0; i < partLen; i++ {
			rawParts[partIdx][i] = (byte)(((partIdx * partLen) + i) % 256)
		}

		// Write to partition
		writePart(t, part, rawParts[partIdx])

		// Read Back from Partition
		retBytes := make([]byte, partLen)
		readPart(t, part, retBytes)

		// Validate
		for i := 0; i < partLen; i++ {
			require.Equal(t, retBytes[i], rawParts[partIdx][i],
				"Returned bytes don't match at index %v: expected %v, got %v", i, rawParts[partIdx][i])
		}
	}

	// Make sure we get the same parts back on subsequent calls
	parts, err = arr.GetParts()
	require.Nilf(t, err, "Failed to re-acquire partitions")
	for partIdx := 0; partIdx < npart; partIdx++ {
		t.Logf("Re-reading partition %v", partIdx)
		// Read Back from Partition
		retBytes := make([]byte, partLen)
		readPart(t, parts[partIdx], retBytes)

		// Validate
		for i := 0; i < partLen; i++ {
			require.Equal(t, retBytes[i], rawParts[partIdx][i],
				"Returned bytes don't match at index %v: expected %v, got %v", i, rawParts[partIdx][i])
		}
	}
}

// Test the distributed array in a more complex test case using uint32s, this
// is how it will be used during sorting
func testDistribArrUints(t *testing.T, arr DistribArray, npart int, partLen int) {
	parts, err := arr.GetParts()
	require.Nilf(t, err, "Failed to get partitions: %v", err)

	// Give each partition a unique set of data (just incrementing counter)
	rawParts := make([][]uint32, npart)
	for partIdx := 0; partIdx < npart; partIdx++ {
		rawParts[partIdx] = make([]uint32, partLen)
		// Give each partition a unique set of data (just incrementing counter)
		for i := 0; i < partLen; i++ {
			rawParts[partIdx][i] = (uint32)((partIdx * partLen) + i)
		}
	}

	for partIdx, part := range parts {
		partWriter, err := part.GetWriter()
		require.Nilf(t, err, "Failed to get writer for partition %v", partIdx)

		err = binary.Write(partWriter, binary.LittleEndian, rawParts[partIdx])
		require.Nilf(t, err, "Couldn't write to partition %v: %v", partIdx, err)

		partWriter.Close()
	}

	// Check partitions from same partition object
	for partIdx, part := range parts {
		err = checkPart(part, rawParts[partIdx])
		require.Nilf(t, err, "Partition %v validation failure: %v", partIdx, err)
	}

	// Get new handle for partitions and check
	newParts, err := arr.GetParts()
	require.Nilf(t, err, "Failed to get partitions from array: %v", err)

	for partIdx, part := range newParts {
		err = checkPart(part, rawParts[partIdx])
		require.Nilf(t, err, "Re-Opened Partition %v validation failure: %v", partIdx, err)
	}
}
