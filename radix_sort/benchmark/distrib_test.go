package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func checkPart(part DistribPart, expected []uint32) error {
	partReader := part.GetReader()
	retInts := make([]uint32, len(expected))
	err := binary.Read(partReader, binary.LittleEndian, retInts)
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
	partWriter := part.GetWriter()

	n, err := partWriter.Write(src)
	require.Nilf(t, err, "Writing failed: %v", err)
	require.Equalf(t, n, len(src), "Failed to write all bytes but didn't receive error")

	err = partWriter.Close()
	require.Nilf(t, err, "Closing partWriter failed: %v", err)
}

func readPart(t *testing.T, part DistribPart, dst []byte) {
	var err error
	partReader := part.GetReader()
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
		partWriter := part.GetWriter()
		err := binary.Write(partWriter, binary.LittleEndian, rawParts[partIdx])
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

func testRangeReader(t *testing.T, part *MemDistribPart, start int, stop int) {
	// semantics of RangeReader
	var realStop int
	if stop <= 0 {
		realStop = len(part.buf) + stop
	} else {
		realStop = stop
	}

	readLen := realStop - start

	out := make([]byte, len(part.buf))
	reader := part.GetRangeReader((int64)(start), (int64)(stop))
	n, err := reader.Read(out)
	require.Equalf(t, io.EOF, err, "Didn't return EOF")
	require.Equalf(t, readLen, n, "Didn't read enough values")
	require.Truef(t, bytes.Equal(out[:readLen], part.buf[start:realStop]), "Returned wrong values: expected %v, got %v", part.buf, out)
}

func TestMemDistribPartRange(t *testing.T) {
	var err error
	pBuf := []byte{0, 1, 2, 3}
	p, err := NewMemDistribPart(pBuf)
	require.Nil(t, err)

	t.Run("Full Range", func(t *testing.T) { testRangeReader(t, p, 0, 0) })
	t.Run("First Two", func(t *testing.T) { testRangeReader(t, p, 0, 2) })
	t.Run("Middle", func(t *testing.T) { testRangeReader(t, p, 1, 3) })
	t.Run("Last Two Explicit", func(t *testing.T) { testRangeReader(t, p, 3, 4) })
	t.Run("Last Two Zero End", func(t *testing.T) { testRangeReader(t, p, 3, 0) })
	t.Run("Negative End", func(t *testing.T) { testRangeReader(t, p, 1, -1) })
}

func TestMemDistribPart(t *testing.T) {
	pBuf := make([]byte, 0)
	p, err := NewMemDistribPart(pBuf)
	require.Nil(t, err)

	require.Zerof(t, len(p.buf), "Initial partition has non-zero length: %v", len(p.buf))

	writer := p.GetWriter()
	n, err := writer.Write([]byte{(byte)(42)})
	require.Nilf(t, err, "Write reported an error: %v", err)
	writer.Close()

	require.NotZerof(t, n, "Writer didn't report writing anything")
	require.Equalf(t, 1, len(p.buf), "Writer expanded slice wrong: Expected 1, Got %v", len(p.buf))
	require.Equalf(t, (byte)(42), p.buf[0], "Wrote incorrect value: Expected 42, Got %v", p.buf[0])

	reader := p.GetReader()
	out := make([]byte, 1)
	n, err = reader.Read(out)
	require.Equalf(t, err, io.EOF, "Read did not report EOF, actual error: %v", err)
	reader.Close()

	require.Equalf(t, 1, n, "Reader reported wrong number of bytes")
	require.Equalf(t, 1, len(out), "Reader broke slice length")
	require.Equalf(t, (byte)(42), out[0], "Read Incorrect Value")
}

func TestMemDistribArrUints(t *testing.T) {
	npart := 2
	partLen := 1024

	arr, err := NewMemDistribArray(npart)
	require.Nilf(t, err, "Failed to initialize array: %v", err)

	testDistribArrUints(t, arr, npart, partLen)
}

func TestMemDistribArrBytes(t *testing.T) {
	npart := 2
	partLen := 64

	arr, err := NewMemDistribArray(npart)
	require.Nilf(t, err, "Failed to initialize array: %v", err)

	testDistribArrBytes(t, arr, npart, partLen)
}
