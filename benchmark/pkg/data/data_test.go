package data

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func generateBytes(t *testing.T, arr DistribArray, partLen int) (raw []byte) {
	parts, err := arr.GetParts()
	require.Nilf(t, err, "Failed to get partitions: %v", err)

	npart := len(parts)
	raw = make([]byte, npart*partLen)
	for partIdx := 0; partIdx < npart; partIdx++ {
		globalStart := partIdx * partLen
		t.Logf("Processing partition %v\n", partIdx)
		part := parts[partIdx]

		// Generate Inputs
		for i := 0; i < partLen; i++ {
			globalPos := globalStart + i
			raw[globalPos] = (byte)(globalPos % 256)
		}

		// Write to partition
		writePart(t, part, raw[globalStart:globalStart+partLen])
	}

	return raw
}

func checkArr(t *testing.T, arr DistribArray, ref []byte, partLen int) {
	parts, err := arr.GetParts()
	require.Nilf(t, err, "Failed to get partitions: %v", err)

	npart := len(parts)

	for partIdx := 0; partIdx < npart; partIdx++ {
		// Re-allocate each time to zero out
		retBytes := make([]byte, partLen)
		readPart(t, parts[partIdx], retBytes)

		// Validate
		for i := 0; i < partLen; i++ {
			refPos := partIdx*partLen + i
			require.Equal(t, retBytes[i], ref[refPos],
				"Returned bytes don't match at index %v", i)
		}
	}
}
