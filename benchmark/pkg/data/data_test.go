package data

import (
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// A very pedantic read procedure with lots of checking. Most people will just use ioutil.ReadFull.
func readPart(t *testing.T, partReader io.ReadCloser, dst []byte) {
	var err error

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
	shape, err := arr.GetShape()
	require.Nilf(t, err, "Failed to get shape of array")

	raw = make([]byte, shape.NPart()*partLen)
	rand.Read(raw)
	for partIdx := 0; partIdx < shape.NPart(); partIdx++ {
		globalStart := partIdx * partLen

		t.Logf("Processing partition %v\n", partIdx)
		writer, err := arr.GetPartWriter(partIdx)
		require.Nilf(t, err, "Failed to get writer for partition %v", partIdx)

		n, err := writer.Write(raw[globalStart : globalStart+partLen])
		require.Nilf(t, err, "Error while writing to part %v", partIdx)
		require.Equalf(t, partLen, n, "Failed to write enough data to part %v", partIdx)

		err = writer.Close()
		require.Nilf(t, err, "Failed to close writer for part %v", partIdx)
	}

	return raw
}

func checkArr(t *testing.T, arr DistribArray, ref []byte) {
	shape, err := arr.GetShape()
	require.Nilf(t, err, "Failed to get shape of array")

	for partIdx := 0; partIdx < shape.NPart(); partIdx++ {
		partLen := shape.Len(partIdx)
		retBytes := make([]byte, partLen)

		reader, err := arr.GetPartReader(partIdx)
		require.Nilf(t, err, "Failed to get reader for part %v", partIdx)

		readPart(t, reader, retBytes)

		// Validate
		for i := 0; i < partLen; i++ {
			refPos := partIdx*partLen + i
			require.Equal(t, retBytes[i], ref[refPos],
				"Returned bytes don't match at index %v", i)
		}
	}
}
