package data

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// Check if a PartRangeReader returns the right data. Always checks part0 which
// is assumed to contain ref.
func testPartRangeReader(t *testing.T, arr DistribArray, ref []byte, start int, stop int) {
	// semantics of RangeReader
	var realStop int
	if stop <= 0 {
		realStop = len(ref) + stop
	} else {
		realStop = stop
	}

	readLen := realStop - start

	out := make([]byte, len(ref))
	reader, err := arr.GetPartRangeReader(0, start, stop)
	require.Nil(t, err, "Failed to get reader")

	n, err := reader.Read(out)
	require.Equalf(t, io.EOF, err, "Didn't return EOF")
	require.Equalf(t, readLen, n, "Didn't read enough values")
	require.Truef(t, bytes.Equal(out[:readLen], ref[start:realStop]), "Returned wrong values: expected %v, got %v", ref, out)

	err = reader.Close()
	require.Nilf(t, err, "Failed to close reader")
}

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

	totalRead := (int64)(0)
	for partIdx := 0; partIdx < shape.NPart(); partIdx++ {
		partLen := shape.Len(partIdx)
		retBytes := make([]byte, partLen)

		reader, err := arr.GetPartReader(partIdx)
		require.Nilf(t, err, "Failed to get reader for part %v", partIdx)

		readPart(t, reader, retBytes)

		// Validate
		for i := (int64)(0); i < partLen; i++ {
			refPos := (int64)(partIdx)*partLen + i
			require.Equal(t, retBytes[i], ref[refPos],
				"Returned bytes don't match at index %v", i)
		}
		totalRead += partLen
	}

	require.Equal(t, (int64)(len(ref)), totalRead, "Didn't read enough data")
}

func testArrayFactory(t *testing.T, fact *ArrayFactory) {
	var err error
	targetSz := (int64)(16)
	shape := CreateShapeUniform(targetSz, 2)

	arr, err := fact.Create("testFactory0", shape)
	require.Nil(t, err, "Failed to create array from factory")
	err = arr.Close()
	require.Nil(t, err, "Failed to close array")

	openArr, err := fact.Open("testFactory0")
	require.Nil(t, err, "Failed to open array from factory")

	openArr.Destroy()
}

func testDistribArr(t *testing.T, factory *ArrayFactory) {
	targetSz := (int64)(64)
	shape := CreateShapeUniform(targetSz, 2)

	t.Run("ReadWrite", func(t *testing.T) {
		arr0, err := factory.Create("ReadWrite", shape)
		require.Nilf(t, err, "Failed to initialize array")
		raw := generateBytes(t, arr0, (int)(targetSz))

		checkArr(t, arr0, raw)

		err = arr0.Destroy()
		require.Nilf(t, err, "Failed to destroy array: %v", "ReRead")
	})

	t.Run("ReRead", func(t *testing.T) {
		arr0, err := factory.Create("ReRead", shape)
		require.Nilf(t, err, "Failed to initialize array")
		raw := generateBytes(t, arr0, (int)(targetSz))

		checkArr(t, arr0, raw)
		checkArr(t, arr0, raw)

		err = arr0.Destroy()
		require.Nilf(t, err, "Failed to destroy array: %v", "ReRead")
	})

	t.Run("ReOpen", func(t *testing.T) {
		arr0, err := factory.Create("ReRead", shape)
		require.Nilf(t, err, "Failed to initialize array")
		raw := generateBytes(t, arr0, (int)(targetSz))

		finalShape, err := arr0.GetShape()
		require.Nil(t, err, "Failed to read shape after writing to initial array")

		err = arr0.Close()
		require.Nil(t, err, "Failed to close array0")

		reArr0, err := factory.Open("ReRead")
		require.Nil(t, err, "Failed to re-open array")

		reShape, err := reArr0.GetShape()
		require.Nil(t, err, "Couldn't read shape of re-opened array")
		require.Equal(t, finalShape.caps, reShape.caps, "Re-opened capacities don't match")
		require.Equal(t, finalShape.lens, reShape.lens, "Re-opened lengths don't match")

		checkArr(t, reArr0, raw)

		err = reArr0.Destroy()
		require.Nilf(t, err, "Failed to destroy array: %v", "ReRead")
	})

	t.Run("MultipleArrays", func(t *testing.T) {
		arr0, err := factory.Create("MultipleArrays0", shape)
		require.Nilf(t, err, "Failed to initialize array")

		newShape := CreateShapeUniform(targetSz, 2)
		arr1, err := factory.Create("MultipleArrays1", newShape)
		require.Nilf(t, err, "Failed to initialize array: %v", err)

		raw0 := generateBytes(t, arr0, (int)(targetSz))
		raw1 := generateBytes(t, arr1, (int)(targetSz))

		err = arr0.Close()
		require.Nil(t, err, "Failed to close array0")
		err = arr1.Close()
		require.Nil(t, err, "Failed to close array1")

		// Check reopening both
		reArr0, err := factory.Open("MultipleArrays0")
		require.Nilf(t, err, "Failed to reopen arr0")
		reArr1, err := factory.Open("MultipleArrays1")
		require.Nilf(t, err, "Failed to reopen arr1")

		checkArr(t, reArr0, raw0)
		checkArr(t, reArr1, raw1)

		err = reArr0.Destroy()
		require.Nil(t, err, "Failed to destroy re-opened arr0")
		err = reArr1.Destroy()
		require.Nil(t, err, "Failed to destroy re-opened arr1")
	})

	t.Run("Destroy", func(t *testing.T) {
		arr, err := factory.Create("Destroy", shape)
		require.Nilf(t, err, "Failed to initialize array")
		generateBytes(t, arr, (int)(targetSz))

		newShape := CreateShapeUniform(targetSz, 3)

		// Should Error
		_, err = factory.Create("Destroy", newShape)
		require.NotNil(t, err, "Did not detect existing array")

		arr.Destroy()

		// Now should succeed
		newArr, err := factory.Create("Destroy", newShape)
		require.Nil(t, err, "Failed to recreate array after destroy")

		shape, err := newArr.GetShape()
		require.Equal(t, 3, shape.NPart(), "New array has old shape")
		require.Equal(t, (int64)(0), shape.Len(0), "New array has non-empty partitions")
	})
}
