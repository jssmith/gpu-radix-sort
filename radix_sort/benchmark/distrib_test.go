package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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

// The simplest test for DistribArray writes raw bytes and uses only
// DistribArray features (no binary decoding or anything)
func testDistribArrBytes(t *testing.T, arr DistribArray, npart int, partLen int) {
	parts, err := arr.GetParts()
	require.Nilf(t, err, "Failed to get partitions: %v", err)

	rawParts := make([]([]byte), npart)
	for partIdx := 0; partIdx < npart; partIdx++ {
		partLog := logrus.WithFields(logrus.Fields{"partition": partIdx})
		part := parts[partIdx]

		// Generate Inputs
		partLog.Infof("Generating inputs:")
		rawParts[partIdx] = make([]byte, partLen)
		for i := 0; i < partLen; i++ {
			rawParts[partIdx][i] = (byte)(((partIdx * partLen) + i) % 256)
		}

		// Write to partition
		partWriter := part.GetWriter()

		partLog.Info("Writing bytes")
		n, err := partWriter.Write(rawParts[partIdx])
		require.Nilf(t, err, "part %v: Writing byte failed: %v", partIdx, err)
		require.Equalf(t, n, len(rawParts[partIdx]), "part %v: Failed to write all bytes but didn't receive error", partIdx)

		err = partWriter.Close()
		require.Nilf(t, err, "part %v: Closing partWriter failed: %v", partIdx, err)

		// Read Back from Partition
		partLog.Info("Reading bytes back")
		partReader := part.GetReader()
		retBytes := make([]byte, partLen)
		ntotal := 0
		for ntotal < partLen {
			n, err := partReader.Read(retBytes[ntotal:])
			ntotal += n
			if err == io.EOF {
				require.Equalf(t, ntotal, partLen,
					"part %v: Didn't read enough bytes: expected %v, got %v", partIdx, partLen, ntotal)
				break
			}
			require.Nil(t, err, "part %v: Error returned after reading %v bytes: %v", partIdx, ntotal, err)
			require.NotZerof(t, n, "part %v: Reader didn't return any data", partIdx)
		}

		// ReadCloser must return io.EOF either with the last bytes or with a zero
		// length read after the last bytes
		if err != io.EOF {
			n, err := partReader.Read(retBytes[n:])
			require.Zerof(t, n, "part %v: read %v extra bytes", partIdx, n)
			require.Equalf(t, err, io.EOF, "part %v: partReader failed to return io.EOF", partIdx)
		}
		err = partReader.Close()
		require.Nilf(t, err, "part %v: Failed to close partReader: %v", partIdx, err)

		// Validate
		partLog.Info("Validating output")
		for i := 0; i < partLen; i++ {
			require.Equal(t, retBytes[i], rawParts[partIdx][i],
				"part %v: Returned bytes don't match at index %v: expected %v, got %v", partIdx, i, rawParts[partIdx][i])
			rawParts[partIdx][i] = (byte)(((partIdx * partLen) + i) % 256)
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

func TestMemDistribPart(t *testing.T) {
	pBuf := make([]byte, 0)
	p, err := NewMemDistribPart(pBuf)
	require.Nil(t, err)

	require.Zerof(t, len(p.part), "Initial partition has non-zero length: %v", len(p.part))

	writer := p.GetWriter()
	n, err := writer.Write([]byte{42})
	require.Nilf(t, err, "Write reported an error: %v", err)
	writer.Close()

	require.NotZerof(t, n, "Writer didn't report writing anything")
	require.Equalf(t, 1, len(p.part), "Writer expanded slice wrong: Expected 1, Got %v", len(p.part))
	require.Equalf(t, 42, p.part[0], "Wrote incorrect value: Expected 42, Got %v", p.part[0])

	reader := p.GetReader()
	out := make([]byte, 1)
	n, err = reader.Read(out)
	require.Nilf(t, err, "Read reported an error: %v", err)
	reader.Close()

	require.NotZerof(t, n, "Reader didn't report reading anything")
	require.Equalf(t, 1, len(out), "Reader broke slice length: Expected 1, Got %v", len(out))
	require.Equalf(t, 42, out[0], "Read Incorrect Value: Expected 42, Got %v", out[0])
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
