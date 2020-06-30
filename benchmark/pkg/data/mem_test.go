package data

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func testMemRangeReader(t *testing.T, part *MemDistribPart, start int, stop int) {
	// semantics of RangeReader
	var realStop int
	if stop <= 0 {
		realStop = len(part.buf) + stop
	} else {
		realStop = stop
	}

	readLen := realStop - start

	out := make([]byte, len(part.buf))
	reader, err := part.GetRangeReader((int64)(start), (int64)(stop))
	require.Nil(t, err, "Failed to get reader")

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

	t.Run("Full Range", func(t *testing.T) { testMemRangeReader(t, p, 0, 0) })
	t.Run("First Two", func(t *testing.T) { testMemRangeReader(t, p, 0, 2) })
	t.Run("Middle", func(t *testing.T) { testMemRangeReader(t, p, 1, 3) })
	t.Run("Last Two Explicit", func(t *testing.T) { testMemRangeReader(t, p, 3, 4) })
	t.Run("Last Two Zero End", func(t *testing.T) { testMemRangeReader(t, p, 3, 0) })
	t.Run("Negative End", func(t *testing.T) { testMemRangeReader(t, p, 1, -1) })
}

func TestMemDistribPart(t *testing.T) {
	pBuf := make([]byte, 0)
	p, err := NewMemDistribPart(pBuf)
	require.Nil(t, err)

	require.Zerof(t, len(p.buf), "Initial partition has non-zero length: %v", len(p.buf))

	writer, err := p.GetWriter()
	require.Nil(t, err, "Failed to get writer")

	n, err := writer.Write([]byte{(byte)(42)})
	require.Nilf(t, err, "Write reported an error: %v", err)
	writer.Close()

	require.NotZerof(t, n, "Writer didn't report writing anything")
	require.Equalf(t, 1, len(p.buf), "Writer expanded slice wrong: Expected 1, Got %v", len(p.buf))
	require.Equalf(t, (byte)(42), p.buf[0], "Wrote incorrect value: Expected 42, Got %v", p.buf[0])

	reader, err := p.GetReader()
	require.Nil(t, err, "Failed to get reader")

	out := make([]byte, 1)
	n, err = reader.Read(out)
	require.Equalf(t, err, io.EOF, "Read did not report EOF, actual error: %v", err)
	reader.Close()

	require.Equalf(t, 1, n, "Reader reported wrong number of bytes")
	require.Equalf(t, 1, len(out), "Reader broke slice length")
	require.Equalf(t, (byte)(42), out[0], "Read Incorrect Value")
}

func TestMemDistribArrBytes(t *testing.T) {
	npart := 2
	partLen := 64

	arr, err := NewMemDistribArray(npart)
	require.Nilf(t, err, "Failed to initialize array: %v", err)

	raw := generateBytes(t, arr, partLen)

	t.Run("ReadWrite", func(t *testing.T) {
		checkArr(t, arr, raw, partLen)
	})

	t.Run("ReRead", func(t *testing.T) {
		checkArr(t, arr, raw, partLen)
	})
}
