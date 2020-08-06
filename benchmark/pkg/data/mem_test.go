package data

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemDistribPartRange(t *testing.T) {
	var err error

	targetSz := 4
	shape := CreateShapeUniform((int64)(targetSz), 1)

	arr, err := CreateMemDistribArray("TestRangeReader", shape)
	require.Nil(t, err)

	raw := generateBytes(t, arr, targetSz)

	t.Run("Full Range", func(t *testing.T) { testPartRangeReader(t, arr, raw, 0, 0) })
	t.Run("First Two", func(t *testing.T) { testPartRangeReader(t, arr, raw, 0, 2) })
	t.Run("Middle", func(t *testing.T) { testPartRangeReader(t, arr, raw, 1, 3) })
	t.Run("Last Two Explicit", func(t *testing.T) { testPartRangeReader(t, arr, raw, 3, 4) })
	t.Run("Last Two Zero End", func(t *testing.T) { testPartRangeReader(t, arr, raw, 3, 0) })
	t.Run("Negative End", func(t *testing.T) { testPartRangeReader(t, arr, raw, 1, -1) })
}

func TestMemDistribArr(t *testing.T) {
	testDistribArr(t, MemArrayFactory)
}

func TestMemFactory(t *testing.T) {
	testArrayFactory(t, MemArrayFactory)
}
