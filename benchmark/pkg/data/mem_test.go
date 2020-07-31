package data

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemDistribPartRange(t *testing.T) {
	var err error

	targetSz := 4
	shape := DistribArrayShape{caps: []int{targetSz}, lens: []int{0}}

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
	targetSz := 64
	shape := DistribArrayShape{caps: []int{targetSz, targetSz}, lens: []int{0, 0}}

	rand.Seed(0)

	arr0, err := CreateMemDistribArray("memArrTest0", shape)
	require.Nilf(t, err, "Failed to initialize array: %v", err)

	raw0 := generateBytes(t, arr0, targetSz)

	t.Run("ReadWrite", func(t *testing.T) {
		checkArr(t, arr0, raw0)
	})

	t.Run("ReRead", func(t *testing.T) {
		checkArr(t, arr0, raw0)
	})

	t.Run("ReOpen", func(t *testing.T) {
		arr0.Close()

		reArr0, err := OpenMemDistribArray("memArrTest0")
		require.Nil(t, err, "Failed to reopen first array")
		checkArr(t, reArr0, raw0)

		reArr0.Close()
	})

	t.Run("MultipleArrays", func(t *testing.T) {
		newShape := DistribArrayShape{caps: []int{targetSz, targetSz}, lens: []int{0, 0}}
		arr1, err := CreateMemDistribArray("memArrTest1", newShape)
		require.Nilf(t, err, "Failed to initialize array: %v", err)

		raw1 := generateBytes(t, arr1, targetSz)
		arr1.Close()

		// Check reopening both
		reArr0, err := OpenMemDistribArray("memArrTest0")
		require.Nilf(t, err, "Failed to reopen memArrTest0")
		reArr1, err := OpenMemDistribArray("memArrTest1")
		require.Nilf(t, err, "Failed to reopen memArrTest1")

		checkArr(t, reArr0, raw0)
		checkArr(t, reArr1, raw1)

		reArr0.Close()
		reArr1.Close()
	})

	t.Run("Destroy", func(t *testing.T) {
		newShape := DistribArrayShape{caps: []int{targetSz, targetSz, targetSz}, lens: []int{0, 0, 0}}

		reArr0, err := OpenMemDistribArray("memArrTest0")
		require.Nilf(t, err, "Failed to reopen memArrTest0")

		// Should Error
		_, err = CreateMemDistribArray("memArrTest0", newShape)
		require.NotNil(t, err, "Did not detect existing array")

		reArr0.Destroy()

		// Now should succeed
		newArr0, err := CreateMemDistribArray("memArrTest0", newShape)
		require.Nil(t, err, "Failed to recreate array after destroy")

		// Though not guaranteed by the interface, MemDistribArray objects
		// happen to remain valid after destroy (they only get really freed
		// when the last object reference goes out of scope). This test
		// wouldn't work for e.g. FileDistribArrays.
		require.NotEqualf(t, newArr0.shape.lens, reArr0.shape.lens, "Re-used destroyed array (shape lengths didn't get reset)")
		require.Equalf(t, 0, len(newArr0.parts[0]), "Re-used destroyed array (same backing store for parts)")

		newArr0.Destroy()
	})
}
