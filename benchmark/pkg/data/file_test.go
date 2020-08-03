package data

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileDistribArr(t *testing.T) {
	targetSz := 64
	shape := CreateShapeUniform(targetSz, 2)

	rand.Seed(0)

	tmpDir, err := ioutil.TempDir("", "radixSortDataTest")
	require.Nilf(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	arrPath0 := filepath.Join(tmpDir, "testFileArr0")
	arrPath1 := filepath.Join(tmpDir, "testFileArr1")

	arr0, err := CreateFileDistribArray(arrPath0, shape)
	require.Nilf(t, err, "Failed to initialize array: %v", err)

	raw0 := generateBytes(t, arr0, targetSz)

	t.Run("ReadWrite", func(t *testing.T) {
		checkArr(t, arr0, raw0)
	})

	t.Run("ReRead", func(t *testing.T) {
		checkArr(t, arr0, raw0)
	})

	t.Run("ReOpenArr", func(t *testing.T) {
		reArr0, err := OpenFileDistribArray(arrPath0)
		require.Nil(t, err, "Failed to re-open array")
		checkArr(t, reArr0, raw0)
	})

	t.Run("MultipleArrays", func(t *testing.T) {
		newShape := CreateShapeUniform(targetSz, 2)

		arr1, err := CreateFileDistribArray(arrPath1, newShape)
		require.Nilf(t, err, "Failed to initialize array: %v", err)

		raw1 := generateBytes(t, arr1, targetSz)
		arr1.Close()

		// Check reopening both
		reArr0, err := OpenFileDistribArray(arrPath0)
		require.Nilf(t, err, "Failed to reopen testFileArr0")
		reArr1, err := OpenFileDistribArray(arrPath1)
		require.Nilf(t, err, "Failed to reopen testFileArr1")

		checkArr(t, reArr0, raw0)
		checkArr(t, reArr1, raw1)

		reArr0.Close()
		reArr1.Close()
	})

	t.Run("Destroy", func(t *testing.T) {
		newShape := CreateShapeUniform(targetSz, 3)

		reArr0, err := OpenFileDistribArray(arrPath0)
		require.Nilf(t, err, "Failed to reopen testFileArr0")

		// Should Error
		_, err = CreateFileDistribArray(arrPath0, newShape)
		require.NotNil(t, err, "Did not detect existing array")

		reArr0.Destroy()

		_, err = os.Stat(arrPath0)
		require.True(t, os.IsNotExist(err), "Array0 not destroyed")

		// Now should succeed
		_, err = CreateFileDistribArray(arrPath0, newShape)
		require.Nil(t, err, "Failed to recreate array after destroy")
	})

}

func TestFileFactory(t *testing.T) {
	testArrayFactory(t, FileArrayFactory)
}
