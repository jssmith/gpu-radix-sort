package data

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileDistribArrBytes(t *testing.T) {
	targetSz := 64
	shape := &DistribArrayShape{caps: []int{targetSz, targetSz}, lens: []int{0, 0}}

	rand.Seed(0)

	tmpDir, err := ioutil.TempDir("", "radixSortDataTest*")
	require.Nilf(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	arr, err := NewFileDistribArray(filepath.Join(tmpDir, "TestFileDistribArrBytes"), shape)
	require.Nilf(t, err, "Failed to initialize array: %v", err)

	raw := generateBytes(t, arr, targetSz)

	t.Run("ReadWrite", func(t *testing.T) {
		checkArr(t, arr, raw)
	})

	t.Run("ReRead", func(t *testing.T) {
		checkArr(t, arr, raw)
	})

	t.Run("ReOpenArr", func(t *testing.T) {
		arr2, err := NewFileDistribArrayExisting(filepath.Join(tmpDir, "TestFileDistribArrBytes"))
		require.Nil(t, err, "Failed to re-open array")
		checkArr(t, arr2, raw)
	})
}
