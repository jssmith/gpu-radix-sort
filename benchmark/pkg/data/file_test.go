package data

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileDistribArrBytes(t *testing.T) {
	npart := 2
	partLen := 64

	tmpDir, err := ioutil.TempDir("", "radixSortDataTest*")
	require.Nilf(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	arr, err := NewFileDistribArray(filepath.Join(tmpDir, "TestFileDistribArrBytes"), npart)
	require.Nilf(t, err, "Failed to initialize array: %v", err)

	testDistribArrBytes(t, arr, npart, partLen)
}
