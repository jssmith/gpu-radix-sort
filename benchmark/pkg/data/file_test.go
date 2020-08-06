package data

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileDistribArr(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "radixSortDataTest")
	require.Nilf(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	testDistribArr(t, NewFileArrayFactory(tmpDir))
}

func TestFileFactory(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "radixSortDataTest")
	require.Nilf(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	testArrayFactory(t, NewFileArrayFactory(tmpDir))
}
