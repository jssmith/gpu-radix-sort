package faas

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/stretchr/testify/require"
)

func TestFaasFilePartRef(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "radixSortFaasFilePartRef*")
	require.Nilf(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	origRootPath := filepath.Join(tmpDir, "TestFileDistribArrBytes")

	arr, err := data.NewFileDistribArray(origRootPath, 2)
	require.Nilf(t, err, "Failed to initialize array: %v", err)

	localRef := &data.PartRef{Arr: arr, PartIdx: 0, Start: 1, NByte: 2}

	faasRef, err := FilePartRefToFaas(localRef)
	require.Nil(t, err, "Failed to convert local PartRef to FaaS")

	require.Equal(t, filepath.Base(origRootPath), faasRef.ArrayName,
		"Array path not converted to FaaS")
	require.Equal(t, localRef.PartIdx, faasRef.PartId, "Part ID not converted to FaaS")
	require.Equal(t, localRef.Start, faasRef.Start, "Start not converted to FaaS")
	require.Equal(t, localRef.NByte, faasRef.NByte, "NByte not converted to FaaS")

	newLocalRef, err := LoadFaasFilePartRef(faasRef, tmpDir)
	require.Nil(t, err, "Failed to convert FaaS ref to local")

	require.Equal(t, localRef.PartIdx, newLocalRef.PartIdx, "Part ID not converted to Local")
	require.Equal(t, localRef.Start, newLocalRef.Start, "Start not converted to Local")
	require.Equal(t, localRef.NByte, newLocalRef.NByte, "NByte not converted to Local")

	newFileArr, _ := newLocalRef.Arr.(*data.FileDistribArray)
	require.Equal(t, origRootPath, newFileArr.RootPath, "Array path not converted to Local")

	t.Run("JSON", func(t *testing.T) {
		jRef, err := json.Marshal(faasRef)
		require.Nil(t, err, "Failed to marshal FaaSRef to JSON")

		var newFaasRef FaasFilePartRef
		err = json.Unmarshal(jRef, &newFaasRef)
		require.Nil(t, err, "Failed to unmarshal FaaSRef from JSON")

		require.Equal(t, *faasRef, newFaasRef, "FaaS Ref changed when passing through JSON")
	})
}
