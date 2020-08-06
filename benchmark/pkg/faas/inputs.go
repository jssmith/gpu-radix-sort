package faas

import (
	"fmt"
	"path/filepath"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/pkg/errors"
)

// JSON serializable version of data.PartRef expected by the faas sorter. See
// the faas documentation for the meaning of these fields (faasTest/README.md)
type FaasFilePartRef struct {
	ArrayName string `json:"arrayName"`
	PartId    int    `json:"partID"`
	Start     int    `json:"start"`
	NByte     int    `json:"nbyte"`
}

// Argument expected by the radix sort function in SRK. See the faas
// documentation for the meaning of these fields (faasTest/README.md)
type FaasArg struct {
	Offset  int                `json:"offset"`
	Width   int                `json:"width"`
	ArrType string             `json:"arrType"`
	Input   []*FaasFilePartRef `json:"input"`
	Output  string             `json:"output"`
}

type FaasResp struct {
	Success bool   `json:"success"`
	Err     string `json:"err"`
}

// Convert a data.PartRef to FaasPartRef
func FilePartRefToFaas(ref *data.PartRef) (*FaasFilePartRef, error) {
	fileArr, ok := ref.Arr.(*data.FileDistribArray)
	if !ok {
		return nil, fmt.Errorf("PartRef array has wrong type \"%T\", must be data.FileDistribArray", ref.Arr)
	}

	arg := &FaasFilePartRef{
		ArrayName: filepath.Base(fileArr.RootPath),
		PartId:    ref.PartIdx,
		Start:     ref.Start,
		NByte:     ref.NByte,
	}
	return arg, nil
}

// Load a FaasFilePartRef into a local data.PartRef. localArrDir is the local mount
// point for file distributed arrays (the directory shared between FaaS and
// local for storing distributed arrays).
func LoadFaasFilePartRef(ref *FaasFilePartRef, localArrDir string) (*data.PartRef, error) {
	localArrPath := filepath.Join(localArrDir, ref.ArrayName)

	arr, err := data.OpenFileDistribArray(localArrPath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to load referenced FileDistributedArray")
	}

	return &data.PartRef{Arr: arr, PartIdx: ref.PartId, Start: ref.Start, NByte: ref.NByte}, nil
}
