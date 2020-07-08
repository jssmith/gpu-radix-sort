package main

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/benchmark"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/faas"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
	"github.com/pkg/errors"
	"github.com/serverlessresearch/srk/pkg/srkmgr"
)

func createFaasInput(inData []uint32, sharedDir string) (*faas.FaasArg, error) {
	origArr, err := data.NewFileDistribArray(filepath.Join(sharedDir, "testFaasSortIn"), 1)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create input distributed array")
	}

	parts, err := origArr.GetParts()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get partitions from input array")
	}

	writer, err := parts[0].GetWriter()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get input writer")
	}

	err = binary.Write(writer, binary.LittleEndian, inData)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to write test inputs to distributed array")
	}
	writer.Close()

	PartRef := &data.PartRef{Arr: origArr, PartIdx: 0, Start: 0, NByte: (len(inData) * 4)}

	faasRef, err := faas.FilePartRefToFaas(PartRef)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to encode faas part reference")
	}

	faasArg := &faas.FaasArg{
		Offset:  0,
		Width:   32,
		ArrType: "file",
		Input:   []*faas.FaasFilePartRef{faasRef},
		Output:  "testFaasSortOut",
	}

	return faasArg, nil
}

func checkFaasResp(expected []uint32, outArr data.DistribArray) error {
	parts, err := outArr.GetParts()
	if err != nil {
		return errors.Wrap(err, "Failed to get partitions from output")
	}

	if len(parts) != 1 {
		return fmt.Errorf("Output contains more than one partition: %v", len(parts))
	}

	reader, err := parts[0].GetReader()
	if err != nil {
		return errors.Wrap(err, "Failed to get reader from output")
	}

	outRaw := make([]uint32, len(expected))
	err = binary.Read(reader, binary.LittleEndian, outRaw)
	if err != nil {
		return errors.Wrap(err, "Failed to read output array")
	}

	err = sort.CheckSort(expected, outRaw)
	if err != nil {
		return errors.Wrap(err, "Sorted incorrectly")
	}

	return nil
}

func testFaasSort(mgr *srkmgr.SrkManager, nelem int) error {
	origRaw := sort.RandomInputs(nelem)

	tmpDir, err := ioutil.TempDir("", "radixSortDataTest*")
	if err != nil {
		return errors.Wrap(err, "Couldn't create temporary test directory")
	}
	defer os.RemoveAll(tmpDir)

	//OL will mount tmpDir to the FaaS worker so it can find the distributed
	//arrays
	os.Setenv("OL_SHARED_VOLUME", tmpDir)

	faasArg, err := createFaasInput(origRaw, tmpDir)
	if err != nil {
		return err
	}

	// We must create the output array on the host side to avoid permissions
	// errors from docker
	outArr, err := data.NewFileDistribArray(filepath.Join(tmpDir, "testFaasSortOut"), 1)
	if err != nil {
		return errors.Wrap(err, "Failed to create out distributed array")
	}

	err = faas.InvokeFaasSort(mgr, faasArg)
	if err != nil {
		return errors.Wrap(err, "FaaS sort failure")
	}

	// Process response
	err = checkFaasResp(origRaw, outArr)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	var err error
	retcode := 0
	defer func() { os.Exit(retcode) }()

	fmt.Println("Getting SRK manager")
	mgr := benchmark.GetMgr()
	defer mgr.Destroy()

	err = testFaasSort(mgr, 1021)
	if err != nil {
		fmt.Printf("FaaS sort test failed: %v", err)
		retcode = 1
		return
	}

	fmt.Println("Success!")
	return
}
