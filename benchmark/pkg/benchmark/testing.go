package benchmark

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/faas"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
	"github.com/pkg/errors"
)

// End to end testing

// Creates a FaaS test input from an input byte array. This includes populating
// a FileDistribArray in sharedDir. narr and npart specify how inData should be
// split among distributed arrays. Typically they will be (1,1) but may be set
// differently for testing purposes.
func createFaasRefs(inData []byte, sharedDir string, narr int, npart int) ([]*faas.FaasFilePartRef, error) {
	nPartTotal := narr * npart
	nElem := len(inData) / 4
	maxPerPart := (nElem / nPartTotal) * 4

	var refs []*faas.FaasFilePartRef
	for arrX := 0; arrX < narr; arrX++ {
		origArr, err := data.NewFileDistribArray(
			filepath.Join(sharedDir, fmt.Sprintf("testFaasSortIn%v", arrX)), npart)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create input distributed array")
		}

		parts, err := origArr.GetParts()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to get partitions from input array")
		}

		for partX := 0; partX < npart; partX++ {
			start := ((arrX * npart) + partX) * maxPerPart
			end := start + maxPerPart
			if (arrX*npart + partX) == (narr*npart)-1 {
				end = len(inData)
			}

			writer, err := parts[partX].GetWriter()
			if err != nil {
				return nil, errors.Wrap(err, "Failed to get input writer")
			}

			n, err := writer.Write(inData[start:end])
			if err != nil {
				return nil, errors.Wrap(err, "Error while writing to input")
			} else if n < end-start {
				return nil, fmt.Errorf("Failed to write entire partition")
			}
			writer.Close()

			PartRef := &data.PartRef{
				Arr: origArr, PartIdx: partX, Start: 0, NByte: -1}

			faasRef, err := faas.FilePartRefToFaas(PartRef)
			if err != nil {
				return nil, errors.Wrap(err, "Failed to encode faas part reference")
			}
			refs = append(refs, faasRef)
		}
	}

	return refs, nil
}

func checkFaasPartialSort(expected []byte, outArr data.DistribArray, offset int, width int) error {
	ngroup := (1 << width)

	parts, err := outArr.GetParts()
	if err != nil {
		return errors.Wrap(err, "Failed to get partitions from output")
	}

	if len(parts) != ngroup {
		return fmt.Errorf("Output doesn't have enough partitions: %v", len(parts))
	}

	expectedInts := make([]uint32, len(expected)/4)
	err = binary.Read(bytes.NewReader(expected), binary.LittleEndian, expectedInts)
	if err != nil {
		return errors.Wrap(err, "Could not interpret reference input")
	}

	refGroupLens := make([]int, ngroup)
	for _, v := range expectedInts {
		group := sort.GroupBits(v, offset, width)
		refGroupLens[group]++
	}

	for gID := 0; gID < ngroup; gID++ {
		reader, err := parts[gID].GetReader()
		if err != nil {
			return errors.Wrap(err, "Failed to get reader from output")
		}

		groupLen, err := parts[gID].Len()
		if err != nil {
			return errors.Wrapf(err, "Couldn't determine length of group %v", gID)
		}
		groupLen = groupLen / 4

		outRaw := make([]uint32, groupLen)
		err = binary.Read(reader, binary.LittleEndian, outRaw)
		if err != nil {
			return errors.Wrap(err, "Failed to read output array")
		}

		if groupLen != refGroupLens[gID] {
			return fmt.Errorf("Group %v has wrong number of elements: Expected %v, Got %v",
				gID, refGroupLens[gID], groupLen)
		}

		for i, v := range outRaw {
			vGroup := sort.GroupBits(v, offset, width)
			if vGroup != gID {
				return fmt.Errorf("Item %v:%v has wrong group: %v", gID, i, vGroup)
			}
		}
	}

	return nil
}

func TestFaasSortPartial(nelem int) error {
	offset := 4
	width := 4
	ngroup := (1 << width)

	origRaw, err := sort.GenerateInputs((uint64)(nelem))
	if err != nil {
		return errors.Wrap(err, "Couldn't create inputs")
	}

	tmpDir, err := ioutil.TempDir("", "radixSortPartialFaasTest")
	if err != nil {
		return errors.Wrap(err, "Couldn't create temporary test directory")
	}
	defer os.RemoveAll(tmpDir)

	partRefs, err := createFaasRefs(origRaw, tmpDir, 2, 2)
	if err != nil {
		return err
	}

	// We must create the output array on the host side to avoid permissions
	// errors from docker
	outArr, err := data.NewFileDistribArray(
		filepath.Join(tmpDir, "testFaasSortPartialOut"), ngroup)
	if err != nil {
		return errors.Wrap(err, "Failed to create out distributed array")
	}

	faasArg := &faas.FaasArg{
		Offset:  offset,
		Width:   width,
		ArrType: "file",
		Input:   partRefs,
		Output:  "testFaasSortPartialOut",
	}

	//Configure SRK
	//OL will mount tmpDir to the FaaS worker so it can find the distributed
	//arrays
	os.Setenv("OL_SHARED_VOLUME", tmpDir)
	fmt.Println("Getting SRK manager")
	mgr := GetMgr()
	defer mgr.Destroy()

	err = faas.InvokeFaasSort(mgr, faasArg)
	if err != nil {
		return errors.Wrap(err, "FaaS sort failure")
	}

	// Process response
	err = checkFaasPartialSort(origRaw, outArr, offset, width)
	if err != nil {
		return err
	}

	return nil
}

func TestFaasSortFull(nelem int) error {
	origRaw, err := sort.GenerateInputs((uint64)(nelem))
	if err != nil {
		return errors.Wrap(err, "Failed to generate inputs")
	}

	tmpDir, err := ioutil.TempDir("", "radixSortFullFaasTest")
	if err != nil {
		return errors.Wrap(err, "Couldn't create temporary test directory")
	}
	defer os.RemoveAll(tmpDir)

	partRefs, err := createFaasRefs(origRaw, tmpDir, 1, 1)
	if err != nil {
		return err
	}

	inArr, err := data.NewFileDistribArrayExisting(
		filepath.Join(tmpDir, partRefs[0].ArrayName))
	if err != nil {
		return errors.Wrap(err, "Failed to reopen input array")
	}

	arrFactory := func(name string, nbucket int) (data.DistribArray, error) {
		var arr data.DistribArray
		arr, err := data.NewFileDistribArray(filepath.Join(tmpDir, name), nbucket)
		return arr, err
	}

	//Configure SRK
	//OL will mount tmpDir to the FaaS worker so it can find the distributed arrays
	os.Setenv("OL_SHARED_VOLUME", tmpDir)
	fmt.Println("Getting SRK manager")
	mgr := GetMgr()
	defer mgr.Destroy()

	outArrs, err := sort.SortDistribFromArr(inArr, nelem*4, arrFactory, sort.InitFaasWorker(mgr))
	if err != nil {
		return errors.Wrapf(err, "Sort returned an error: %v", err)
	}

	reader, err := sort.NewBucketReader(outArrs, sort.STRIDED)
	if err != nil {
		return errors.Wrapf(err, "Failed to create bucket iterator")
	}

	outRaw := make([]byte, nelem)
	for n := 0; n < len(origRaw); {
		nCur, err := reader.Read(outRaw[n:])
		if err != nil {
			return errors.Wrap(err, "Failed to read results")
		}
		n += nCur
	}

	// Process response
	err = sort.CheckSort(origRaw, outRaw)
	if err != nil {
		return err
	}

	return nil
}
