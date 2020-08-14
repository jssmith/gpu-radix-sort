package benchmark

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/faas"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
	"github.com/pkg/errors"
)

// End to end testing

func createFaasRefs(inData []byte, factory *data.ArrayFactory, shapes []data.DistribArrayShape) ([]*faas.FaasFilePartRef, error) {
	inPos := 0
	var refs []*faas.FaasFilePartRef
	for arrX := 0; arrX < len(shapes); arrX++ {
		arr, err = data.Create(fmt.Sprintf("testFaasSortIn%v", arrX), shapes[arrX])
		if err != nil {
			return refs, errors.Wrapf(err, "Failed to create input array %v", arrX)
		}

		shape = shapes[arrX]
		for partX := 0; partX < shape.NPart(); partX++ {
			start = inPos
			end = inPos + shape.Len(partX)

			writer, err := arr.GetPartWriter(partX)
			if err != nil {
				return refs, errors.Wrapf(err, "Failed to get input writer for %v:%v", arrX, partX)
			}

			n, err := writer.Write(inData[start:end])
			if err != nil {
				return refs, errors.Wrapf(err, "Error while writing to input %v:%v", arrX, partX)
			} else if n < end-start {
				return refs, fmt.Errorf("Failed to write entire partition for %v:%v", arrX, partX)
			}
			writer.Close()

			PartRef := &data.PartRef{
				Arr: origArr, PartIdx: partX, Start: 0, NByte: -1}

			faasRef, err := faas.FilePartRefToFaas(PartRef)
			if err != nil {
				return refs, errors.Wrapf(err, "Failed to encode faas part reference for %v:%v", arrX, partX)
			}

			refs = append(refs, faasRef)
			inPos += shape.Len(partX)
		}
		arr.Close()
	}

	return refs, nil
}

func TestFaasSortPartial(nelem int) error {
	offset := 4
	width := 4
	ngroup := (1 << width)

	origRaw, err := sort.GenerateInputs((uint64)(nelem))
	if err != nil {
		return errors.Wrap(err, "Failed to generate inputs")
	}

	tmpDir, err := ioutil.TempDir("", "radixSortPartialFaasTest")
	if err != nil {
		return errors.Wrap(err, "Couldn't create temporary test directory")
	}
	defer os.RemoveAll(tmpDir)

	arrFactory = data.NewFileArrayFactory(tmpDir)
	shapes = []data.DistribArrayShape{data.CreateShapeUniform(nByte, 2), data.CreateShapeUniform(nByte, 2)}

	partRefs, err := createFaasRefs(origRaw, arrFactory, shapes)
	if err != nil {
		return err
	}

	//Configure SRK
	//OL will mount tmpDir to the FaaS worker so it can find the distributed arrays
	os.Setenv("OL_SHARED_VOLUME", tmpDir)
	fmt.Println("Getting SRK manager")
	mgr := GetMgr()
	defer mgr.Destroy()

	faasArg := &faas.FaasArg{
		Offset:  offset,
		Width:   width,
		ArrType: "file",
		Input:   partRefs,
		Output:  "testFaasSortPartialOut",
	}

	err = faas.InvokeFaasSort(mgr, faasArg)
	if err != nil {
		return errors.Wrap(err, "FaaS sort failure")
	}

	// Process response
	err = sort.CheckPartialArray(outArr, offset, width)
	if err != nil {
		return err
	}

	return nil
}

func TestFaasSortFull(nelem int) error {
	nByte := nelem * 4

	origRaw, err := sort.GenerateInputs((uint64)(nelem))
	if err != nil {
		return errors.Wrap(err, "Failed to generate inputs")
	}

	tmpDir, err := ioutil.TempDir("", "radixSortFullFaasTest")
	if err != nil {
		return errors.Wrap(err, "Couldn't create temporary test directory")
	}
	defer os.RemoveAll(tmpDir)

	arrFactory = data.NewFileArrayFactory(tmpDir)
	shapes = []data.DistribArrayShape{data.CreateShapeUniform(nByte, 1)}

	partRefs, err := createFaasRefs(origRaw, arrFactory, shapes)
	if err != nil {
		return err
	}

	inArr, err := arrFactory.Open(partRefs[0].ArrayName)
	if err != nil {
		return errors.Wrap(err, "Failed to reopen input array")
	}

	//Configure SRK
	//OL will mount tmpDir to the FaaS worker so it can find the distributed arrays
	os.Setenv("OL_SHARED_VOLUME", tmpDir)
	fmt.Println("Getting SRK manager")
	mgr := GetMgr()
	defer mgr.Destroy()

	outArrs, err := sort.SortDistribFromArr(inArr, nByte, arrFactory, sort.InitFaasWorker(mgr))
	if err != nil {
		return errors.Wrapf(err, "Sort returned an error: %v", err)
	}

	reader, err := sort.NewBucketReader(outArrs, sort.STRIDED)
	if err != nil {
		return errors.Wrapf(err, "Failed to create bucket iterator")
	}

	outRaw := make([]byte, nByte)
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
