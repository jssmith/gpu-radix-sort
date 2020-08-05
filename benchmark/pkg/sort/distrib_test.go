package sort

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestLocalDistribWorker(t *testing.T) {
	var err error

	nElem := 1021
	nByte := nElem * 4
	width := 4
	npart := 1 << width

	err = InitLibSort()
	require.Nil(t, err, "Failed to initialize libsort")

	origRaw, err := GenerateInputs((uint64)(nElem))
	require.Nil(t, err, "Failed to generate test inputs")

	shape := data.CreateShapeUniform(nByte, 1)
	origArr, err := data.CreateMemDistribArray("testLocalDistribWorker", shape)
	require.Nil(t, err)

	writer, err := origArr.GetPartWriter(0)
	require.Nil(t, err, "Failed to get writer")

	n, err := writer.Write(origRaw)
	require.Nil(t, err)
	require.Equal(t, n, nByte)
	writer.Close()

	PartRefs := []*data.PartRef{&data.PartRef{Arr: origArr, PartIdx: 0, Start: 0, NByte: (nElem / 2) * 4},
		&data.PartRef{Arr: origArr, PartIdx: 0, Start: (nElem / 2) * 4, NByte: (nElem - (nElem / 2)) * 4}}

	outArr, err := LocalDistribWorker(PartRefs, 0, width, "testLocalDistribWorker", data.MemArrayFactory)
	require.Nil(t, err)

	outShape, err := outArr.GetShape()
	require.Nil(t, err, "Failed to get output array shape")
	require.Equal(t, npart, outShape.NPart(), "Output array has wrong number of partitions")

	outRaw := make([]byte, nByte)
	boundaries := make([]uint64, npart)
	totalLen := 0

	for i := 0; i < outShape.NPart(); i++ {
		partLen := outShape.Len(i)

		boundaries[i] = (uint64)(totalLen)

		totalLen += partLen
		require.LessOrEqual(t, totalLen, nByte, "Too much data returned")

		reader, err := outArr.GetPartReader(i)
		require.Nilf(t, err, "Failed to get reader for output partition %v", i)

		n, err = reader.Read(outRaw[boundaries[i]:totalLen])
		require.Equal(t, partLen, n, "Reader didn't return enough data")
		if err != io.EOF && err != nil {
			t.Fatalf("Error when reading output partition: %v", err)
		}

		reader.Close()
	}
	require.Equal(t, nByte, totalLen, "Output buckets have the wrong number of elements")

	checkPartial(t, outRaw, boundaries, origRaw)
}

// Generates a list of narr arrays with npart partitions each, with elemPerPart
// elements per partition. The value in each partition will be (partId << 4 |
// arrId). This means that a strided access will be in-order (mimicking the
// output of a radix sort). narr and npart must be < 16.
func generateArrs(t *testing.T, narr int, baseName string,
	factory *data.ArrayFactory, shape data.DistribArrayShape) []data.DistribArray {
	var err error

	arrs := make([]data.DistribArray, narr)
	for arrX := 0; arrX < narr; arrX++ {
		arrs[arrX], err = factory.Create(fmt.Sprintf("%v_%v", baseName, arrX), shape)
		require.Nilf(t, err, "Failed to build array %v", arrX)

		for partX := 0; partX < shape.NPart(); partX++ {
			// Total data will be ordered by partition ID first, then by array
			// (the strided access by the generator should produce in-order
			// data)
			partRaw := bytes.Repeat([]byte{(byte)((partX << 4) | arrX)}, shape.Cap(partX))

			writer, err := arrs[arrX].GetPartWriter(partX)
			require.Nilf(t, err, "Failed to get writer for output %v:%v", arrX, partX)

			n, err := writer.Write(partRaw)
			require.Equal(t, shape.Cap(partX), n, "Didn't write enough to initial data")
			require.Nil(t, err, "Failed to write initial data")
			writer.Close()
		}
	}

	return arrs
}

func testBucketReader(t *testing.T, order ReadOrder,
	arrReader func(*BucketReader, []byte) (int, error),
	baseName string) {

	narr := 2
	npart := 16
	// npart := 4
	elemPerPart := 256
	nElem := narr * npart * elemPerPart
	uniformShape := data.CreateShapeUniform(elemPerPart, npart)

	// Given the global 'index' of 'value' taken from an array list returned by
	// generateArrs, ensures that the value is correct for that index.
	var checker func(*testing.T, int, byte, data.DistribArrayShape)
	if order == STRIDED {
		checker = func(t *testing.T, index int, value byte, shape data.DistribArrayShape) {
			globalPart := 0
			sum := 0
		partCalc:
			for partX := 0; partX < shape.NPart(); partX++ {
				for arrX := 0; arrX < narr; arrX++ {
					sum += shape.Cap(partX)
					// t.Logf("Shape %v:%v: %v", shape.Cap(partX), arrX, partX, sum)
					if sum >= index+1 {
						// t.Logf("idx=%v, [%v:%v]->%v", index, arrX, partX, globalPart)
						break partCalc
					}
					globalPart++
				}
			}

			// the input generator can only encode the first 4b of part/arr ids
			outArrId := (int)(value & 0xf)
			outPartId := (int)(value >> 4)
			expectPartId := (globalPart / narr) & 0xf
			expectArrId := (globalPart % narr) & 0xf

			// t.Logf("Checking %v: 0x%04x", index, value)
			// t.Logf("Expecting %v:%v", expectArrId, expectPartId)
			// t.Logf("Got %v:%v", outArrId, outPartId)

			require.Equal(t, expectPartId, outPartId, "Partitions out of order")
			require.Equal(t, expectArrId, outArrId, "Arrays out of order")
		}
	} else {
		//INORDER

		checker = func(t *testing.T, index int, value byte, shape data.DistribArrayShape) {
			outArrId := (int)(value & 0xf)
			outPartId := (int)(value >> 4)

			globalPart := 0
			sum := 0
		partCalc:
			for arrX := 0; arrX < narr; arrX++ {
				for partX := 0; partX < shape.NPart(); partX++ {
					sum += shape.Cap(partX)
					// t.Logf("Shape %v:%v: %v", arrX, partX, sum)
					if sum >= index+1 {
						// t.Logf("idx=%v, [%v:%v]->%v", index, arrX, partX, globalPart)
						break partCalc
					}
					globalPart++
				}
			}

			// the input generator can only encode the first 4b of part/arr ids
			expectPartId := (globalPart % npart) & 0xf
			expectArrId := (globalPart / npart) & 0xf

			// t.Logf("Checking %v: 0x%04x", index, value)
			// t.Logf("Expecting %v:%v", expectArrId, expectPartId)
			// t.Logf("Got %v:%v", outArrId, outPartId)

			require.Equalf(t, expectPartId, outPartId, "Partitions out of order at %v", index)
			require.Equal(t, expectArrId, outArrId, "Arrays out of order at %v", index)
		}
	}
	fullArrs := generateArrs(t, narr, baseName+"_testBucketReader", data.MemArrayFactory, uniformShape)

	t.Run("All", func(t *testing.T) {
		iter, err := NewBucketReader(fullArrs, order)
		require.Nil(t, err, "Couldn't create reader")

		out := make([]byte, nElem)
		n, err := arrReader(iter, out)
		require.Nil(t, err, "Failed to read from iterator")
		require.Equal(t, nElem, n, "Didn't read enough")

		for i, v := range out {
			checker(t, i, v, uniformShape)
		}
	})

	t.Run("Unaligned", func(t *testing.T) {
		iter, err := NewBucketReader(fullArrs, order)
		require.Nil(t, err, "Failed to create BucketReader")

		// Read almost 1 partition per read, but not exact to prevent reads
		// from aligning perfectly with partition boundaries
		readSz := elemPerPart - 1
		out := make([]byte, readSz)
		for i := 0; i < npart*narr; i++ {
			n, err := arrReader(iter, out)

			if i != npart*narr {
				require.Nil(t, err, "Error during %vth read", i)

				// Technically, Read() is allowed to return <len(out) but
				// BucketReader doesn't do this.
				require.Equal(t, readSz, n, "Read %v wrong amount of data", i)
			} else {
				require.Equal(t, nElem-(readSz*(npart-1)), n, "Wrong amount of data returned on last read")
				out = out[:n]
				if err != io.EOF {
					// Read() is not required to return EOF with the last data
					// (although it usually will), but it must return it on
					// subsequent reads.
					require.Nil(t, err, "Error on last read")

					dummy := make([]byte, 1)
					n, err = iter.Read(dummy)
					require.Equal(t, 0, n, "Extra read due to no EOF returned data")
					require.Equal(t, io.EOF, err, "Extra read due to no EOF didn't return EOF")
				}
			}
			for outX, v := range out {
				globalIdx := i*readSz + outX
				checker(t, globalIdx, v, uniformShape)
			}
		}
	})

	// Test with some partitions containing zero elements
	t.Run("ZeroParts", func(t *testing.T) {
		zeroRatio := 4
		caps := make([]int, npart)
		for i := 0; i < npart; i++ {
			if i%zeroRatio == 0 {
				caps[i] = elemPerPart
			} else {
				caps[i] = 0
			}
		}
		zerosShape := data.CreateShape(caps)

		zeroArrs := generateArrs(t, narr, baseName+"_testBucketReaderZero", data.MemArrayFactory, zerosShape)
		iter, err := NewBucketReader(zeroArrs, order)

		out := make([]byte, nElem/zeroRatio)
		n, err := arrReader(iter, out)
		require.Nil(t, err, "Failed to read from iterator")
		require.Equal(t, n, nElem/zeroRatio, "Iterator returned wrong number of bytes")

		for i, v := range out {
			checker(t, i, v, zerosShape)
		}

		for i := 0; i < narr; i++ {
			zeroArrs[i].Destroy()
		}
	})

	for i := 0; i < narr; i++ {
		fullArrs[i].Destroy()
	}
}

func bucketRead(reader *BucketReader, out []byte) (int, error) {
	var err error
	var n int
	for n = 0; n < len(out); {
		nCur, err := reader.Read(out[n:])
		if err != nil {
			return n, err
		}
		n += nCur
	}
	return n, err
}

func bucketReadRef(reader *BucketReader, out []byte) (int, error) {
	refs, err := reader.ReadRef(len(out))
	if err != nil {
		return 0, err
	}

	inBytes, err := data.FetchPartRefs(refs)
	if err != nil {
		return 0, errors.Wrap(err, "Couldn't read input references")
	}

	copy(out, inBytes)
	return len(out), nil
}

func TestBucketReaderStrided(t *testing.T) {
	testBucketReader(t, STRIDED, bucketRead, "strided")
}

func TestBucketReaderInOrder(t *testing.T) {
	testBucketReader(t, INORDER, bucketRead, "inorder")
}

func TestBucketRefReaderStrided(t *testing.T) {
	testBucketReader(t, STRIDED, bucketReadRef, "stridedRef")
}

func TestBucketRefReaderInOrder(t *testing.T) {
	testBucketReader(t, INORDER, bucketReadRef, "inorderRef")
}

// We only test STRIDED access for reading ref's because the traversal logic is
// shared with the Read() interface and we already test that there
func TestBucketReaderRef(t *testing.T) {
	narr := 2
	npart := 2
	elemPerPart := 256
	nElem := narr * npart * elemPerPart
	shape := data.CreateShapeUniform(elemPerPart, npart)

	arrs := generateArrs(t, narr, "testBucketReaderRef", data.MemArrayFactory, shape)

	t.Run("Aligned", func(t *testing.T) {
		g, err := NewBucketReader(arrs, STRIDED)
		require.Nil(t, err, "Couldn't initialize generator")

		for i := 0; i < npart*narr; i++ {
			refs, err := g.ReadRef(elemPerPart)
			require.Nilf(t, err, "Failed to get %vth reference from generator", i)
			require.Equal(t, 1, len(refs), "Returned too many references")
			require.Equal(t, arrs[i%narr], refs[0].Arr, "DistribArrays returned in wrong order")
			require.Equal(t, i/narr, refs[0].PartIdx, "Partitions returned in wrong order")
			require.Equal(t, 0, refs[0].Start, "Reference should start from beginning")
			require.Equal(t, elemPerPart, refs[0].NByte, "Reference has wrong size")
		}
		refs, err := g.ReadRef(elemPerPart)
		require.Equal(t, io.EOF, err, "Generator did not return EOF")
		require.Zero(t, len(refs), "Returned too much data")
	})

	// Reads not aligned to partition boundaries, the generator will have to
	// split inputs across partitions.
	t.Run("Unaligned", func(t *testing.T) {
		g, err := NewBucketReader(arrs, STRIDED)
		require.Nil(t, err, "Couldn't initialize generator")

		elemPerInput := elemPerPart - 1
		globalSz := 0

		inX := 0
		lastVal := (byte)(0)
		for {
			refs, genErr := g.ReadRef(elemPerInput)
			if genErr != io.EOF {
				require.Nilf(t, genErr, "Error while reading input %v", inX)
			}

			//process input
			inputSz := 0
			for refX, ref := range refs {
				inputSz += (int)(ref.NByte)

				reader, err := ref.Arr.GetPartRangeReader(ref.PartIdx, ref.Start, ref.Start+ref.NByte)
				require.Nilf(t, err, "Failed to get reader for %vth reference", refX)

				refRaw, err := ioutil.ReadAll(reader)
				require.Nil(t, err, "Failed to read from reference %v", refX)
				reader.Close()

				for i := 0; i < (int)(ref.NByte); i++ {
					require.GreaterOrEqual(t, refRaw[i], lastVal, "Input %v:%v returned out of order data at index %v", inX, refX, i)
					lastVal = refRaw[i]
				}

				globalSz += (int)(ref.NByte)
			}

			if genErr == io.EOF {
				// If EOF is given, the input size may be less than requested, but the total read must be right
				require.Equal(t, globalSz, nElem, "Read the wrong amount of data")
				break
			} else {
				// Non EOF next() calls must return the exact size
				require.Equalf(t, elemPerInput, inputSz, "Input %v returned wrong amount of data", inX)
				require.Less(t, globalSz, nElem, "Did not return EOF after reading enough data")
			}
			inX++
		}
	})

}

func sortDistribTest(t *testing.T, baseName string, factory *data.ArrayFactory, worker DistribWorker) {
	var err error

	err = InitLibSort()
	require.Nil(t, err, "Failed to initialize libsort")

	// Should be an odd (in both senses) number to pick up unaligned corner
	// cases
	nElem := 1111
	// nElem := (1024 * 1024) + 5
	origRaw, err := GenerateInputs((uint64)(nElem))
	require.Nil(t, err, "Failed to generate test inputs")

	outRaw, err := SortDistribFromRaw(origRaw, baseName, factory, worker)
	require.Nil(t, err, "Sort Error")

	err = CheckSort(origRaw, outRaw)
	require.Nilf(t, err, "Did not sort correctly: %v", err)
}

func TestSortMemDistrib(t *testing.T) {
	sortDistribTest(t, "TestSortMemDistrib", data.MemArrayFactory, LocalDistribWorker)
}

func TestSortFileDistrib(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "radixSortLocalTest")
	require.Nilf(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	sortDistribTest(t, tmpDir+"/", data.FileArrayFactory, LocalDistribWorker)
}
