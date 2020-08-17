package sort

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"testing"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func CheckSort(orig []byte, new []byte) error {
	var err error

	if len(orig) != len(new) {
		return fmt.Errorf("Lengths do not match: Expected %v, Got %v\n", len(orig), len(new))
	}

	intOrig := make([]uint32, len(orig)/4)
	intNew := make([]uint32, len(new)/4)

	err = binary.Read(bytes.NewReader(orig), binary.LittleEndian, intOrig)
	if err != nil {
		return errors.Wrap(err, "Couldn't interpret orig")
	}

	err = binary.Read(bytes.NewReader(new), binary.LittleEndian, intNew)
	if err != nil {
		return errors.Wrap(err, "Couldn't interpret new")
	}

	// Set membership test
	// intOrigCpy := make([]uint32, len(intOrig))
	// intNewCpy := make([]uint32, len(intNew))
	// copy(intOrigCpy, intOrig)
	// copy(intNewCpy, intNew)
	// sort.Slice(intOrigCpy, func(i, j int) bool { return intOrigCpy[i] < intOrigCpy[j] })
	// sort.Slice(intNewCpy, func(i, j int) bool { return intNewCpy[i] < intNewCpy[j] })
	// for i := 0; i < len(intOrigCpy); i++ {
	// 	if intOrigCpy[i] != intNewCpy[i] {
	// 		fmt.Printf("Response doesn't have same elements as ref at %v\n: Expected %v, Got %v\n", i, intOrigCpy[i], intNew[i])
	// 		// return fmt.Errorf("Response doesn't have same elements as ref at %v\n: Expected %v, Got %v\n", i, intOrigCpy[i], intNew[i])
	// 	}
	// }

	// In order test
	// prev := (uint32)(0)
	// nerr := 0
	// for i := 0; i < len(intNew); i++ {
	// 	// fmt.Printf("%v: 0x%08x\n", i, intNew[i])
	// 	if intNew[i] < prev {
	// 		// fmt.Printf("Out of order at index %v:\t%x < %x\n", i, intNew[i], prev)
	// 		nerr += 1
	// 		return fmt.Errorf("Out of order at index %v: 0x%08x < 0x%08x", i, intNew[i], prev)
	// 	}
	// 	prev = intNew[i]
	// }
	// fmt.Printf("Nerror: %v\n", nerr)

	// Full match against orig
	intOrigCpy := make([]uint32, len(intOrig))
	copy(intOrigCpy, intOrig)
	sort.Slice(intOrigCpy, func(i, j int) bool { return intOrigCpy[i] < intOrigCpy[j] })
	for i := 0; i < len(intOrigCpy); i++ {
		if intOrigCpy[i] != intNew[i] {
			return fmt.Errorf("Response doesn't match reference at %v\n: Expected %v, Got %v\n", i, intOrigCpy[i], intNew[i])
		}
	}
	return nil
}

func CheckPartialArray(arr data.DistribArray, offset, width int) error {
	reader, err := NewBucketReader([]data.DistribArray{arr}, INORDER)
	if err != nil {
		return errors.Wrap(err, "Failed to get reader for output")
	}

	testRaw, err := ioutil.ReadAll(reader)
	if err != nil {
		return errors.Wrap(err, "couldn't read input")
	}
	testInts := make([]uint32, len(testRaw)/4)

	err = binary.Read(bytes.NewReader(testRaw), binary.LittleEndian, testInts)
	if err != nil {
		return errors.Wrap(err, "Couldn't interpret output")
	}

	shape, err := arr.GetShape()
	if err != nil {
		return errors.Wrap(err, "Couldn't get shape of input")
	}
	boundaries := make([]uint64, shape.NPart()+1)

	sum := (uint64)(len(testInts))
	boundaries[shape.NPart()] = sum
	for i := shape.NPart() - 1; i > 0; i-- {
		sum -= (uint64)(shape.Len(i) / 4)
		boundaries[i] = sum
	}

	curGroup := 0
	for i := 0; i < len(testInts); i++ {
		for (uint64)(i) == boundaries[curGroup+1] {
			curGroup++
		}
		group := GroupBits(testInts[i], offset, width)
		if group != curGroup {
			return fmt.Errorf("Element %v in wrong group: expected %v, got %v", i, curGroup, group)
			// fmt.Printf("(%v:%v) Element %v (0x%x) in wrong group: expected %x, got %x\n", offset, width, i, testInts[i], curGroup, group)
		}

	}

	return nil
}

// Used primarily by bucketReader tests.
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
			partRaw := bytes.Repeat([]byte{(byte)((partX << 4) | arrX)}, (int)(shape.Cap(partX)))

			writer, err := arrs[arrX].GetPartWriter(partX)
			require.Nilf(t, err, "Failed to get writer for output %v:%v", arrX, partX)

			n, err := writer.Write(partRaw)
			require.Equal(t, (int)(shape.Cap(partX)), n, "Didn't write enough to initial data")
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
	uniformShape := data.CreateShapeUniform((int64)(elemPerPart), npart)

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
					sum += (int)(shape.Cap(partX))
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
					sum += (int)(shape.Cap(partX))
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
		caps := make([]int64, npart)
		for i := 0; i < npart; i++ {
			if i%zeroRatio == 0 {
				caps[i] = (int64)(elemPerPart)
			} else {
				caps[i] = (int64)(0)
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

func DistribWorkerTest(t *testing.T, factory *data.ArrayFactory, worker DistribWorker) {
	var err error

	nElem := 1021
	nByte := nElem * 4
	width := 4
	npart := 1 << width

	err = InitLibSort()
	require.Nil(t, err, "Failed to initialize libsort")

	origRaw, err := GenerateInputs((uint64)(nElem))
	require.Nil(t, err, "Failed to generate test inputs")

	shape := data.CreateShapeUniform((int64)(nByte), 1)
	origArr, err := factory.Create("initial", shape)
	require.Nil(t, err)

	writer, err := origArr.GetPartWriter(0)
	require.Nil(t, err, "Failed to get writer")

	n, err := writer.Write(origRaw)
	require.Nil(t, err)
	require.Equal(t, n, nByte)
	writer.Close()

	PartRefs := []*data.PartRef{&data.PartRef{Arr: origArr, PartIdx: 0, Start: 0, NByte: (nElem / 2) * 4},
		&data.PartRef{Arr: origArr, PartIdx: 0, Start: (nElem / 2) * 4, NByte: (nElem - (nElem / 2)) * 4}}

	origArr.Close()

	outArr, err := worker(PartRefs, 0, width, "testDistribWorker", factory)
	require.Nil(t, err)

	outShape, err := outArr.GetShape()
	require.Nil(t, err, "Failed to get output array shape")
	require.Equal(t, npart, outShape.NPart(), "Output array has wrong number of partitions")

	outRaw := make([]byte, nByte)
	boundaries := make([]int64, npart)
	totalLen := 0

	for i := 0; i < outShape.NPart(); i++ {
		partLen := (int)(outShape.Len(i))

		boundaries[i] = (int64)(totalLen)

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

func SortDistribTest(t *testing.T, baseName string, factory *data.ArrayFactory, worker DistribWorker) {
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

// Make sure the partial sort worked and set the boundaries correctly
func checkPartial(t *testing.T, testBytes []byte, boundaries []int64, origBytes []byte) {
	var err error

	require.Equal(t, len(origBytes), len(testBytes), "Test array has the wrong length")

	test := make([]uint32, len(testBytes)/4)
	orig := make([]uint32, len(origBytes)/4)

	intLen := len(test)

	err = binary.Read(bytes.NewReader(origBytes), binary.LittleEndian, orig)
	require.Nil(t, err, "Couldn't interpret orig")

	err = binary.Read(bytes.NewReader(testBytes), binary.LittleEndian, test)
	require.Nil(t, err, "Couldn't interpret test")

	// len(boundaries) is 2^radixWidth, -1 gives us ones for the first width bits
	mask := (uint32)(len(boundaries) - 1)

	boundaries = append(boundaries, (int64)(len(testBytes)))
	curBucket := (uint32)(0)
	for i := 0; i < intLen; i++ {
		for i == (int)(boundaries[curBucket+1])/4 {
			curBucket++
		}

		bucket := test[i] & mask
		require.Equal(t, curBucket, bucket, "Buckets not in order")
	}

	// Make sure all the right values are in the output, the sort here is just
	// to compare set membership.
	sort.Slice(orig, func(i, j int) bool { return orig[i] < orig[j] })
	sort.Slice(test, func(i, j int) bool { return test[i] < test[j] })
	for i := 0; i < intLen; i++ {
		require.Equalf(t, orig[i], test[i], "output does not contain all the same values as the input at index %v", i)
	}
}
