package sort

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"testing"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/stretchr/testify/require"
)

func TestLocalDistribWorker(t *testing.T) {
	var err error

	nElem := 1021
	width := 4
	npart := 1 << width

	err = InitLibSort()
	require.Nil(t, err, "Failed to initialize libsort")

	origRaw := RandomInputs(nElem)

	origArr, err := data.NewMemDistribArray(1)
	require.Nil(t, err)

	parts, err := origArr.GetParts()
	require.Nil(t, err)

	writer, err := parts[0].GetWriter()
	require.Nil(t, err, "Failed to get writer")

	err = binary.Write(writer, binary.LittleEndian, origRaw)
	require.Nil(t, err)
	writer.Close()

	PartRefs := []*PartRef{&PartRef{Arr: origArr, PartIdx: 0, Start: 0, NByte: (int64)(nElem * 4)}}

	outArr, err := localDistribWorker(PartRefs, 0, width)
	require.Nil(t, err)

	outParts, err := outArr.GetParts()
	require.Nil(t, err)
	require.Equal(t, npart, len(outParts), "Did not return the right number of output partitions")

	outRaw := make([]uint32, nElem)
	boundaries := make([]uint32, npart)
	totalLen := 0
	for i, p := range outParts {
		partLen, err := p.Len()
		require.Nilf(t, err, "Failed to determine length of output partition %v", i)

		bucketLen := (int)(partLen / 4)

		boundaries[i] = (uint32)(totalLen)

		totalLen += bucketLen
		require.LessOrEqual(t, totalLen, nElem, "Too much data returned")

		reader, err := p.GetReader()
		require.Nilf(t, err, "Failed to get reader for output partition %v", i)

		err = binary.Read(reader, binary.LittleEndian, outRaw[boundaries[i]:totalLen])
		require.Nilf(t, err, "Failed to read bucket %v", i)
		reader.Close()
	}

	require.Equal(t, nElem, totalLen, "Output buckets have the wrong number of elements")

	checkPartial(t, outRaw, boundaries, origRaw)
}

func TestFetchDistribArrays(t *testing.T) {
	var err error
	sz := 64
	narr := 2
	npart := 2
	partSz := sz / (narr * npart)
	arrSz := npart * partSz
	rawIn := RandomInputs(sz)

	arrs := make([]data.DistribArray, narr)
	for arrX := 0; arrX < narr; arrX++ {
		arrs[arrX], err = data.NewMemDistribArray(npart)
		require.Nilf(t, err, "Failed to build array %v", arrX)

		parts, err := arrs[arrX].GetParts()
		require.Nilf(t, err, "Failed to get parts from array %v", arrX)

		for partX := 0; partX < npart; partX++ {
			startIdx := (arrX * arrSz) + (partX * partSz)

			writer, err := parts[partX].GetWriter()
			require.Nilf(t, err, "Failed to get writer for partition %v", partX)

			err = binary.Write(writer, binary.LittleEndian, rawIn[startIdx:startIdx+partSz])
			require.Nil(t, err, "Failed to write initial data")
			writer.Close()
		}
	}

	out, err := FetchDistribArrays(arrs)
	require.Nil(t, err, "Fetching distrib arrays returned an error")
	require.Equal(t, len(rawIn), len(out), "Output has wrong number of elements")

	for i := 0; i < sz; i++ {
		require.Equalf(t, rawIn[i], out[i], "Output has wrong value at index %v", i)
	}
}

// Generates a list of narr arrays with npart partitions each, with elemPerPart
// elements per partition. The value in each partition will be (partId << 4 |
// arrId). This means that a strided access will be in-order (mimicking the
// output of a radix sort). narr and npart must be < 16.
func generateArrs(t *testing.T, narr, npart, elemPerPart int) []data.DistribArray {
	var err error

	arrs := make([]data.DistribArray, narr)
	for arrX := 0; arrX < narr; arrX++ {
		arrs[arrX], err = data.NewMemDistribArray(npart)
		require.Nilf(t, err, "Failed to build array %v", arrX)

		parts, err := arrs[arrX].GetParts()
		require.Nilf(t, err, "Failed to get parts from array %v", arrX)

		for partX := 0; partX < npart; partX++ {
			// Total data will be ordered by partition ID first, then by array
			// (the strided access by the generator should produce in-order
			// data)
			partRaw := bytes.Repeat([]byte{(byte)((partX << 4) | arrX)}, elemPerPart)

			writer, err := parts[partX].GetWriter()
			require.Nilf(t, err, "Failed to get writer for output %v:%v", arrX, partX)

			n, err := writer.Write(partRaw)
			require.Equal(t, elemPerPart, n, "Didn't write enough to initial data")
			require.Nil(t, err, "Failed to write initial data")
			writer.Close()
		}
	}

	return arrs
}

func TestBucketReader(t *testing.T) {
	narr := 2
	npart := 2
	elemPerPart := 256
	nElem := narr * npart * elemPerPart

	// Given the global 'index' of 'value' taken from an array list returned by
	// generateArrs, ensures that the value is correct for that index.
	checker := func(t *testing.T, index int, value byte) {
		outArrId := (int)(value & 0xf)
		outPartId := (int)(value >> 4)

		globalPart := index / elemPerPart
		expectPartId := globalPart / narr
		expectArrId := globalPart % npart

		// t.Logf("Checking %v: 0x%04v", index, value)
		// t.Logf("Expecting %v:%v", expectArrId, expectPartId)
		// t.Logf("Got %v:%v", outArrId, outPartId)

		require.Equal(t, expectPartId, outPartId, "Partitions out of order")
		require.Equal(t, expectArrId, outArrId, "Arrays out of order")
	}
	arrs := generateArrs(t, narr, npart, elemPerPart)

	t.Run("All", func(t *testing.T) {
		iter, err := NewBucketReader(arrs)

		out, err := ioutil.ReadAll(iter)
		require.Nil(t, err, "Failed to read from iterator")
		require.Equal(t, len(out), nElem, "Iterator returned wrong number of bytes")

		for i, v := range out {
			checker(t, i, v)
		}
	})

	t.Run("Unaligned", func(t *testing.T) {
		iter, err := NewBucketReader(arrs)
		require.Nil(t, err, "Failed to create BucketReader")

		// Read almost 1 partition per read, but not exact to prevent reads
		// from aligning perfectly with partition boundaries
		readSz := elemPerPart - 1
		out := make([]byte, readSz)
		for i := 0; i < npart*narr; i++ {
			n, err := iter.Read(out)

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
				checker(t, globalIdx, v)
			}
		}
	})
}

func TestBucketRefIterator(t *testing.T) {
	narr := 2
	npart := 2
	elemPerPart := 256
	nElem := narr * npart * elemPerPart

	arrs := generateArrs(t, narr, npart, elemPerPart)

	t.Run("Aligned", func(t *testing.T) {
		g, err := NewBucketRefIterator(arrs)
		require.Nil(t, err, "Couldn't initialize generator")

		for i := 0; i < npart*narr; i++ {
			refs, err := g.Next((int64)(elemPerPart))
			require.Nilf(t, err, "Failed to get %vth reference from generator", i)
			require.Equal(t, 1, len(refs), "Returned too many references")
			require.Equal(t, arrs[i%narr], refs[0].Arr, "DistribArrays returned in wrong order")
			require.Equal(t, i/narr, refs[0].PartIdx, "Partitions returned in wrong order")
			require.Equal(t, (int64)(0), refs[0].Start, "Reference should start from beginning")
			require.Equal(t, (int64)(elemPerPart), refs[0].NByte, "Reference has wrong size")
		}
		refs, err := g.Next((int64)(elemPerPart))
		require.Equal(t, io.EOF, err, "Generator did not return EOF")
		require.Zero(t, len(refs), "Returned too much data")
	})

	// Reads not aligned to partition boundaries, the generator will have to
	// split inputs across partitions.
	t.Run("Unaligned", func(t *testing.T) {
		g, err := NewBucketRefIterator(arrs)
		require.Nil(t, err, "Couldn't initialize generator")

		elemPerInput := elemPerPart - 1
		globalSz := 0

		inX := 0
		lastVal := (byte)(0)
		for {
			refs, genErr := g.Next((int64)(elemPerInput))
			if genErr != io.EOF {
				require.Nilf(t, genErr, "Error while reading input %v", inX)
			}

			//process input
			inputSz := 0
			for refX, ref := range refs {
				inputSz += (int)(ref.NByte)

				refParts, err := ref.Arr.GetParts()
				require.Nilf(t, err, "Input %v:%v: failed to read partitions", inX, refX)

				reader, err := refParts[ref.PartIdx].GetRangeReader(ref.Start, ref.Start+ref.NByte)
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

func TestSortDistrib(t *testing.T) {
	var err error

	err = InitLibSort()
	require.Nil(t, err, "Failed to initialize libsort")

	// Should be an odd (in both senses) number to pick up unaligned corner
	// cases
	nElem := 1111
	// nElem := (1024 * 1024) + 5
	origRaw := RandomInputs(nElem)

	origArr, err := data.NewMemDistribArray(1)
	require.Nil(t, err)

	parts, err := origArr.GetParts()
	require.Nil(t, err)

	writer, err := parts[0].GetWriter()
	require.Nilf(t, err, "Failed to get writer for partition")

	err = binary.Write(writer, binary.LittleEndian, origRaw)
	require.Nil(t, err)
	writer.Close()

	outArrs, err := SortDistrib(origArr, (int64)(nElem))
	require.Nilf(t, err, "Sort returned an error: %v", err)

	reader, err := NewBucketReader(outArrs)
	require.Nil(t, err, "Failed to create bucket iterator")

	outRaw := make([]uint32, nElem)
	err = binary.Read(reader, binary.LittleEndian, outRaw)
	require.Nil(t, err, "Failed while reading output")

	prev := outRaw[0]
	for i := 0; i < nElem; i++ {
		require.GreaterOrEqualf(t, outRaw[i], prev, "output not in order at index %v", i)
		prev = outRaw[i]
	}

	// sort.Slice(outRaw, func(i, j int) bool { return outRaw[i] < outRaw[j] })
	err = CheckSort(origRaw, outRaw)
	// fmt.Printf("output:\n")
	// sort.Slice(outRaw, func(i, j int) bool { return outRaw[i] < outRaw[j] })
	// PrintHex(outRaw)
	// fmt.Printf("\n\nReference:\n")
	// sort.Slice(origRaw, func(i, j int) bool { return origRaw[i] < origRaw[j] })
	// PrintHex(origRaw)
	require.Nilf(t, err, "Did not sort correctly: %v", err)
}
