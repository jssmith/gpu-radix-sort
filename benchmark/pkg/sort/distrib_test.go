package sort

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestLocalDistribWorker(t *testing.T) {
	DistribWorkerTest(t, data.MemArrayFactory, LocalDistribWorker)
}

func TestLocalDistribWorkerFile(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "radixSortLocalTest")
	require.Nilf(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	DistribWorkerTest(t, data.NewFileArrayFactory(tmpDir), LocalDistribWorker)
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
	shape := data.CreateShapeUniform((int64)(elemPerPart), npart)

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

func TestSortMemDistrib(t *testing.T) {
	SortDistribTest(t, "TestSortMemDistrib", data.MemArrayFactory, LocalDistribWorker)
}

func TestSortFileDistrib(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "radixSortLocalTest")
	require.Nilf(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	SortDistribTest(t, "testSortFileDistrib", data.NewFileArrayFactory(tmpDir), LocalDistribWorker)
}
