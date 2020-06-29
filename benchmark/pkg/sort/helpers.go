package sort

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/pkg/errors"
)

func PrintHex(a []uint32) {
	for i, v := range a {
		fmt.Printf("%3v: 0x%08x\n", i, v)
	}
}

func RandomInputs(len int) []uint32 {
	rand.Seed(0)
	out := make([]uint32, len)
	for i := 0; i < len; i++ {
		out[i] = rand.Uint32()
	}
	return out
}

// Coalesce the data in arrs into a single slice. Data will be read strided by
// partition (all part0's first, then all part 1's etc)
func FetchDistribArraysStrided(arrs []data.DistribArray, npart int) ([]uint32, error) {
	var err error
	var outs [][]uint32

	parts := make([][]data.DistribPart, len(arrs))
	for arrX, arr := range arrs {
		parts[arrX], err = arr.GetParts()
		if err != nil {
			return nil, errors.Wrapf(err, "Couldn't get partitions from array %v", arrX)
		}
	}

	totalLen := (int64)(0)
	for partX := 0; partX < npart; partX++ {
		for arrX := 0; arrX < len(arrs); arrX++ {
			part := parts[arrX][partX]
			nPartElem := part.Len() / 4
			partOut := make([]uint32, nPartElem)

			reader := part.GetReader()
			err = binary.Read(reader, binary.LittleEndian, partOut)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to read from array %v, part %v", arrX, partX)
			}

			reader.Close()

			outs = append(outs, partOut)
			totalLen += nPartElem
		}
	}

	final := make([]uint32, 0, totalLen)
	for i := 0; i < len(outs); i++ {
		final = append(final, outs[i]...)
	}
	return final, nil
}

// Coalesce the data in arrs into a single slice. Data will be read in order.
func FetchDistribArrays(arrs []data.DistribArray) ([]uint32, error) {
	var outs [][]uint32

	totalLen := (int64)(0)
	for arrX, arr := range arrs {
		parts, err := arr.GetParts()
		if err != nil {
			return nil, errors.Wrapf(err, "Couldn't get partitions from array %v", arrX)
		}

		for partX, part := range parts {
			nPartElem := part.Len() / 4
			partOut := make([]uint32, nPartElem)

			reader := part.GetReader()
			err = binary.Read(reader, binary.LittleEndian, partOut)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to read from array %v, part %v", arrX, partX)
			}

			reader.Close()

			outs = append(outs, partOut)
			totalLen += nPartElem
		}
	}

	final := make([]uint32, 0, totalLen)
	for i := 0; i < len(outs); i++ {
		final = append(final, outs[i]...)
	}
	return final, nil
}

func CheckSort(orig []uint32, new []uint32) error {
	if len(orig) != len(new) {
		return fmt.Errorf("Lengths do not match: Expected %v, Got %v\n", len(orig), len(new))
	}

	origCpy := make([]uint32, len(orig))
	copy(origCpy, orig)
	sort.Slice(origCpy, func(i, j int) bool { return origCpy[i] < origCpy[j] })
	for i := 0; i < len(orig); i++ {
		if origCpy[i] != new[i] {
			return fmt.Errorf("Response doesn't match reference at %v\n: Expected %v, Got %v\n", i, origCpy[i], new[i])
		}
	}
	return nil
}
