package sort

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/pkg/errors"
)

// Isolate the radix group from v (returns the groupID)
func GroupBits(v uint32, offset int, width int) int {
	return (int)((v >> offset) & ((1 << width) - 1))
}

func PrintHex(a []uint32) {
	for i, v := range a {
		fmt.Printf("%3v: 0x%08x\n", i, v)
	}
}

type ReadOrder int

const (
	INORDER ReadOrder = iota
	STRIDED
)

// Iterate a list of arrays by bucket (every array's part 0 then every array's
// part 1). Implements io.Reader.
type BucketReader struct {
	arrs  []data.DistribArray
	parts [][]data.DistribPart
	arrX  int // Index of next array to read from
	partX int // Index of next partition (bucket) to read from
	dataX int // Index of next address within the partition to read from
	nArr  int // Number of arrays
	nPart int // Number of partitions (should be fixed for each array)

	incIdx func() bool // Function to increment the index while iterating (modifies arrX and partX)
}

func NewBucketReader(sources []data.DistribArray, order ReadOrder) (*BucketReader, error) {
	var err error

	parts := make([][]data.DistribPart, len(sources))
	for i, arr := range sources {
		parts[i], err = arr.GetParts()
		if err != nil {
			return nil, err
		}
	}

	reader := &BucketReader{arrs: sources, parts: parts,
		arrX: 0, partX: 0,
		nArr: len(sources), nPart: len(parts[0]),
	}

	if order == INORDER {
		reader.incIdx = reader.incIdxInOrder
	} else if order == STRIDED {
		reader.incIdx = reader.incIdxStrided
	}

	return reader, nil
}

func (self *BucketReader) incIdxStrided() bool {
	self.arrX++
	if self.arrX >= self.nArr {
		self.arrX = 0
		self.partX++

		if self.partX >= self.nPart {
			return true
		}
	}
	return false
}

func (self *BucketReader) incIdxInOrder() bool {
	self.partX++
	if self.partX >= self.nPart {
		self.partX = 0
		self.arrX++

		if self.arrX >= self.nArr {
			return true
		}
	}
	return false
}

// Like Read but returns PartRefs instead of bytes
func (self *BucketReader) ReadRef(sz int) ([]*data.PartRef, error) {
	var out []*data.PartRef
	nNeeded := sz

	for done := false; !done; done = self.incIdx() {
		part := self.parts[self.arrX][self.partX]
		partLen, err := part.Len()
		if err != nil {
			return nil, errors.Wrapf(err, "Couldn't determine length of input %v:%v", self.arrX, self.partX)
		}

		for self.dataX < partLen {
			nRemaining := partLen - self.dataX

			var toWrite int
			if nRemaining <= nNeeded {
				toWrite = nRemaining
			} else {
				toWrite = nNeeded
			}
			out = append(out, &data.PartRef{Arr: self.arrs[self.arrX], PartIdx: self.partX, Start: self.dataX, NByte: toWrite})
			self.dataX += toWrite
			nNeeded -= toWrite

			if nNeeded == 0 {
				return out, nil
			}
		}
		self.dataX = 0
	}
	return out, io.EOF
}

func (self *BucketReader) Read(out []byte) (n int, err error) {
	nNeeded := len(out)
	outX := 0

	for done := false; !done; done = self.incIdx() {
		part := self.parts[self.arrX][self.partX]
		partLen, err := part.Len()
		if err != nil {
			return 0, errors.Wrapf(err, "Couldn't determine length of input %v:%v", self.arrX, self.partX)
		}

		for self.dataX < partLen {
			reader, err := part.GetRangeReader(self.dataX, 0)
			if err != nil {
				return outX, errors.Wrapf(err, "Couldnt read input %v:%v", self.arrX, self.partX)
			}

			nRead, readErr := reader.Read(out[outX:])
			reader.Close()

			self.dataX += nRead
			nNeeded -= nRead
			outX += nRead

			if readErr != io.EOF && readErr != nil {
				return outX, errors.Wrapf(err, "Failed to read from partition %v:%v", self.arrX, self.partX)
			} else if nNeeded == 0 {
				// There is a corner case where nNeeded==0 and
				// readErr==io.EOF. In this case, the next call to
				// BucketReader.Read() will re-read the partition and
				// immediately get EOF again, which is fine (if slightly
				// inefficient)
				return outX, nil
			} else if err == io.EOF {
				break
			}
		}
		self.dataX = 0
	}

	return outX, io.EOF
}

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

	sort.Slice(intOrig, func(i, j int) bool { return intOrig[i] < intOrig[j] })
	sort.Slice(intNew, func(i, j int) bool { return intNew[i] < intNew[j] })
	for i := 0; i < len(intOrig); i++ {
		if intOrig[i] != intNew[i] {
			// fmt.Println("Orig:")
			// PrintHex(intOrig)
			// fmt.Println("New:")
			// PrintHex(intNew)
			return fmt.Errorf("Response doesn't match reference at %v\n: Expected %v, Got %v\n", i, intOrig[i], intNew[i])
		}
	}

	// intOrigCpy := make([]uint32, len(intOrig))
	// copy(intOrigCpy, intOrig)
	// sort.Slice(intOrigCpy, func(i, j int) bool { return intOrigCpy[i] < intOrigCpy[j] })
	// for i := 0; i < len(intOrigCpy); i++ {
	// 	if intOrigCpy[i] != intNew[i] {
	// 		fmt.Println("Orig:")
	// 		PrintHex(intOrigCpy)
	// 		fmt.Println("New:")
	// 		PrintHex(intNew)
	// 		return fmt.Errorf("Response doesn't match reference at %v\n: Expected %v, Got %v\n", i, intOrigCpy[i], intNew[i])
	// 	}
	// }
	return nil
}
