package main

import (
	"encoding/binary"
	"io"
)

// Represents a partition of a DistribArray
type DistribPart interface {
	// Returns a reader that will returns bytes from the partition, starting
	// from the beginning.
	GetReader() io.ReadCloser

	// Returns a writer that will append to the partition
	GetWriter() io.WriteCloser
}

// Represents an array of bytes that is suitable for a distributed sort.
type DistribArray interface {
	GetParts() ([]DistribPart, error)
}

// A reference to an input partition
type partRef struct {
	Arr     *DistribArray // DistribArray to read from
	PartIdx int           // Partition to read from
	Start   int           // Offset to start reading
	NByte   int           // Number of bytes to read
}

// Read InBkts in order and sort by the radix of width width and starting at
// offset Returns a distributed array with one part per unique radix value
func distribWorker(inBkts []*partRef, offset int, width int) (*DistribArray, error) {
	var err error

	totalLen := 0
	for i := 0; i < len(inBkts); i++ {
		totalLen += inBkts[i].NBytes
	}
	nInt := totalLen / 4

	// Fetch data to local memory
	var inInts = make([]uint32, nInt)
	inPos := 0
	for i := 0; i < len(inBkts); i++ {
		bktRef := inBkts[i]
		parts, err := bktRef.GetParts()
		if err != nil {
			return nil, err
		}
		reader := io.LimitReader(parts[bktRef.PartIdx].GetReader(bktRef.Start), bktRef.NByte)
		err = binary.Read(reader, binary.LittleEndian, inInts[inPos:])
		if err != nil {
			return nil, err
		}
		inPos += bktRef.NByte / 4
		reader.Close()
	}

	// Actual Sort
	nBucket := 1 << width
	boundaries := make([]uint32, nBucket)
	if err := localSortPartial(inInts, boundaries, offset, width); err != nil {
		return nil, err
	}

	// Write Outputs
	outArr, err := NewMemDistribArray(nBucket)
	if err != nil {
		return nil, err
	}

	outParts, err := outArr.GetParts()
	if err != nil {
		return nil, err
	}

	for i := 0; i < nBucket; i++ {
		writer := outParts[i].GetWriter()
		start := boundaries[i]
		var end int
		if i == nBucket-1 {
			end = nInt
		} else {
			end = boundaries[i+1]
		}

		err = binary.Write(writer, binary.LittleEndian, inInts[start:end])
		if err != nil {
			writer.Close()
			return nil, err
		}
		writer.Close()
	}
	return &outArr, nil
}

type distribInputGenerator struct {
	arrs  []DistribArray
	parts [][]DistribPart
	arrX  int // Index of next array to read from
	partX int // Index of next partition (bucket) to read from
	dataX int // Index of next address within the partition to read from
	nArr  int // Number of arrays
	nPart int // Number of partitions (should be fixed for each array)
}

func newDistribInputGenerator(source []DistribArray) (*distribInputIterator, error) {
	var err error

	parts := make([][]DistribPart, len(source))
	for i, arr := range source {
		parts[i], err = arr.GetParts()
		if err != nil {
			return nil, err
		}
	}

	return &distribInputGenerator{arrs: source, parts: parts,
		arrX: 0, partX: 0,
		nArr: len(source), nPart: len(parts[0]),
	}
}

// Return the next group of partReferences to cover sz bytes. If there is no
// more data, returns io.EOF. The returned partRefs may not contain sz bytes in
// this case.
func (self *distribInputGenerator) next(sz int) ([]*partRef, error) {
	var out []*partRef

	nNeeded := sz
	for ; self.partX < self.nPart; self.partX++ {
		for ; self.arrX < nArr; self.arrX++ {
			part := self.parts[self.arrX][self.partX]
			for self.dataX < part.Len() {
				nRemaining := part.Len() - self.dataX

				if nRemaining <= nNeeded {
					toWrite := nRemaining
				} else {
					toWrite := nNeeded
				}
				out = append(out, &partRef{Arr: self.arrs[self.arrX], PartIdx: self.partX, Start: self.dataX, Nbyte: toWrite})
				self.dataX += toWrite
				nNeeded -= toWrite

				if nNeeded == 0 {
					return &out, nil
				}
			}
			self.dataX = 0
		}
		self.arrX = 0
	}
	return &out, io.EOF
}

// Distributed sort of arr. The bytes in arr will be interpreted as uint32's
// Returns an ordered list of distributed arrays containing the sorted output
// (concatenate each array's partitions in order to get final result)
func sortDistrib(arr DistribArray) ([]DistribArray, error) {
	// Data Layout:
	//	 - Distrib Arrays store all output from a single node
	//	 - DistribParts represent radix sort buckets (there will be nbucket parts per DistribArray)
	//
	// Basic algorithm/schema:
	//   - Inputs: each worker recieves as input a reference to the
	//     DistribParts it should consume. The first partition may include an
	//     offset to start reading from. Likewise, the last partition may include
	//     an offest to stop reading at. Intermediate partitions are read in
	//     their entirety.
	//	 - Outputs: Each worker will output a DistribArray with one partition
	//	   per radix bucket. Partitions may have zero length, but they will
	//	   always exist.
	//	 - Input distribArrays may be garbage collected after every worker has
	//     provided their output (output distribArrays are copies, not references).
	nworker := 2           //number of workers (degree of parallelism)
	width := 4             //number of bits to sort per round
	nbucket := (1 << step) //number of unique width-sized values
	nstep := (32 / width)  // number of steps needed to fully sort

	var err error

	// Target number of bytes to process per worker
	nPerWorker := (len(arr) / nworker)
	inputs := make([][]*partRef, nworker)
	outputs := make([]DistribArray, nworker)

	// Initial input is the output for "step -1"
	outputs[workerId] = []DistribArray{arr}

	for step := 0; step < nstep; step++ {
		inGen := newDistribInputGenerator(outputs)
		for workerId := 0; workerId < nworker; workerId++ {
			// Repartition previous output
			inputs, genErr := inGen.next(nPerWorker)
			if genErr == io.EOF {
				if len(inputs) == 0 {
					// iterator is allowed to issue EOF either with the last
					// data, or on the next call after all the data is read
					break
				}
			} else if genErr != nil {
				return nil, genErr
			}

			outputs[workerId], err = distribWorker(inputs, step*width, width)
			if err != nil {
				return nil, err
			}

			if genErr == io.EOF {
				break
			}
		}
	}
}
