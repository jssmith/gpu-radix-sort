package sort

import (
	"encoding/binary"
	"io"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/pkg/errors"
)

// A reference to an input partition
type PartRef struct {
	Arr     data.DistribArray // DistribArray to read from
	PartIdx int               // Partition to read from
	Start   int64             // Offset to start reading
	NByte   int64             // Number of bytes to read
}

// Read InBkts in order and sort by the radix of width width and starting at
// offset Returns a distributed array with one part per unique radix value
func localDistribWorker(inBkts []*PartRef, offset int, width int) (data.DistribArray, error) {
	var err error

	totalLen := (int64)(0)
	for i := 0; i < len(inBkts); i++ {
		totalLen += inBkts[i].NByte
	}
	nInt := totalLen / 4

	// Fetch data to local memory
	var inInts = make([]uint32, nInt)
	inPos := (int64)(0)
	for i := 0; i < len(inBkts); i++ {
		bktRef := inBkts[i]
		parts, err := bktRef.Arr.GetParts()
		if err != nil {
			return nil, errors.Wrapf(err, "Couldn't get partitions from input ref %v", i)
		}
		reader := parts[bktRef.PartIdx].GetRangeReader(bktRef.Start, bktRef.Start+bktRef.NByte)
		err = binary.Read(reader, binary.LittleEndian, inInts[inPos:inPos+(bktRef.NByte/4)])
		if err != nil {
			return nil, errors.Wrapf(err, "Couldn't read from input ref %v", i)
		}
		inPos += bktRef.NByte / 4
		reader.Close()
	}

	// Actual Sort
	nBucket := 1 << width
	boundaries := make([]uint32, nBucket)
	if err := localSortPartial(inInts, boundaries, offset, width); err != nil {
		return nil, errors.Wrap(err, "Local sort failed")
	}

	// Write Outputs
	outArr, err := data.NewMemDistribArray(nBucket)
	if err != nil {
		return nil, errors.Wrap(err, "Could not allocate outpt")
	}

	outParts, err := outArr.GetParts()
	if err != nil {
		return nil, errors.Wrap(err, "Output array failure")
	}

	for i := 0; i < nBucket; i++ {
		writer := outParts[i].GetWriter()
		start := boundaries[i]
		var end int64
		if i == nBucket-1 {
			end = nInt
		} else {
			end = (int64)(boundaries[i+1])
		}

		err = binary.Write(writer, binary.LittleEndian, inInts[start:end])
		if err != nil {
			writer.Close()
			return nil, errors.Wrap(err, "Could not write to output")
		}
		writer.Close()
	}
	return outArr, nil
}

// Iterate a list of arrays by bucket (every array's part 0 then every array's
// part 1). Implements io.Reader.
type BucketReader struct {
	arrs  []data.DistribArray
	parts [][]data.DistribPart
	arrX  int   // Index of next array to read from
	partX int   // Index of next partition (bucket) to read from
	dataX int64 // Index of next address within the partition to read from
	nArr  int   // Number of arrays
	nPart int   // Number of partitions (should be fixed for each array)
}

func NewBucketReader(sources []data.DistribArray) (*BucketReader, error) {
	var err error

	parts := make([][]data.DistribPart, len(sources))
	for i, arr := range sources {
		parts[i], err = arr.GetParts()
		if err != nil {
			return nil, err
		}
	}

	return &BucketReader{arrs: sources, parts: parts,
		arrX: 0, partX: 0,
		nArr: len(sources), nPart: len(parts[0]),
	}, nil
}

func (self *BucketReader) Read(out []byte) (n int, err error) {
	nNeeded := len(out)
	outX := 0
	for ; self.partX < self.nPart; self.partX++ {
		for ; self.arrX < self.nArr; self.arrX++ {
			part := self.parts[self.arrX][self.partX]
			for self.dataX < part.Len() {
				reader := part.GetRangeReader(self.dataX, 0)
				nRead, readErr := reader.Read(out[outX:])
				reader.Close()

				self.dataX += (int64)(nRead)
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
		self.arrX = 0
	}
	return outX, io.EOF
}

// Same as BucketReader but returns PartRef's instead of bytes (doesn't
// implement io.Reader but has similar behavior).
type BucketRefIterator struct {
	arrs  []data.DistribArray
	parts [][]data.DistribPart
	arrX  int   // Index of next array to read from
	partX int   // Index of next partition (bucket) to read from
	dataX int64 // Index of next address within the partition to read from
	nArr  int   // Number of arrays
	nPart int   // Number of partitions (should be fixed for each array)
}

func NewBucketRefIterator(source []data.DistribArray) (*BucketRefIterator, error) {
	var err error

	parts := make([][]data.DistribPart, len(source))
	for i, arr := range source {
		parts[i], err = arr.GetParts()
		if err != nil {
			return nil, err
		}
	}

	return &BucketRefIterator{arrs: source, parts: parts,
		arrX: 0, partX: 0,
		nArr: len(source), nPart: len(parts[0]),
	}, nil
}

// Return the next group of PartReferences to cover sz bytes. If there is no
// more data, returns io.EOF. The returned PartRefs may not contain sz bytes in
// this case.
func (self *BucketRefIterator) Next(sz int64) ([]*PartRef, error) {
	var out []*PartRef

	nNeeded := sz
	for ; self.partX < self.nPart; self.partX++ {
		for ; self.arrX < self.nArr; self.arrX++ {
			part := self.parts[self.arrX][self.partX]
			for self.dataX < part.Len() {
				nRemaining := part.Len() - self.dataX

				var toWrite int64
				if nRemaining <= nNeeded {
					toWrite = nRemaining
				} else {
					toWrite = nNeeded
				}
				out = append(out, &PartRef{Arr: self.arrs[self.arrX], PartIdx: self.partX, Start: self.dataX, NByte: toWrite})
				self.dataX += toWrite
				nNeeded -= toWrite

				if nNeeded == 0 {
					return out, nil
				}
			}
			self.dataX = 0
		}
		self.arrX = 0
	}
	return out, io.EOF
}

// Distributed sort of arr. The bytes in arr will be interpreted as uint32's
// Returns an ordered list of distributed arrays containing the sorted output
// (concatenate each array's partitions in order to get final result). 'size' is
// the number of bytes in arr.
func SortDistrib(arr data.DistribArray, size int64) ([]data.DistribArray, error) {
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
	nworker := 2          //number of workers (degree of parallelism)
	width := 4            //number of bits to sort per round
	nstep := (32 / width) // number of steps needed to fully sort
	// nstep := 1 // number of steps needed to fully sort

	// Target number of bytes to process per worker
	nPerWorker := (size / (int64)(nworker))

	// Initial input is the output for "step -1"
	var outputs []data.DistribArray
	outputs = []data.DistribArray{arr}

	for step := 0; step < nstep; step++ {
		inputs := outputs
		outputs = make([]data.DistribArray, nworker)

		inGen, err := NewBucketRefIterator(inputs)
		if err != nil {
			return nil, err
		}

		for workerId := 0; workerId < nworker; workerId++ {
			// Repartition previous output
			workerInputs, genErr := inGen.Next(nPerWorker)
			if genErr == io.EOF {
				if len(workerInputs) == 0 {
					// iterator is allowed to issue EOF either with the last
					// data, or on the next call after all the data is read
					break
				}
			} else if genErr != nil {
				return nil, errors.Wrapf(genErr, "Couldn't generate input for step %v worker %v", step, workerId)
			}

			outputs[workerId], err = localDistribWorker(workerInputs, step*width, width)
			if err != nil {
				return nil, errors.Wrapf(err, "Worker failure on step %v, worker %v", step, workerId)
			}

			if genErr == io.EOF {
				break
			}
		}
	}
	return outputs, nil
}
