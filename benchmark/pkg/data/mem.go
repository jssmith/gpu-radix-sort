package data

import (
	"fmt"
	"io"
)

// A place to store MemDistribArray data in between create and close calls.
var memArrBacking map[string]*MemDistribArray = map[string]*MemDistribArray{}

// A write-closer for MemDistrib, close is a nop in this case
type MemDistribPartWriteCloser struct {
	arr    *MemDistribArray
	partId int
}

// A read-closer for MemDistrib, close is a nop in this case
type MemDistribPartReadCloser struct {
	buf []byte

	// Start and limit act like slice indices (reader will return [start, limit])
	start int
	limit int
}

func (self *MemDistribPartWriteCloser) Write(in []byte) (n int, err error) {
	shape := self.arr.shape

	toWrite := len(in)
	nRemaining := shape.caps[self.partId] - shape.lens[self.partId]

	if toWrite > nRemaining {
		toWrite = nRemaining
		err = io.EOF
	}

	self.arr.parts[self.partId] = append(self.arr.parts[self.partId], in[:toWrite]...)
	shape.lens[self.partId] += toWrite

	return toWrite, err
}

func (self *MemDistribPartWriteCloser) Close() error {
	return nil
}

func (self *MemDistribPartReadCloser) Read(dst []byte) (n int, err error) {
	n = copy(dst, self.buf[self.start:self.limit])
	self.start += n

	if self.start == self.limit {
		err = io.EOF
	} else {
		err = nil
	}

	return
}

func (self *MemDistribPartReadCloser) Close() error {
	return nil
}

// In-memory 'distributed' array. Does not provide any persistence and cannot
// share between processes (only threads in the same address space).
type MemDistribArray struct {
	name  string
	shape DistribArrayShape
	parts [][]byte
}

func CreateMemDistribArray(name string, shape DistribArrayShape) (*MemDistribArray, error) {
	if _, ok := memArrBacking[name]; ok {
		return nil, fmt.Errorf("Array %v exists", name)
	}

	// Deep copy because MemDistribArray modifies these values internally, even though the user can't
	arrShape := DistribArrayShape{caps: make([]int, len(shape.caps)), lens: make([]int, len(shape.lens))}
	copy(arrShape.caps, shape.caps)
	copy(arrShape.lens, shape.lens)

	arr := &MemDistribArray{name: name, shape: arrShape}

	arr.parts = make([][]byte, len(shape.caps))
	for i := 0; i < len(shape.caps); i++ {
		arr.parts[i] = make([]byte, arrShape.lens[i], arrShape.caps[i])
	}

	memArrBacking[name] = arr

	return arr, nil
}

func OpenMemDistribArray(name string) (*MemDistribArray, error) {

	arr, ok := memArrBacking[name]
	if !ok {
		return nil, fmt.Errorf("Array %v does not exist", name)
	}

	return arr, nil
}

func (self *MemDistribArray) GetShape() (*DistribArrayShape, error) {
	// Copy the slices but not their underlying array (DistribArrayShape is immutable by clients)
	return &DistribArrayShape{lens: self.shape.lens, caps: self.shape.caps}, nil
}

func (self *MemDistribArray) GetPartRangeReader(partId, start, end int) (io.ReadCloser, error) {
	if end <= 0 {
		return &MemDistribPartReadCloser{buf: self.parts[partId], start: start, limit: self.shape.caps[partId] + end}, nil
	} else {
		return &MemDistribPartReadCloser{buf: self.parts[partId], start: start, limit: end}, nil
	}
}

func (self *MemDistribArray) GetPartReader(partId int) (io.ReadCloser, error) {
	return self.GetPartRangeReader(partId, 0, 0)
}

func (self *MemDistribArray) GetPartWriter(partId int) (io.WriteCloser, error) {
	return &MemDistribPartWriteCloser{arr: self, partId: partId}, nil
}

func (self *MemDistribArray) Close() error {
	return nil
}

func (self *MemDistribArray) Destroy() error {
	delete(memArrBacking, self.name)
	return nil
}
