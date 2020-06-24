package data

import (
	"io"
)

// A write-closer for MemDistrib, close is a nop in this case
type MemDistribPartWriteCloser struct {
	p *MemDistribPart
}

func (self *MemDistribPartWriteCloser) Write(in []byte) (n int, err error) {
	self.p.buf = append(self.p.buf, in...)
	return len(in), nil
}

func (self *MemDistribPartWriteCloser) Close() error {
	return nil
}

// A read-closer for MemDistrib, close is a nop in this case
type MemDistribPartReadCloser struct {
	p *MemDistribPart // Reference to parent partition

	// Start and limit act like slice indices (reader will return [start, limit])
	start int64
	limit int64
}

func (self *MemDistribPartReadCloser) Read(dst []byte) (n int, err error) {
	n = copy(dst, self.p.buf[self.start:self.limit])
	self.start += (int64)(n)

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

// Partition of a MemDistribArray
type MemDistribPart struct {
	buf []byte
}

func NewMemDistribPart(p []byte) (*MemDistribPart, error) {
	return &MemDistribPart{buf: p}, nil
}

func (self *MemDistribPart) GetRangeReader(start, end int64) io.ReadCloser {
	if end <= 0 {
		return &MemDistribPartReadCloser{p: self, start: start, limit: (int64)(len(self.buf)) + end}
	} else {
		return &MemDistribPartReadCloser{p: self, start: start, limit: end}
	}
}

func (self *MemDistribPart) GetReader() io.ReadCloser {
	return self.GetRangeReader(0, 0)
}

func (self *MemDistribPart) GetWriter() io.WriteCloser {
	return &MemDistribPartWriteCloser{p: self}
}

func (self *MemDistribPart) Len() int64 {
	return (int64)(len(self.buf))
}

// In-memory 'distributed' array. Does not provide any persistence and cannot
// share between processes (only threads in the same address space).
type MemDistribArray struct {
	parts []*MemDistribPart
}

func NewMemDistribArray(npart int) (*MemDistribArray, error) {
	var err error
	parts := make([]*MemDistribPart, npart)
	for i := 0; i < npart; i++ {
		parts[i], err = NewMemDistribPart([]byte{})
		if err != nil {
			return nil, err
		}
	}
	arr := &MemDistribArray{
		parts: parts,
	}
	return arr, nil
}

func (self *MemDistribArray) GetParts() ([]DistribPart, error) {
	genericParts := make([]DistribPart, len(self.parts))
	for i, p := range self.parts {
		genericParts[i] = p
	}
	return genericParts, nil
}
