package main

import (
	"bytes"
	"io"
	"io/ioutil"
)

// A write-closer for MemDistrib, close is a nop in this case
type MemDistribPartWriteCloser struct {
	w *bytes.Buffer
}

func (self *MemDistribPartWriteCloser) Write(p []byte) (n int, err error) {
	return self.w.Write(p)
}

func (self *MemDistribPartWriteCloser) Close() error {
	return nil
}

// Partition of a MemDistribArray
type MemDistribPart struct {
	part []byte
}

func NewMemDistribPart(p []byte) (*MemDistribPart, error) {
	return &MemDistribPart{part: p}, nil
}

func (self *MemDistribPart) GetReader() io.ReadCloser {
	return ioutil.NopCloser(bytes.NewBuffer(self.part))
}

func (self *MemDistribPart) GetWriter() io.WriteCloser {
	return &MemDistribPartWriteCloser{w: bytes.NewBuffer(self.part)}
}

// In-memory 'distributed' array. Does not provide any persistence and cannot
// share between processes (only threads in the same address space).
type MemDistribArray struct {
	parts [][]byte
}

func NewMemDistribArray(npart int) (*MemDistribArray, error) {
	parts := make([][]byte, npart)
	for i := 0; i < npart; i++ {
		parts[i] = make([]byte, 0)
	}
	arr := &MemDistribArray{
		parts: parts,
	}
	return arr, nil
}

func (self *MemDistribArray) GetParts() ([]DistribPart, error) {
	var err error

	partObjs := make([]DistribPart, len(self.parts))
	for i, p := range self.parts {
		partObjs[i], err = NewMemDistribPart(p)
		if err != nil {
			return nil, err
		}
	}
	return partObjs, nil
}
