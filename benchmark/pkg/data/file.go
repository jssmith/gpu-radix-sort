package data

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

var FileArrayFactory *ArrayFactory = &ArrayFactory{
	Create: func(name string, shape DistribArrayShape) (DistribArray, error) {
		a, err := CreateFileDistribArray(name, shape)
		return (DistribArray)(a), err
	},

	Open: func(name string) (DistribArray, error) {
		a, err := OpenFileDistribArray(name)
		return (DistribArray)(a), err
	},
}

// Stores a distributed array in the filesystem (in the directory at RootPath).
// There are two files:
//		meta.dat: stores metadata about the array. First 'lens', then 'caps'
//			(file size can be used to dermine the number of partitions)
//		data.dat: Stores the actual data, each partition starts at offset
//			starts[partID] in the file.
type FileDistribArray struct {
	RootPath string
	fd       *os.File

	// like len and cap for slices for each partition
	shape DistribArrayShape

	// Optimization/convenience stores the starting point of each partition in
	// the file
	starts []int64
}

type FileDistribRangeReader struct {
	file *os.File

	// The number of bytes still to read before hitting the limit
	nRemaining int
}

type FileDistribWriter struct {
	arr    *FileDistribArray
	partId int
}

// Create a new FileDistribArray object from an existing on-disk array
func OpenFileDistribArray(rootPath string) (*FileDistribArray, error) {
	var err error

	arr := &FileDistribArray{}

	arr.RootPath, err = filepath.Abs(rootPath)
	if err != nil {
		return nil, err
	}

	metaPath := filepath.Join(rootPath, "meta.dat")
	metaFile, err := os.OpenFile(metaPath, os.O_CREATE, 0600)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create metdata file")
	}

	stat, err := metaFile.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "Could not stat metadata file %v", metaPath)
	}

	// Two arrays of int64s
	nPart := (stat.Size() / 8) / 2

	fullMeta := make([]int, nPart*2)
	binary.Read(metaFile, binary.LittleEndian, fullMeta)
	arr.shape.lens = fullMeta[:nPart]
	arr.shape.caps = fullMeta[nPart:]

	if err := metaFile.Close(); err != nil {
		return nil, errors.Wrapf(err, "Failed to close metadata file")
	}

	arr.starts = make([]int64, nPart)
	cumCap := (int64)(0)
	for i := (int64)(0); i < nPart; i++ {
		arr.starts[i] = cumCap
		cumCap += (int64)(arr.shape.caps[i])
	}

	dataPath := filepath.Join(arr.RootPath, "data.dat")
	dataFile, err := os.OpenFile(dataPath, os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create data file")
	}
	arr.fd = dataFile

	return arr, nil
}

// Create a new file-backed distributed array. caps describes the size of each
// partition (like capacity in a slice). Partitions cannot be resized.
func CreateFileDistribArray(rootPath string, shape DistribArrayShape) (*FileDistribArray, error) {
	var err error

	arr := &FileDistribArray{}

	rootPath, err = filepath.Abs(rootPath)
	if err != nil {
		return nil, err
	}
	arr.RootPath = rootPath

	err = os.Mkdir(rootPath, 0700)
	if err != nil {
		return nil, err
	}

	// =======================
	// Metadata Management
	// =======================
	arr.shape.caps = make([]int, len(shape.caps))
	arr.shape.lens = make([]int, len(shape.caps))
	copy(arr.shape.caps, shape.caps)
	copy(arr.shape.lens, shape.lens)

	arr.starts = make([]int64, len(shape.caps))
	capSum := (int64)(0)
	for i := 0; i < len(shape.caps); i++ {
		arr.starts[i] = capSum
		capSum += (int64)(shape.caps[i])
	}

	metaPath := filepath.Join(rootPath, "meta.dat")
	metaFile, err := os.OpenFile(metaPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create metdata file")
	}
	binary.Write(metaFile, binary.LittleEndian, arr.shape.lens)
	binary.Write(metaFile, binary.LittleEndian, arr.shape.caps)

	if err := metaFile.Close(); err != nil {
		return nil, errors.Wrapf(err, "Failed to close metadata file")
	}

	//=============================
	// Backing file
	//=============================
	dataPath := filepath.Join(rootPath, "data.dat")

	// Go's create() doesn't allow you to set permissions so we have to
	// open and then immediately close
	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create data file")
	}
	arr.fd = dataFile

	return arr, nil
}

func (self *FileDistribArray) GetShape() (*DistribArrayShape, error) {
	// Copy the slices but not their underlying array (DistribArrayShape is immutable)
	return &DistribArrayShape{lens: self.shape.lens, caps: self.shape.caps}, nil
}

func (self *FileDistribArray) GetPartRangeReader(partId, start, end int) (io.ReadCloser, error) {
	var err error

	reader := FileDistribRangeReader{}

	// Re-open file to get thread-safe readers
	reader.file, err = os.Open(filepath.Join(self.RootPath, "data.dat"))
	if err != nil {
		return nil, err
	}

	_, err = reader.file.Seek(self.starts[partId]+(int64)(start), 0)
	if err != nil {
		return nil, err
	}

	if end <= 0 {
		reader.nRemaining = (self.shape.lens[partId] + end) - start
	} else {
		reader.nRemaining = end - start
	}

	return &reader, nil
}

func (self *FileDistribArray) GetPartReader(partId int) (io.ReadCloser, error) {
	return self.GetPartRangeReader(partId, 0, 0)
}

func (self *FileDistribArray) Close() error {
	var eMsg string

	// Commit metadata
	metaPath := filepath.Join(self.RootPath, "meta.dat")
	if metaFile, err := os.OpenFile(metaPath, os.O_WRONLY, 0600); err == nil {
		e1 := binary.Write(metaFile, binary.LittleEndian, self.shape.lens)
		e2 := binary.Write(metaFile, binary.LittleEndian, self.shape.caps)
		if e2 != nil {
			eMsg += fmt.Sprintf(" Failed to write metadata: %v. ", e2)
		} else if e1 != nil {
			eMsg += fmt.Sprintf(" Failed to write metadata: %v. ", e1)
		}

		if err := metaFile.Close(); err != nil {
			eMsg += fmt.Sprintf(" Failed to close metadata file. Array may be corrupted!: %v. ", err)
		}
	} else {
		eMsg += fmt.Sprintf(" Failed to open metdata file. Array may be corrupted!: %v. ", err)
	}

	if err := self.fd.Close(); err != nil {
		eMsg += fmt.Sprintf(" Failed to close data file. Array may be corrupted!: %v. ", err)
	}

	if eMsg != "" {
		return fmt.Errorf(eMsg)
	}

	return nil
}

func (self *FileDistribArray) Destroy() error {
	// It really doesn't matter if there is an error on closing. We might eat
	// up resources but RemoveAll means the OS will get to it eventually (the
	// fd will be closed on process exit at a minimum). Consistency is
	// irrelevant since the resource is being removed anyway.
	self.fd.Close()

	return os.RemoveAll(self.RootPath)
}
func (self *FileDistribRangeReader) Read(dst []byte) (n int, err error) {
	var toRead int
	if len(dst) < self.nRemaining {
		toRead = len(dst)
	} else {
		toRead = self.nRemaining
		err = io.EOF
	}

	n, readErr := self.file.Read(dst[:toRead])
	self.nRemaining -= n
	if readErr != nil {
		err = readErr
	}

	return n, err
}

func (self *FileDistribRangeReader) Close() error {
	return self.file.Close()
}

func (self *FileDistribArray) GetPartWriter(partId int) (io.WriteCloser, error) {
	var err error

	writer := &FileDistribWriter{arr: self, partId: partId}

	_, err = writer.arr.fd.Seek(self.starts[partId]+(int64)(self.shape.lens[partId]), 0)
	if err != nil {
		return nil, err
	}

	return writer, nil
}

func (self *FileDistribWriter) Write(b []byte) (int, error) {
	var err error

	nRemaining := self.arr.shape.caps[self.partId] - self.arr.shape.lens[self.partId]

	// File arrays have fixed-sized partitions (they're also append-only)
	toWrite := len(b)
	if toWrite > nRemaining {
		err = io.EOF
		toWrite = nRemaining
	}

	n, wErr := self.arr.fd.Write(b[:toWrite])
	self.arr.shape.lens[self.partId] += n

	if wErr != nil {
		err = wErr
	}
	return n, err
}

func (self *FileDistribWriter) Close() error {
	return nil
}
