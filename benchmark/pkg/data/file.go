package data

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// A version of DistribArrayShape that is JSON serializable and can be updated
// if needed. This is required because we don't want to export the fields of
// DistribArrayShape but JSON can't handle unexported fields.
type fileShape struct {
	Lens []int64
	Caps []int64
}

func NewFileArrayFactory(rootDir string) *ArrayFactory {
	return &ArrayFactory{
		Create: func(name string, shape DistribArrayShape) (DistribArray, error) {
			a, err := CreateFileDistribArray(filepath.Join(rootDir, name), shape)
			return (DistribArray)(a), err
		},

		Open: func(name string) (DistribArray, error) {
			a, err := OpenFileDistribArray(filepath.Join(rootDir, name))
			return (DistribArray)(a), err
		},
	}
}

// Stores a distributed array in the filesystem (in the directory at RootPath).
// There are two files:
//		meta.json: stores metadata about the array. First 'lens', then 'caps'
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

	err = arr.loadMeta()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to load metadata")
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
	arr.shape.caps = make([]int64, len(shape.caps))
	arr.shape.lens = make([]int64, len(shape.caps))
	copy(arr.shape.caps, shape.caps)
	copy(arr.shape.lens, shape.lens)

	arr.starts = make([]int64, len(shape.caps))
	capSum := (int64)(0)
	for i := 0; i < len(shape.caps); i++ {
		arr.starts[i] = capSum
		capSum += (int64)(shape.caps[i])
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

	err = arr.commitMeta()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create array metdata")
	}

	return arr, nil
}

func (self *FileDistribArray) commitMeta() error {
	jsonShape := fileShape{Lens: self.shape.lens, Caps: self.shape.caps}

	metaPath := filepath.Join(self.RootPath, "meta.json")
	metaFile, err := os.OpenFile(metaPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return errors.Wrapf(err, "Failed to create metdata file")
	}

	jsonBytes, err := json.Marshal(jsonShape)
	if err != nil {
		return errors.Wrapf(err, "Couldn't convert shape to json")
	}
	_, err = metaFile.Write(jsonBytes)
	if err != nil {
		return errors.Wrap(err, "Error while writing metadata")
	}

	if err := metaFile.Close(); err != nil {
		return errors.Wrapf(err, "Failed to close metadata file")
	}
	return nil
}

func (self *FileDistribArray) loadMeta() error {
	metaPath := filepath.Join(self.RootPath, "meta.json")
	metaFile, err := os.OpenFile(metaPath, os.O_CREATE, 0600)
	if err != nil {
		return errors.Wrap(err, "Failed to create metdata file")
	}

	metaBytes, err := ioutil.ReadAll(metaFile)
	if err != nil {
		return errors.Wrap(err, "Failed to read metadata")
	}

	var jsonShape fileShape
	err = json.Unmarshal(metaBytes, &jsonShape)
	if err != nil {
		return errors.Wrap(err, "Failed to interpret metadata")
	}

	self.shape.lens = jsonShape.Lens
	self.shape.caps = jsonShape.Caps

	if err := metaFile.Close(); err != nil {
		return errors.Wrapf(err, "Failed to close metadata file")
	}

	self.starts = make([]int64, len(self.shape.lens))
	cumCap := (int64)(0)
	for i := 0; i < len(self.shape.lens); i++ {
		self.starts[i] = cumCap
		cumCap += (int64)(self.shape.caps[i])
	}
	return nil
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
		reader.nRemaining = (int)(self.shape.lens[partId] + (int64)(end) - (int64)(start))
	} else {
		reader.nRemaining = end - start
	}

	return &reader, nil
}

func (self *FileDistribArray) GetPartReader(partId int) (io.ReadCloser, error) {
	return self.GetPartRangeReader(partId, 0, 0)
}

func (self *FileDistribArray) Close() error {
	// var eMsg string

	closeErr := self.fd.Close()
	metaErr := self.commitMeta()

	if closeErr != nil || metaErr != nil {
		return fmt.Errorf("Array commit failure (data may be corrupted): metadata: %v, data: %v", metaErr, closeErr)
	}

	return nil
}

func (self *FileDistribArray) Destroy() error {
	// It really doesn't matter if there is an error on closing. We might eat
	// up resources but RemoveAll means the OS will get to it eventually (the
	// fd will be closed on process exit at a minimum). Consistency is
	// irrelevant since the resource is being removed anyway.
	self.Close()

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
	toWrite := (int64)(len(b))
	if toWrite > nRemaining {
		err = io.EOF
		toWrite = nRemaining
	}

	n, wErr := self.arr.fd.Write(b[:toWrite])
	self.arr.shape.lens[self.partId] += (int64)(n)

	if wErr != nil {
		err = wErr
	}
	return n, err
}

func (self *FileDistribWriter) Close() error {
	return nil
}
