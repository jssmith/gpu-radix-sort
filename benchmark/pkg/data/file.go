package data

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type FileDistribPartRangeReader struct {
	file *os.File

	// The number of bytes still to read before hitting the limit
	nRemaining int
}

func (self *FileDistribPartRangeReader) Read(dst []byte) (n int, err error) {
	var toRead int
	if len(dst) > self.nRemaining {
		toRead = len(dst)
	} else {
		toRead = self.nRemaining
	}

	n, err = self.file.Read(dst[:toRead])

	self.nRemaining -= n
	return n, err
}

func (self *FileDistribPartRangeReader) Close() error {
	return self.file.Close()
}

type FileDistribPart struct {
	partPath string
}

func (self *FileDistribPart) Len() (int, error) {
	stat, err := os.Stat(self.partPath)
	if err != nil {
		return 0, err
	}

	return (int)(stat.Size()), nil
}

func (self *FileDistribPart) GetRangeReader(start, end int) (io.ReadCloser, error) {
	var err error

	reader := FileDistribPartRangeReader{}

	reader.file, err = os.Open(self.partPath)
	if err != nil {
		return nil, err
	}

	if end <= 0 {
		stat, err := reader.file.Stat()
		if err != nil {
			reader.file.Close()
			return nil, err
		}

		end = (int)(stat.Size()) + end
	}

	if start != 0 {
		_, err = reader.file.Seek((int64)(start), 0)
		if err != nil {
			reader.file.Close()
			return nil, errors.Wrapf(err, "Could not seek to provided start: %v", start)
		}
	}

	reader.nRemaining = (int)(end - start)
	return &reader, nil
}

func (self *FileDistribPart) GetReader() (io.ReadCloser, error) {
	return os.Open(self.partPath)
}

func (self *FileDistribPart) GetWriter() (io.WriteCloser, error) {
	return os.OpenFile(self.partPath, os.O_APPEND|os.O_WRONLY, 0)
}

type FileDistribArray struct {
	RootPath string
}

// Create a new FileDistribArray object from an existing on-disk array
func NewFileDistribArrayExisting(rootPath string) (*FileDistribArray, error) {
	rootPath, err := filepath.Abs(rootPath)
	if err != nil {
		return nil, err
	}

	return &FileDistribArray{RootPath: rootPath}, nil
}

func NewFileDistribArray(rootPath string, npart int) (*FileDistribArray, error) {
	var err error

	rootPath, err = filepath.Abs(rootPath)
	if err != nil {
		return nil, err
	}

	err = os.Mkdir(rootPath, 0700)
	if err != nil {
		return nil, err
	}

	for i := 0; i < npart; i++ {
		partPath := filepath.Join(rootPath, fmt.Sprintf("p%v.dat", i))

		// Go's create() doesn't allow you to set permissions so we have to
		// open and then immediately close
		pFile, err := os.OpenFile(partPath, os.O_CREATE, 0600)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to create file for partition %v", i)
		}

		if err := pFile.Close(); err != nil {
			return nil, errors.Wrapf(err, "Failed to create file for partition %v", i)
		}
	}

	return &FileDistribArray{RootPath: rootPath}, nil
}

func (self *FileDistribArray) GetParts() ([]DistribPart, error) {
	var parts []DistribPart

	rootDir, err := os.Open(self.RootPath)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to open array root %v", self.RootPath)
	}

	partInfos, err := rootDir.Readdir(0)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to read array root %v", self.RootPath)
	}

	for _, info := range partInfos {
		parts = append(parts, (DistribPart)(&FileDistribPart{partPath: filepath.Join(self.RootPath, info.Name())}))
	}

	return parts, nil
}
