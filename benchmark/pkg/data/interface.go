package data

import (
	"fmt"
	"io"
)

// Describe the logical layout of a distributed array
type DistribArrayShape struct {
	lens []int64 // Current number of bytes per partition
	caps []int64 // Current capacity of each partition a zero capcity indicates unlimited
}

// Create a DistribArrayShape with the provided capacities
func CreateShape(caps []int64) DistribArrayShape {
	npart := len(caps)
	shapeCaps := make([]int64, npart)
	shapeLens := make([]int64, npart)

	copy(shapeCaps, caps)
	return DistribArrayShape{caps: shapeCaps, lens: shapeLens}
}

// Create a DistribArrayShape with npart partitions and all capacities set to 'cap'.
func CreateShapeUniform(cap int64, npart int) DistribArrayShape {
	caps := make([]int64, npart)
	for i := 0; i < npart; i++ {
		caps[i] = cap
	}
	lens := make([]int64, npart)

	return DistribArrayShape{caps: caps, lens: lens}
}

// DistribArrayShapes are immutable so we abstract access
func (self *DistribArrayShape) Len(partIdx int) int64 {
	return self.lens[partIdx]
}

func (self *DistribArrayShape) Cap(partIdx int) int64 {
	return self.caps[partIdx]
}

func (self *DistribArrayShape) NPart() int {
	return len(self.caps)
}

func (self *DistribArrayShape) ToString() string {
	var lenStr string
	var capStr string
	//XXX
	// for i := 0; i < len(self.caps); i++ {
	for i := 0; i < 10; i++ {
		capStr += fmt.Sprintf("%v:%v\n", i, self.caps[i])
		lenStr += fmt.Sprintf("%v:%v\n", i, self.lens[i])
	}

	var s string
	s += fmt.Sprintf("Lens: %v\n", lenStr)
	s += fmt.Sprintf("Caps: %v\n", capStr)
	return s
}

// Represents an array of bytes that is suitable for a distributed sort.
//
// Semantics:
//		DistribArrays represent an interface to some external storage. Arrays
//		can be opened and closed multiple times, the way the backing data is
//		identified is implementation specific. The exact consistency semantics
//		are up to the implementation, but a conservative client should not have
//		more than one open DistribArray for the same backing array at the same
//		time (Close() commits local changes, the backing object may be in an
//		inconsistent state between create and Close() calls).
type DistribArray interface {
	GetShape() (*DistribArrayShape, error)

	// It is not generally safe to have more than one writer open at a time.
	// Closing a writer commits changes to the local object but may or may not
	// modify the backing store (call DistribArray.Close() to commit changes to
	// the backing store).
	GetPartReader(partId int) (io.ReadCloser, error)

	// Multiple readers may exist simultaneously for the same array, but the
	// user must ensure that the array does not change while there are active
	// readers.
	GetPartRangeReader(partId, start, end int) (io.ReadCloser, error)

	// Writers are append-only
	GetPartWriter(partId int) (io.WriteCloser, error)

	// Release any process-local resources associated with this array. It is
	// no longer safe to use this object.
	Close() error

	// Release any global resources (e.g. the backing store) associated with
	// this array (implies close, no need to call both). No users internal or
	// external may access the object after this.
	Destroy() error
}

// A reference to an input partition
type PartRef struct {
	Arr     DistribArray // DistribArray to read from
	PartIdx int          // Partition to read from
	Start   int          // Offset to start reading
	NByte   int          // Number of bytes to read
}

type ArrayFactory struct {
	Create func(name string, shape DistribArrayShape) (DistribArray, error)
	Open   func(name string) (DistribArray, error)
}
