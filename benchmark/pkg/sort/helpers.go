package sort

import (
	"fmt"
	"math/rand"
	"sort"
)

func RandomInputs(len int) []uint32 {
	rand.Seed(0)
	out := make([]uint32, len)
	for i := 0; i < len; i++ {
		out[i] = rand.Uint32()
	}
	return out
}

func CheckSort(orig []uint32, new []uint32) error {
	if len(orig) != len(new) {
		return fmt.Errorf("Lengths do not match: Expected %v, Got %v\n", len(orig), len(new))
	}

	origCpy := make([]uint32, len(orig))
	copy(origCpy, orig)
	sort.Slice(origCpy, func(i, j int) bool { return origCpy[i] < origCpy[j] })
	for i := 0; i < len(orig); i++ {
		if origCpy[i] != new[i] {
			return fmt.Errorf("Response doesn't match reference at %v\n: Expected %v, Got %v\n", i, origCpy[i], new[i])
		}
	}
	return nil
}
