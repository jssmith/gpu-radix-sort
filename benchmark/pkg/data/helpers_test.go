package data

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFetchPartRefs(t *testing.T) {
	nByte := 1024

	a1, err := NewMemDistribArray(1)
	require.Nil(t, err, "Failed to create input array")
	a2, err := NewMemDistribArray(2)
	require.Nil(t, err, "Failed to create input array")

	raw1 := generateBytes(t, a1, nByte)
	raw2 := generateBytes(t, a2, nByte)

	refs := make([]*PartRef, 3)
	refs[0] = &PartRef{Arr: a1, PartIdx: 0, Start: 0, NByte: nByte}
	refs[1] = &PartRef{Arr: a2, PartIdx: 0, Start: nByte / 2, NByte: nByte / 4}
	refs[2] = &PartRef{Arr: a2, PartIdx: 1, Start: 0, NByte: nByte / 4}

	out, err := FetchPartRefs(refs)
	require.Nil(t, err, "FetchPartRefs error")

	outPos := 0

	rawStart := (refs[0].PartIdx * nByte) + refs[0].Start
	sz := refs[0].NByte
	require.Equal(t,
		raw1[rawStart:rawStart+sz],
		out[outPos:outPos+sz],
		"First ref (whole partition) wrong")
	outPos += sz

	rawStart = (refs[1].PartIdx * nByte) + refs[1].Start
	sz = refs[1].NByte
	require.Equal(t,
		raw2[rawStart:rawStart+sz],
		out[outPos:outPos+sz],
		"Second ref wrong")
	outPos += sz

	rawStart = (refs[2].PartIdx * nByte) + refs[2].Start
	sz = refs[2].NByte
	require.Equal(t,
		raw2[rawStart:rawStart+sz],
		out[outPos:outPos+sz],
		"Third ref wrong")
}
