package faas

import (
	"testing"
)

func TestNewFaas(t *testing.T) {
	nint := 32
	input := make([]uint32, nint)

	for i := 0; i < nint; i++ {
		input[i] = uint32(i)
	}

	fArg, err := NewFaasArg("provided", input)
	if err != nil {
		t.Fatalf("Constructor failed: %v", err)
	}

	decoded, err := DecodeFaasResp(fArg.Data)
	if err != nil {
		t.Fatalf("Could not decode output: %v", err)
	}

	if len(decoded) != len(input) {
		t.Fatalf("Decoded length incorrect: Expected %v, Got %v", len(input), len(decoded))
	}

	for i := 0; i < nint; i++ {
		if decoded[i] != input[i] {
			t.Fatalf("Decoded incorrectly at index %v: Expected %v, Got %v", i, input[i], decoded[i])
		}
	}
}
