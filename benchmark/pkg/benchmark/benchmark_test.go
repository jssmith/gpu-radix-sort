package benchmark

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
)

func BenchmarkFileDistribLocal(b *testing.B) {
	var err error

	err = sort.InitLibSort()
	if err != nil {
		b.Fatalf("Failed to initialize libsort: %v", err)
	}

	// Should be an odd (in both senses) number to pick up unaligned corner
	// cases
	nElem := 1111
	// nElem := (1024 * 1024) + 5
	origRaw, err := sort.GenerateInputs((uint64)(nElem))
	if err != nil {
		b.Fatalf("Failed to generate inputs: %v", err)
	}

	iterIn := make([]byte, len(origRaw))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// anonymous function for defer behavior
		func() {
			b.StopTimer()
			copy(iterIn, origRaw)
			tmpDir, err := ioutil.TempDir("", "radixSortLocalTest*")
			if err != nil {
				b.Fatalf("Couldn't create temporary test directory %v", err)
			}

			defer os.RemoveAll(tmpDir)
			b.StartTimer()

			_, err = sort.SortDistribFromRaw(iterIn, func(name string, nbucket int) (data.DistribArray, error) {
				var arr data.DistribArray
				arr, err := data.NewFileDistribArray(filepath.Join(tmpDir, name), nbucket)
				return arr, err
			}, sort.LocalDistribWorker)

			if err != nil {
				b.Fatalf("Sort failed: %v", err)
			}
		}()
	}
}
