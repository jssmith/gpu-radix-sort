package benchmark

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
	"github.com/pkg/errors"
)

func BenchMemLocalDistrib(arr []uint32, stats *SortStats) error {
	var err error

	stats.TTotal.Start()
	_, err = sort.SortDistribFromRaw(arr, func(name string, nbucket int) (data.DistribArray, error) {
		var arr data.DistribArray
		arr, err := data.NewMemDistribArray(nbucket)
		return arr, err
	}, sort.LocalDistribWorker)
	stats.TTotal.Record()

	if err != nil {
		return err
	}

	return nil
}

func BenchFileLocalDistrib(arr []uint32, stats *SortStats) error {
	tmpDir, err := ioutil.TempDir("", "radixSortLocalTest*")
	if err != nil {
		return errors.Wrap(err, "Failed to create temporary directory")
	}
	defer os.RemoveAll(tmpDir)

	stats.TTotal.Start()
	_, err = sort.SortDistribFromRaw(arr, func(name string, nbucket int) (data.DistribArray, error) {
		var arr data.DistribArray
		arr, err := data.NewFileDistribArray(filepath.Join(tmpDir, name), nbucket)
		return arr, err
	}, sort.LocalDistribWorker)
	stats.TTotal.Record()

	if err != nil {
		return err
	}

	return nil
}

// This runs manual benchmarks (not managed by Go's benchmarking tool)
// Even if an error is returned, the returned stats may be non-nil and contain
// valid results up until the error
func RunBenchmarks() (map[string]*SortStats, error) {
	var err error

	const nrepeat = 2
	stats := make(map[string]*SortStats)

	// nElem := 1024 * 1024
	nElem := nmax_per_dev * ndev

	origRaw, err := sort.GenerateInputs((uint64)(nElem))
	if err != nil {
		return stats, errors.Wrap(err, "Failed to generate inputs")
	}
	iterIn := make([]uint32, nElem)

	stats["FileLocalDistrib"] = &SortStats{}
	for i := 0; i < nrepeat; i++ {
		copy(iterIn, origRaw)
		err = BenchMemLocalDistrib(iterIn, stats["FileLocalDistrib"])
		if err != nil {
			return stats, errors.Wrap(err, "Failed to benchmark FileLocalDistrib")
		}
	}

	return stats, nil
}
