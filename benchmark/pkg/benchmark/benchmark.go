package benchmark

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
	"github.com/pkg/errors"
)

func BenchMemLocalDistrib(arr []uint32, stats SortStats) error {
	var err error
	var ok bool

	var TTotal *PerfTimer
	if TTotal, ok = stats["TTotal"]; !ok {
		TTotal = &PerfTimer{}
		stats["TTotal"] = TTotal
	}

	TTotal.Start()
	_, err = sort.SortDistribFromRaw(arr, func(name string, nbucket int) (data.DistribArray, error) {
		var arr data.DistribArray
		arr, err := data.NewMemDistribArray(nbucket)
		return arr, err
	}, sort.LocalDistribWorker)
	TTotal.Record()

	if err != nil {
		return err
	}

	return nil
}

func BenchFileLocalDistrib(arr []uint32, stats SortStats) error {
	var ok bool

	var TTotal *PerfTimer
	if TTotal, ok = stats["TTotal"]; !ok {
		TTotal = &PerfTimer{}
		stats["TTotal"] = TTotal
	}

	tmpDir, err := ioutil.TempDir("", "radixSortLocalTest*")
	if err != nil {
		return errors.Wrap(err, "Failed to create temporary directory")
	}
	defer os.RemoveAll(tmpDir)

	TTotal.Start()
	_, err = sort.SortDistribFromRaw(arr, func(name string, nbucket int) (data.DistribArray, error) {
		var arr data.DistribArray
		arr, err := data.NewFileDistribArray(filepath.Join(tmpDir, name), nbucket)
		return arr, err
	}, sort.LocalDistribWorker)
	TTotal.Record()

	if err != nil {
		return err
	}

	return nil
}

// This runs manual benchmarks (not managed by Go's benchmarking tool)
// Even if an error is returned, the returned stats may be non-nil and contain
// valid results up until the error
func RunBenchmarks() (map[string]SortStats, error) {
	var err error

	const nrepeat = 2
	stats := make(map[string]SortStats)

	nElem := 1024 * 1024
	// nElem := nmax_per_dev * ndev

	origRaw, err := sort.GenerateInputs((uint64)(nElem))
	if err != nil {
		return stats, errors.Wrap(err, "Failed to generate inputs")
	}
	iterIn := make([]uint32, nElem)

	stats["MemLocalDistrib"] = make(SortStats)
	for i := 0; i < nrepeat; i++ {
		copy(iterIn, origRaw)
		err = BenchMemLocalDistrib(iterIn, stats["MemLocalDistrib"])
		if err != nil {
			return stats, errors.Wrap(err, "Failed to benchmark MemLocalDistrib")
		}
	}

	stats["FileLocalDistrib"] = make(SortStats)
	for i := 0; i < nrepeat; i++ {
		copy(iterIn, origRaw)
		err = BenchFileLocalDistrib(iterIn, stats["FileLocalDistrib"])
		if err != nil {
			return stats, errors.Wrap(err, "Failed to benchmark MemLocalDistrib")
		}
	}
	return stats, nil
}
