package benchmark

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/faas"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
	"github.com/pkg/errors"
)

func BenchMemLocalDistrib(arr []byte, stats SortStats) error {
	var err error
	var ok bool

	var TTotal *PerfTimer
	if TTotal, ok = stats["TTotal"]; !ok {
		TTotal = &PerfTimer{}
		stats["TTotal"] = TTotal
	}

	TTotal.Start()
	_, err = sort.SortDistribFromRaw(arr, "BenchMemLocalDistrib", data.MemArrayFactory, sort.LocalDistribWorker)
	TTotal.Record()

	if err != nil {
		return err
	}

	return nil
}

func BenchFileLocalDistrib(arr []byte, stats SortStats) error {
	var ok bool

	var TTotal *PerfTimer
	if TTotal, ok = stats["TTotal"]; !ok {
		TTotal = &PerfTimer{}
		stats["TTotal"] = TTotal
	}

	tmpDir, err := ioutil.TempDir("", "benchFileLocalDistrib")
	if err != nil {
		return errors.Wrap(err, "Failed to create temporary directory")
	}
	defer os.RemoveAll(tmpDir)

	TTotal.Start()
	_, err = sort.SortDistribFromRaw(arr, "benchLocalDistrib", data.NewFileArrayFactory(tmpDir), sort.LocalDistribWorker)
	TTotal.Record()

	if err != nil {
		return err
	}

	return nil
}

func BenchFaasOne(arr []byte, stats SortStats) error {
	var ok bool

	var TTotal *PerfTimer
	if TTotal, ok = stats["TTotal"]; !ok {
		TTotal = &PerfTimer{}
		stats["TTotal"] = TTotal
	}

	tmpDir, err := ioutil.TempDir("", "benchFileLocalDistrib")
	if err != nil {
		return errors.Wrap(err, "Failed to create temporary directory")
	}
	defer os.RemoveAll(tmpDir)

	//Configure SRK
	//OL will mount tmpDir to the FaaS worker so it can find the distributed arrays
	os.Setenv("OL_SHARED_VOLUME", tmpDir)
	fmt.Println("Getting SRK manager")
	mgr := faas.GetMgr()
	defer mgr.Destroy()

	arrFactory := data.NewFileArrayFactory(tmpDir)
	worker := faas.InitFaasWorker(mgr)

	TTotal.Start()
	_, err = sort.SortDistribFromRaw(arr, "benchLocalDistrib", arrFactory, worker)
	TTotal.Record()

	if err != nil {
		return err
	}

	return nil
}

func BenchFaasAll(origRaw []byte, name string) (SortStats, error) {
	var err error
	const nrepeat = 5

	stats := make(SortStats)

	iterIn := make([]byte, len(origRaw))

	// Timed runs
	for i := 0; i < nrepeat; i++ {
		copy(iterIn, origRaw)
		err = BenchFaasOne(iterIn, stats)
		if err != nil {
			return stats, errors.Wrap(err, "Failed to benchmark FaaS")
		}

		cmd := exec.Command("./copyResult.sh", fmt.Sprintf("%s_%d", name, i), "faasStats/")
		err = cmd.Run()
		if err != nil {
			return stats, errors.Wrap(err, "Error while copying profiling results")
		}
	}
	return stats, nil
}

// This runs manual benchmarks (not managed by Go's benchmarking tool)
// Even if an error is returned, the returned stats may be non-nil and contain
// valid results up until the error
func RunBenchmarks() (map[string]SortStats, error) {
	var err error

	stats := make(map[string]SortStats)

	// nElem := 1024 * 1024
	nElem := nmax_per_dev * ndev

	origRaw, err := sort.GenerateInputs((uint64)(nElem))
	if err != nil {
		return stats, errors.Wrap(err, "Failed to generate inputs")
	}

	// stats["MemLocalDistrib"] = make(SortStats)
	// for i := 0; i < nrepeat; i++ {
	// 	copy(iterIn, origRaw)
	// 	err = BenchMemLocalDistrib(iterIn, stats["MemLocalDistrib"])
	// 	if err != nil {
	// 		return stats, errors.Wrap(err, "Failed to benchmark MemLocalDistrib")
	// 	}
	// 	runtime.GC()
	// }

	// stats["FileLocalDistrib"] = make(SortStats)
	// for i := 0; i < nrepeat; i++ {
	// 	copy(iterIn, origRaw)
	// 	err = BenchFileLocalDistrib(iterIn, stats["FileLocalDistrib"])
	// 	if err != nil {
	// 		return stats, errors.Wrap(err, "Failed to benchmark FileLocalDistrib")
	// 	}
	// }

	err = os.Mkdir("faasStats", 0o700)
	if os.IsExist(err) {
		return stats, fmt.Errorf("Profiling results dir already exists, please cleanup first")
	} else if err != nil {
		return stats, errors.Wrapf(err, "Error creating profiling results directory")
	}

	sort.SetWidth(8)
	runStats, err := BenchFaasAll(origRaw, "8b")
	if err != nil {
		return stats, err
	}
	stats["FaaS8"] = runStats

	sort.SetWidth(16)
	runStats, err = BenchFaasAll(origRaw, "16b")
	if err != nil {
		return stats, err
	}
	stats["FaaS16"] = runStats

	return stats, nil
}
