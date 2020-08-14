package benchmark

import (
	"fmt"
	"io"
	"time"

	"github.com/serverlessresearch/srk/pkg/srkmgr"
	"gonum.org/v1/gonum/stat"
)

// Specific to agpu1 machine, edit for your environment

// Maximum number of uints that can be sorted by one device
const nmax_per_dev = (256 * 1024 * 1024)

// Number of available devices (GPUs)
const ndev = 2

// A helper object for timing events, the timer can be reused multiple times in
// order to derive averages or other statistics (Record() saves the current
// measurement and begins a new measurement).
type PerfTimer struct {
	Vals  []float64 // the stats module wants float64
	cur   time.Duration
	start time.Time
}

// Begin (or resume) the timer
func (self *PerfTimer) Start() {
	self.start = time.Now()
}

// Stop (or pause) the timer
func (self *PerfTimer) Stop() {
	self.cur += time.Since(self.start)
}

// Finalize the timer, adding it as a new datapoint and resetting the timer to
// 0.
func (self *PerfTimer) Record() {
	self.Stop()
	self.Vals = append(self.Vals, (float64)(self.cur))
	self.cur = 0
}

// Add the recorded values from new to the current object. Does not modify new.
func (self *PerfTimer) Update(new *PerfTimer) {
	self.Vals = append(self.Vals, new.Vals...)
}

// Collects statistics about a sort. Not all fields are applicable (or
// measurable) for all sort types.
type SortStats map[string]*PerfTimer

func ReportStats(stats SortStats, writer io.Writer) {
	for name, timer := range stats {
		mean, stdev := stat.MeanStdDev(timer.Vals, nil)
		fmt.Fprintf(writer, "%v (mean):\t%vs\n", name, mean/1e9)
		fmt.Fprintf(writer, "%v (std):\t%vs\n", name, stdev/1e9)
	}
}

func printCSV(m map[string]float64) {
	var ks []string
	var vs []float64
	for k, v := range m {
		ks = append(ks, k)
		vs = append(vs, v)
	}

	for i := 0; i < len(m); i++ {
		fmt.Printf("%v,", ks[i])
	}
	fmt.Printf("\n")
	for i := 0; i < len(m); i++ {
		fmt.Printf("%v,", vs[i])
	}
}

func reportStats(mgr *srkmgr.SrkManager) {
	fmt.Println("Provider Statistics:")
	pstat, _ := mgr.Provider.Faas.ReportStats()
	printCSV(pstat)
	fmt.Printf("\n\n")
}
