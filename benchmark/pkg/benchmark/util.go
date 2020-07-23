package benchmark

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/serverlessresearch/srk/pkg/srkmgr"
	"github.com/sirupsen/logrus"
	"gonum.org/v1/gonum/stat"
)

// Specific to agpu1 machine, edit for your environment
const nmax_per_dev = (256 * 1024 * 1024)
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
type SortStats struct {
	TTotal   PerfTimer
	TWorker  PerfTimer
	TShuffle PerfTimer
	TRead    PerfTimer
	TWrite   PerfTimer
}

func (self *SortStats) Report(writer io.Writer) {
	mean, stdev := stat.MeanStdDev(self.TTotal.Vals, nil)
	fmt.Fprintf(writer, "TTotal (mean):\t%v\n", mean)
	fmt.Fprintf(writer, "TTotal (std):\t%v\n", stdev)
	// fmt.Fprintf(writer, "TTotal (std):\t%v\n", stats.StdDev(self.TTotal.Vals))
}

// Creates a new srk manager (interface to SRK). Be sure to call mgr.Destroy()
// to clean up (failure to do so may require manual cleanup for open-lambda)
func GetMgr() *srkmgr.SrkManager {
	mgrArgs := map[string]interface{}{}
	mgrArgs["config-file"] = "./srk.yaml"
	srkLogger := logrus.New()
	srkLogger.SetLevel(logrus.WarnLevel)
	mgrArgs["logger"] = srkLogger

	mgr, err := srkmgr.NewManager(mgrArgs)
	if err != nil {
		fmt.Printf("Failed to initialize: %v\n", err)
		os.Exit(1)
	}
	return mgr
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
