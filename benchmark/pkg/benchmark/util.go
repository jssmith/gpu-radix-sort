package benchmark

import (
	"bytes"
	"fmt"
	"os"

	"github.com/serverlessresearch/srk/pkg/srkmgr"
	"github.com/sirupsen/logrus"
)

// Generic invoker function for tests
type invokerFunc func(*srkmgr.SrkManager) (*bytes.Buffer, error)

type faasResp struct {
	Success bool
	Result  string
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
