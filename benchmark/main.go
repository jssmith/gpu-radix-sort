package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/serverlessresearch/srk/pkg/srkmgr"
	"github.com/sirupsen/logrus"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/faas"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
)

// Generic invoker function for tests
type invokerFunc func(*srkmgr.SrkManager) (*bytes.Buffer, error)

type faasResp struct {
	Success bool
	Result  string
}

// Creates a new srk manager (interface to SRK). Be sure to call mgr.Destroy()
// to clean up (failure to do so may require manual cleanup for open-lambda)
func getMgr() *srkmgr.SrkManager {
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

// Invoke the vector add kernel once and synchronously wait for a response.
// Returns the list of sorted integers
func invokeFaas(mgr *srkmgr.SrkManager, arg *faas.FaasArg) ([]uint32, error) {

	jsonArg, err := json.Marshal(arg)
	if err != nil {
		return nil, err
	}

	rawResp, err := mgr.Provider.Faas.Invoke("radixsort", string(jsonArg))
	if err != nil {
		return nil, fmt.Errorf("Failed to invoke function: %v\n", err)
	}

	var resp faasResp
	// skip first and last characters to strip outer quotes
	err = json.Unmarshal(rawResp.Bytes(), &resp)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("Remote Task Failed")
	}

	return faas.DecodeFaasResp(resp.Result)
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

func main() {
	retcode := 0
	defer func() { os.Exit(retcode) }()

	fmt.Println("Getting SRK manager")
	mgr := getMgr()
	defer mgr.Destroy()

	inputs := sort.RandomInputs(32)
	fArg, err := faas.NewFaasArg("provided", inputs)
	if err != nil {
		fmt.Printf("Failed to create FaaS Argument: %v", err)
		retcode = 1
		return
	}

	outputs, err := invokeFaas(mgr, fArg)
	if err != nil {
		fmt.Printf("Invocation failure: %v\n", err)
		retcode = 1
		return
	}

	err = sort.CheckSort(inputs, outputs)
	if err != nil {
		fmt.Printf("Sorted Wrong: %v\n", err)
		retcode = 1
		return
	}

	fmt.Println("Success!")
	return
}
