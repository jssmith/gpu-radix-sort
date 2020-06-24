package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"

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

func b64ToIntSlice(encoded string) ([]uint32, error) {
	// Convert the contents from base64 encoded to bytes[]
	bytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("Failed to decode response: %v", err)
	}

	numElem := len(bytes) / 4
	ints := make([]uint32, numElem)
	for i := 0; i < numElem; i++ {
		ints[i] = binary.LittleEndian.Uint32(bytes[(i * 4):(i*4 + 4)])
	}

	return ints, nil
}

// Invoke the vector add kernel once and synchronously wait for a response.
// Returns the list of sorted integers
func invokeFaas(mgr *srkmgr.SrkManager, arg *FaasArg) ([]uint32, error) {

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

	return b64ToIntSlice(resp.Result)
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

func generateInputs(len int) []uint32 {
	rand.Seed(0)
	out := make([]uint32, len)
	for i := 0; i < len; i++ {
		out[i] = rand.Uint32()
	}
	return out
}

func checkRes(orig []uint32, new []uint32) error {
	if len(orig) != len(new) {
		return fmt.Errorf("Lengths do not match: Expected %v, Got %v\n", len(orig), len(new))
	}

	origCpy := make([]uint32, len(orig))
	copy(origCpy, orig)
	sort.Slice(origCpy, func(i, j int) bool { return origCpy[i] < origCpy[j] })
	for i := 0; i < len(orig); i++ {
		if origCpy[i] != new[i] {
			return fmt.Errorf("Response doesn't match reference at %v\n: Expected %v, Got %v\n", i, origCpy[i], new[i])
		}
	}
	return nil
}

func main() {
	retcode := 0
	defer func() { os.Exit(retcode) }()

	fmt.Println("Getting SRK manager")
	mgr := getMgr()
	defer mgr.Destroy()

	inputs := generateInputs(32)
	fArg, err := NewFaasArg("provided", inputs)
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

	err = checkRes(inputs, outputs)
	if err != nil {
		fmt.Printf("Sorted Wrong: %v\n", err)
		retcode = 1
		return
	}

	fmt.Println("Success!")
	return
}
