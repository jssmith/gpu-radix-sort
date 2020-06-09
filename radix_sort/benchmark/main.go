package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
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
// Returns: end-to-end invocation latency in us
func invokeFaas(mgr *srkmgr.SrkManager) ([]uint32, error) {
	rawResp, err := mgr.Provider.Faas.Invoke("radixsort", exampleFaasArg)
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

	// Convert the contents from base64 encoded to bytes[]
	respBytes, err := base64.StdEncoding.DecodeString(resp.Result)
	if err != nil {
		return nil, fmt.Errorf("Failed to decode response: %v", err)
	}
	// respLen := base64.StdEncoding.DecodedLen(resp.Len() - 2)
	// respBytes := make([]byte, respLen)
	//
	// _, err = base64.StdEncoding.DecodeStringll(respBytes, resp.Bytes()[1:resp.Len()-1])
	// if err != nil {
	// 	fmt.Println("decode error:", err)
	// }

	// Convert bytes to float
	numElem := len(respBytes) / 4
	respInts := make([]uint32, numElem)
	for i := 0; i < numElem; i++ {
		respInts[i] = binary.LittleEndian.Uint32(respBytes[(i * 4):(i*4 + 4)])
	}

	return respInts, nil
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

func checkRes(res []uint32) error {
	if len(res) != len(goldenOutput) {
		return fmt.Errorf("Lengths do not match: Expected %v, Got %v\n", len(goldenOutput), len(res))
	}

	for i := 0; i < len(res); i++ {
		if res[i] != goldenOutput[i] {
			return fmt.Errorf("Response doesn't match reference at %v\n: Expected %v, Got %v\n", i, goldenOutput[i], res[i])
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

	fmt.Println("Invoking FaaS Sort")
	// oneResp(invokeFaas, mgr)
	ints, err := invokeFaas(mgr)
	if err != nil {
		fmt.Printf("Invocation failure: %v\n", err)
		retcode = 1
		return
	}

	err = checkRes(ints)
	if err != nil {
		fmt.Printf("Failure: %v\n", err)
		retcode = 1
		return
	}

	fmt.Println("Success!")
	return
}
