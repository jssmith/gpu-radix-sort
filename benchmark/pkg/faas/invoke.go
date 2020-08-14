package faas

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
	"github.com/pkg/errors"
	"github.com/serverlessresearch/srk/pkg/srkmgr"
	"github.com/sirupsen/logrus"
)

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

func InvokeFaasSort(mgr *srkmgr.SrkManager, arg *FaasArg) error {
	jsonArg, err := json.Marshal(arg)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal FaaS argument")
	}

	rawResp, err := mgr.Provider.Faas.Invoke("radixsort", string(jsonArg))
	if err != nil {
		return fmt.Errorf("Failed to invoke function: %v\n", err)
	}

	respBytes := rawResp.Bytes()

	var resp FaasResp
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		return errors.Wrapf(err, "Couldn't parse function response: %v", string(respBytes))
	}

	if !resp.Success {
		return fmt.Errorf("Remote function error: %v", resp.Err)
	}

	return nil
}

// Returns a DistribWorker that uses mgr to sort via FaaS
func InitFaasWorker(mgr *srkmgr.SrkManager) sort.DistribWorker {
	return func(inBkts []*data.PartRef,
		offset int, width int, baseName string,
		factory *data.ArrayFactory) (data.DistribArray, error) {

		var err error

		faasRefs := make([]*FaasFilePartRef, len(inBkts))
		for i, bktRef := range inBkts {
			faasRefs[i], err = FilePartRefToFaas(bktRef)
		}

		faasArg := &FaasArg{
			Offset:  offset,
			Width:   width,
			ArrType: "file",
			Input:   faasRefs,
			Output:  baseName + "_output",
		}

		err = InvokeFaasSort(mgr, faasArg)
		if err != nil {
			return nil, errors.Wrap(err, "FaaS sort failure")
		}

		outArr, err := factory.Open(baseName + "_output")
		if err != nil {
			return nil, errors.Wrap(err, "Couldn't open output array")
		}

		return outArr, nil
	}
}
