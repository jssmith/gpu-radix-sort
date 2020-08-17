package faas

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
	"github.com/pkg/errors"
	"github.com/serverlessresearch/srk/pkg/srkmgr"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

type gpuReserver struct {
	devSemaphore *semaphore.Weighted
	devs         []uint32
}

func (self *gpuReserver) reserve() (devId int, err error) {
	self.devSemaphore.Acquire(context.Background(), 1)

	devId = -1
	for i := 0; i < len(self.devs); i++ {
		success := atomic.CompareAndSwapUint32(&self.devs[i], (uint32)(0), (uint32)(1))
		if success {
			devId = i
			break
		}
	}

	// The semaphore ensures the above loop will succeed. This check should
	// never fail.
	if devId == -1 {
		return devId, fmt.Errorf("Failed to find free device. This shouldn't happen!")
	}

	return devId, nil
}

func (self *gpuReserver) release(devId int) {
	atomic.StoreUint32(&self.devs[devId], 0)
	self.devSemaphore.Release(1)
}

var gpuManager gpuReserver

func init() {
	// Determine the number of GPUs
	cmd := exec.Command("nvidia-smi", "-L")
	out, err := cmd.Output()
	if err != nil {
		panic(fmt.Sprintf("Error determining GPU count: %v", err))
	}

	nDev := bytes.Count(out, []byte("\n"))

	gpuManager = gpuReserver{semaphore.NewWeighted((int64)(nDev)), make([]uint32, nDev)}
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

// Run the function as a local process instead of through SRK. This is kind of
// a hack and is fragile, ideally we'd get something close to this in SRK and
// avoid the hacks here. Arguments are passed through the command line args,
// keep them under your system limit (typically 2MB).
//
// You muxt set RADIXBENCH_ROOTPATH to the root dir of the gpu-radix-sort repo
// in your environment for this to work properly (source env.sh).
func InvokeFaasDirect(arg *FaasArg) error {
	devId, err := gpuManager.reserve()
	if err != nil {
		return errors.Wrap(err, "Failed to find free GPU")
	}
	defer gpuManager.release(devId)

	rootPath := os.Getenv("RADIXBENCH_ROOTPATH")
	if rootPath == "" {
		return fmt.Errorf("RADIXBENC_ROOTPATH environment variable not set. Set this to the root of the gpu-radix-sort repo.")
	}

	jsonArg, err := json.Marshal(arg)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal FaaS argument")
	}

	funcPath := filepath.Join(rootPath, "faasTest/f.py")
	cmd := exec.Command("python3", funcPath)

	cmd.Env = append(os.Environ(),
		fmt.Sprintf("CUDA_VISIBLE_DEVICES=%v", devId))

	cmdIn, err := cmd.StdinPipe()
	if err != nil {
		return errors.Wrap(err, "Couldn't get stdin pipe for worker process")
	}

	go func() {
		defer cmdIn.Close()
		cmdIn.Write(jsonArg)
	}()

	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("Worker returned error: %s", exitErr.Stderr)
		}

		return errors.Wrapf(err, "Failed to invoke worker")
	}

	var resp FaasResp
	err = json.Unmarshal(out, &resp)
	if err != nil {
		return errors.Wrapf(err, "Couldn't parse function response: %q", out)
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

		// err = InvokeFaasSort(mgr, faasArg)

		// OL has been giving us some trouble (numpy not importing correctly),
		// we're calling directly for now. Technically we don't need mgr, but
		// I'm lazy and don't feel like refactoring code just for a hack. We
		// can create new code paths if we need. mgr can be nil in this mode.
		err = InvokeFaasDirect(faasArg)
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
