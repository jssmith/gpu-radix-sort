package faas

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/data"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
	"github.com/stretchr/testify/require"
)

func TestFaasWorker(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "radixSortFaasWorkerTest")
	require.Nil(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	//Configure SRK
	//OL will mount tmpDir to the FaaS worker so it can find the distributed arrays
	os.Setenv("OL_SHARED_VOLUME", tmpDir)
	fmt.Println("Getting SRK manager")
	mgr := GetMgr()
	defer mgr.Destroy()

	arrFactory := data.NewFileArrayFactory(tmpDir)
	worker := InitFaasWorker(mgr)

	sort.DistribWorkerTest(t, arrFactory, worker)
}

func TestSortFaaS(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "radixSortFullFaasTest")
	require.Nil(t, err, "Couldn't create temporary test directory")
	defer os.RemoveAll(tmpDir)

	//Configure SRK
	//OL will mount tmpDir to the FaaS worker so it can find the distributed arrays
	os.Setenv("OL_SHARED_VOLUME", tmpDir)
	fmt.Println("Getting SRK manager")
	mgr := GetMgr()
	defer mgr.Destroy()

	arrFactory := data.NewFileArrayFactory(tmpDir)
	worker := InitFaasWorker(mgr)

	sort.SortDistribTest(t, "testSortFaaS", arrFactory, worker)
}
