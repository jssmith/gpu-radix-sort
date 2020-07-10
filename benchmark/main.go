package main

import (
	"fmt"
	"os"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/benchmark"
)

func main() {
	var err error
	retcode := 0
	defer func() { os.Exit(retcode) }()

	fmt.Println("Getting SRK manager")
	mgr := benchmark.GetMgr()
	defer mgr.Destroy()

	err = benchmark.TestFaasSortPartial(mgr, 4051)
	if err != nil {
		fmt.Printf("FaaS sort test failed: %v\n", err)
		retcode = 1
		return
	}

	fmt.Println("Success!")
	return
}
