package main

import (
	"fmt"
	"os"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/benchmark"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/faas"
	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/sort"
)

func main() {
	retcode := 0
	defer func() { os.Exit(retcode) }()

	fmt.Println("Getting SRK manager")
	mgr := benchmark.GetMgr()
	defer mgr.Destroy()

	inputs := sort.RandomInputs(32)
	fArg, err := faas.NewFaasArg("provided", inputs)
	if err != nil {
		fmt.Printf("Failed to create FaaS Argument: %v", err)
		retcode = 1
		return
	}

	outputs, err := benchmark.InvokeFaas(mgr, fArg)
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
