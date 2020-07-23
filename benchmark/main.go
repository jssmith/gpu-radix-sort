package main

import (
	"fmt"
	"os"

	"github.com/nathantp/gpu-radix-sort/benchmark/pkg/benchmark"
)

func main() {
	var err error

	// err = benchmark.TestFaasSortFull(4051)
	// // err = benchmark.TestFaasSortPartial(4051)
	// if err != nil {
	// 	fmt.Printf("FaaS sort test failed: %v\n", err)
	// 	retcode = 1
	// 	return
	// }

	stats, err := benchmark.RunBenchmarks()
	if err != nil {
		fmt.Println("Benchmark failed: %v\n", err)
		os.Exit(1)
	}

	stats["FileLocalDistrib"].Report(os.Stdout)

	fmt.Println("Success!")
	return
}
