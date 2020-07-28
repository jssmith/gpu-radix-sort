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
	// 	os.Exit(1)
	// }

	stats, err := benchmark.RunBenchmarks()
	if err != nil {
		fmt.Println("Benchmark failed: %v\n", err)
		os.Exit(1)
	}

	for testName, testStats := range stats {
		fmt.Printf("%v\n", testName)
		benchmark.ReportStats(testStats, os.Stdout)
	}

	fmt.Println("Success!")
	return
}
