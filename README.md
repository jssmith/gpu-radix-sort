Lambdafication of the gpu-based radix sort from
https://github.com/mark-poscablo/gpu-radix-sort.

# Components
## Libsort
This contains the core sorting routines in C++/CUDA. It includes an unoptimized
local sort and optimized GPU-based sorts. There are two main flavors of sort
included: partial and full. The full sort works totally locally using a single
GPU. The partial sorts are building blocks for a distributed sort.

## localTest
Contains basic unit tests and local sanity checks for libsort. It also provides
a minimal (and likely highest performance) baseline local sort.

## faasTest
This has the function-as-a-service worker for distributed sort. It also
includes a python package for interacting with libsort.

## benchmark
This is the primary end-to-end and full-featured sort application written in
Go. It includes abstractions for distributed data movement, a pluggable
distributed sort algorithm, and tools for interacting with FaaS through SRK.

# Quickstart

    pushd libsort && make && popd
    source env.sh

    cd localTest
    make
    ./radix_sort

See the readmes for each component for details.

# Benchmark Instructions
Data collection is spread among the different subdirectories. Analysis takes
the form of a jupyter notebook and managed in analysis/.

## End-to-End

### Go-Side
The only profiling for go-side execution has been manual invocation of golang
profiling tools. This isn't especially automated or easy to consume
programatically. We'll leave it to future work to include this (it's in the
report but not integrated with other measurements). One challenge here is
accounting for the parallel nature of the algorithm. It's hard to represent
profiling data for parallel code.

Basic timing data is printed after each run from internal timers.

Note: the bitwidth is set in pkg/sort/distrib.go:SortDistribFromArr()

### Python-Side
Python-side profiling is arguably more important. This phase is fairly
automated and can be consumed directly by the Jupyter notebook in analysis/.
faasTest/f.py includes two different main()-like functions: selfTest() and
directInvoke(). selfTest can be used for manual interactive profiling of the
function, it uses python-generated inputs and can only test the first round of
sorting (intermediate inputs aren't supported). directInvoke() is called by the
golang benchmark in lieu of a real FaaS provider, inputs are provided via
stdin. You need to manually set which main() is being used in f.py.

Outputs will be in the form of \*.prof and \*.csv files output to PWD. For
directInvoke(), profiling data will be generated for each step and worker (and
labeled as such). The csvs can be copied into analysis and consumed by the
notebook. NOTE: PWD is wherever you call the Go code from, not faasTest.

## Libsort
You can profile libsort directly in the localTest directory. There is a
profile.sh script to run nvprof with the correct arguments. You can fiddle with
the options there if you want. These are the ls\*raw dataframes in the notebook.
