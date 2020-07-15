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
