This is the main full-featured benchmark for distributed radix sort. It
includes go packages for distributed data movement, sorting (both local and
distributed), as well as tests and benchmarks.

# Requirements
The end-to-end benchmarks in this application will require a properly
configured SRK. If you use Go to get the SRK package, you will need to ensure
you are on the dev branch and then follow the SRK documentation for open-lambda
configuration. You will also need to use the 'sharedfs' branch of open-lambda
to get GPU and shared filesystem support. Go should handle the rest of the
dependenicies with the go modules system.

You will also need to compile libsort (also in this repo) and set
LD\_LIBRAY\_PATH appropriately (see the top-level README).

# Packages
This project follows the 'minimal main' principle with main.go mostly just
calling into the various packages (especially benchmark).

## data
This is the interface to distributed data movement. The primary abstraction is
the DistribArray which has a name and an ordered list of of partitions. There
are currently two implementations of that interface, memory and filesystem. The
memory interface is mostly useful for local testing while the filesystem is
used for interacting with FaaS-based benchmarks. See pkg/data/interface.go for
details.

## sort
This contains the main sorting algorithms. It is agnostic to the specific
DistribArray implementation and contains a number of pluggable worker
implementations for the core distributed sort algorithm.

The distributed sort is bulk-synchronous with the host managing references to
DistribArrays and launching workers to perform the partial sorts. The host
never explicitly interacts with the raw data, only passing references.

## faas
This provides helpers for interacting with SRK and the function-as-a-service
sort workers. It is primarly used by the sort package. See the README in the
faasTest section of this repository for details of the protocol.

We do not handle function installation in this application, you will need to
manually install the faas worker from ../faasTest.

## benchmark
This package provides end-to-end tests and benchmarks using various
configurations. While the other packages provide unit tests with minimal
dependencies, this package requires that you configure SRK properly and have
all the needed resources available.
