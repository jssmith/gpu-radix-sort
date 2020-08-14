#!/bin/bash
set -ex

USAGE="usage: ./profile.sh logname.csv"

if [[ $# -ne 1 ]]; then
  echo $USAGE
  exit 1
fi

# --profile-from-start off: enables region-based profiling instead of profiling the whole thing
# --log-file: put basic profile summary in csv form to $1
nvprof --normalized-time-unit ms --profile-from-start off --csv --log-file $1 ./radix_sort
# nvprof --cpu-profiling-frequency 500Hz --cpu-profiling on --cpu-profiling-max-depth 0 --profile-from-start off --csv --log-file $1 ./radix_sort

# Other options to consider:
# --export-profile outputs an nvprof file that can be consumed offline in the visual profiler
