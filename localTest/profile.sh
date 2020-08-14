#!/bin/bash

# --profile-from-start off: enables region-based profiling instead of profiling the whole thing
# --export-profile outputs an nvprof file that can be consumed offline in the visual profiler

nvprof --profile-from-start off --export-profile 
