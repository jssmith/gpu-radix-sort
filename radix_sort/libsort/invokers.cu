// Various invocation methods for libsort
#include <stdio.h>
#include <string.h>
#include <cuda_runtime.h>
#include <algorithm>

#include "sort.h"
#include "utils.h"
/* #include "pyplover.h" */

// Sort provided input (h_in) in-place using the GPU
// Returns success status
extern "C" bool providedGpu(unsigned int* h_in, size_t len)
{
    SortState state (h_in, len);
    state.Step(0, 32);
    state.GetResult(h_in);
    return true;
}

// Sort provided input (in) using the CPU
// returns success status
extern "C" bool providedCpu(unsigned int* in, size_t len) {
    std::sort(in, in + len);
    return true;
}

// This function can be called by PyPlover as a KaaS function.
/* extern "C" void kaasInvoke(state_t *s, int grid, int block) { */
/*     radix_sort(s->out.dat, s->in.dat, s->in.len / sizeof(unsigned int)); */
/* } */
