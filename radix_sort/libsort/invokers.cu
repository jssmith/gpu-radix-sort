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
    // radix_sort is not in-place on the device so we have a temporary output array
    unsigned int* d_in;
    unsigned int* d_out;

    checkCudaErrors(cudaMalloc(&d_in, sizeof(unsigned int) * len));
    checkCudaErrors(cudaMalloc(&d_out, sizeof(unsigned int) * len));
    checkCudaErrors(cudaMemcpy(d_in, h_in, sizeof(unsigned int) * len, cudaMemcpyHostToDevice));
    radix_sort(d_out, d_in, len);
    checkCudaErrors(cudaMemcpy(h_in, d_out, sizeof(unsigned int) * len, cudaMemcpyDeviceToHost));
    checkCudaErrors(cudaFree(d_out));
    checkCudaErrors(cudaFree(d_in));

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
