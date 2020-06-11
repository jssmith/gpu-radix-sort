//
// Created by Nathan Pemberton on 6/4/20.
//

#ifndef LIBSORT_LIBSORT_H
#define LIBSORT_LIBSORT_H

// Sort host-provided input (h_in) in-place using the GPU
extern "C" bool providedGpu(unsigned int* h_in, size_t len);

// Sort provided input (in) using the CPU
extern "C" bool providedCpu(unsigned int* in, size_t len);

#endif //LIBSORT_LIBSORT_H
