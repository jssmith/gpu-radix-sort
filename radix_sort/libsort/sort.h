#ifndef SORT_H__
#define SORT_H__

#include "cuda_runtime.h"
#include "device_launch_parameters.h"
#include "scan.h"
#include <cmath>

typedef struct sort_state {
  // Input and output device pointers
  unsigned int *d_out;
  unsigned int *d_in;

  // The per-block, per-bit prefix sums (where this value goes in the per-block 2bit group)
  unsigned int *d_prefix_sums;

  // per-block starting index (count) of each 2bit grouped by 2bit (d_block_sums[0-nblock] are all the 0 2bits)
  unsigned int *d_block_sums;

  // prefix-sum of d_block_sums, e.g. the starting position for each block's 2bit group
  // (d_scan_block_sums[1] is where block 1's 2bit group 0 should start)
  unsigned int *d_scan_block_sums;

  unsigned int data_len;
  unsigned int block_sz;
  unsigned int grid_sz;
  unsigned int shmem_sz;
  unsigned int prefix_sums_len;
  unsigned int block_sums_len;
  unsigned int scan_block_sums_len;
} sort_state_t;

void radix_sort(unsigned int* const d_out,
    unsigned int* const d_in,
    unsigned int d_in_len);

#endif
