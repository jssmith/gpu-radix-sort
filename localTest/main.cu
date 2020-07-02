#include <stdio.h>
#include <iostream>
#include <ctime>

#include "../libsort/libsort.h"
//#include "pyplover.h"

#define DISTRIB_STEP_WIDTH 4
#define DISTRIB_NBUCKET (1 << DISTRIB_STEP_WIDTH)
#define DISTRIB_NSTEP (32 / DISTRIB_STEP_WIDTH)

#define group_bits(val, pos, width) ((val >> pos) & ((1 << width) - 1));

bool checkSort(unsigned int *arr, size_t len) {
    unsigned int prev = 0;
    for(size_t i = 0; i < len; i++) {
        if(arr[i] < prev) {
            return false;
        }
        prev = arr[i];
    }
    return true;
}

// Compare two arrays. Return the index of the first item that differs or -1 if
// they are identical.
int cmpArrays(unsigned int *a, unsigned int *b, size_t len) {
  for(size_t i = 0; i < len; i++) {
    if(a[i] != b[i]) {
      return i;
    }
  }
  return -1;
}

void printArray(unsigned int *a, size_t len) {
  for(size_t i = 0; i < len; i++) {
    printf("0x%8x, ", a[i]);
  }
}

unsigned int* generate_input(size_t nelem)
{
    unsigned int* in = new unsigned int[nelem];

    srand(1);
    for (unsigned int j = 0; j < nelem; j++)
    {
        in[j] = rand();
    }
    return in;
}

// Free the output of generate_input
void free_input(unsigned int *in) {
    delete[] in;
}

bool testGpuPartial(uint32_t *data, size_t len)
{
  uint32_t *testBounds = new uint32_t[DISTRIB_NBUCKET];
  uint32_t *radixCounts = new uint32_t[DISTRIB_NBUCKET + 1]();
  uint32_t *refBounds = new uint32_t[DISTRIB_NBUCKET + 1]();

  for(size_t i = 0; i < len; i++) {
    int radix = group_bits(data[i], 0, DISTRIB_STEP_WIDTH);
    radixCounts[radix]++;
  }

  uint32_t prev = 0;
  for(int i = 0; i < DISTRIB_NBUCKET; i++) {
    refBounds[i] = prev;
    prev += radixCounts[i];
  }
  // makes checking loops simpler
  refBounds[DISTRIB_NBUCKET] = len;

  if(!gpuPartial(data, testBounds, len, 0, DISTRIB_STEP_WIDTH)) {
    fprintf(stderr, "gpuPartial call failed\n");
    return false;
  }

  // Make sure boundaries are correct
  for(int i = 0; i < DISTRIB_NBUCKET; i++) {
    if(refBounds[i] != testBounds[i]) {
      fprintf(stderr, "Test Failure: boundary %d wrong, expected: %d, got %d\n", i,
          refBounds[i], testBounds[i]);
      return false;
    }
  }

  // Make sure data was sorted right
  uint32_t refRadix = 0;
  for(size_t i = 0; i < len; i++) {
    uint32_t testRadix = group_bits(data[i], 0, DISTRIB_STEP_WIDTH);

    if(i == refBounds[refRadix+1]) {
      refRadix++;
    }

    if(testRadix != refRadix) {
      fprintf(stderr, "Test Failure: Wrong radix for data[%zu]. Expected %u, Got %u\n", i, refRadix, testRadix);
      return false;
    }
  }
    
  return true;
}

// This simulates a distributed sort with two partitions of the array being
// sorted independently and combined at each step. Really it's just another
// layer of standard radix sort, but it uses libsort the same way a real
// distributed sort would.
bool distribSort(uint32_t *data, size_t len)
{
  // temporary place for shuffle outputs
  uint32_t *tmp = new uint32_t[len*sizeof(uint32_t)];

  // Boundary of each radix bucket in each partition
  uint32_t *p1_bkt_bounds = new uint32_t[DISTRIB_NBUCKET];
  uint32_t *p2_bkt_bounds = new uint32_t[DISTRIB_NBUCKET];

  size_t p1len = len / 2;
  size_t p2len = (len / 2) + (len % 2);
  
  // We zero copy between steps by switching which array we're using as 'data' and which is for shuffling.
  uint32_t *cur = data;
  uint32_t *next = tmp;
  for(int i = 0; i < DISTRIB_NSTEP; i++) {
    uint32_t *p1 = &cur[0];
    uint32_t *p2 = &cur[p1len];

    if(!gpuPartial(p1, p1_bkt_bounds, p1len, i*DISTRIB_STEP_WIDTH, DISTRIB_STEP_WIDTH)) {
      return false;
    }
    if(!gpuPartial(p2, p2_bkt_bounds, p2len, i*DISTRIB_STEP_WIDTH, DISTRIB_STEP_WIDTH)) {
      return false;
    }

    //shuffle
    size_t next_slot = 0;
    for(int bkt = 0; bkt < DISTRIB_NBUCKET; bkt++) {
      size_t p1_bkt_len, p2_bkt_len;
      if(bkt == DISTRIB_NBUCKET - 1) {
        p1_bkt_len = p1len - p1_bkt_bounds[bkt];
        p2_bkt_len = p2len - p2_bkt_bounds[bkt];
      } else {
        p1_bkt_len = p1_bkt_bounds[bkt+1] - p1_bkt_bounds[bkt];
        p2_bkt_len = p2_bkt_bounds[bkt+1] - p2_bkt_bounds[bkt];
      }

      memcpy(&next[next_slot], &p1[p1_bkt_bounds[bkt]], p1_bkt_len*sizeof(uint32_t));
      next_slot += p1_bkt_len;

      memcpy(&next[next_slot], &p2[p2_bkt_bounds[bkt]], p2_bkt_len*sizeof(uint32_t));
      next_slot += p2_bkt_len;
    }

    //swap inputs for zcopy rounds
    uint32_t *tmp = next;
    next = cur;
    cur = tmp;
  }

  return true;
}

int main()
{
    size_t nelem = 1021;
    int diff;

    unsigned int* in = generate_input(nelem);
    unsigned int* gpuRes = new unsigned int[nelem];
    unsigned int* cpuRes = new unsigned int[nelem];
    unsigned int* distribRes = new unsigned int[nelem];
    unsigned int* gpuPartialRes = new unsigned int[nelem];

    if(!initLibSort()) {
      std::cerr << "Failed to initialize libsort\n";
      return EXIT_FAILURE;
    }

    memcpy(gpuPartialRes, in, nelem*sizeof(unsigned int));
    if(!testGpuPartial(gpuPartialRes, nelem)) {
        std::cerr << "GPU Partial Test Failed!\n";
        return EXIT_FAILURE;
    }

    /* printArray(in, nelem); */
    memcpy(gpuRes, in, nelem*sizeof(unsigned int));
    if(!providedGpu(gpuRes, nelem)) {
        std::cerr << "Failure! Local GPU sorted had an internal error!\n";
        return EXIT_FAILURE;
    }
    if(!checkSort(gpuRes, nelem)) {
        std::cerr << "Failure! Local GPU sorted array sorted wrong!\n";
        return EXIT_FAILURE;
    }

    memcpy(cpuRes, in, nelem*sizeof(unsigned int));
    if(!providedCpu(cpuRes, nelem)) {
        std::cerr << "Failure! Local CPU sorted array sorted wrong!\n";
        return EXIT_FAILURE;
    }
    if(!checkSort(cpuRes, nelem)) {
        std::cerr << "Failure! Local CPU sorted array sorted wrong!\n";
        return EXIT_FAILURE;
    }

    memcpy(distribRes, in, nelem*sizeof(unsigned int));
    if(!distribSort(distribRes, nelem)) {
        std::cerr << "Failure! Distributed sorted array sorted wrong!\n";
        return EXIT_FAILURE;
    }
    if(!checkSort(distribRes, nelem)) {
        std::cerr << "Failure! Distributed sorted array sorted wrong!\n";
        return EXIT_FAILURE;
    }

    //Directly compare CPU and GPU based sorts (they ought to agree)
    diff = cmpArrays(gpuRes, cpuRes, nelem);
    if(diff != -1) {
      std::cerr << "CPU and GPU results disagree!";
      fprintf(stderr, "gpuRes[%d]=%u cpuRes[%d]=%u\n", diff, gpuRes[diff], diff, cpuRes[diff]);
      /* printArray(gpuRes, nelem); */
      return EXIT_FAILURE;
    }

    //Directly compare Distrib and GPU based sorts (they ought to agree)
    diff = cmpArrays(gpuRes, distribRes, nelem);
    if(diff != -1) {
      std::cerr << "Distributed and Single Threaded results disagree!";
      fprintf(stderr, "single threaded Res[%d]=%u distribRes[%d]=%u\n", diff, gpuRes[diff], diff, distribRes[diff]);
      /* printArray(distribRes, nelem); */
      return EXIT_FAILURE;
    }
    
    std::cout << "Success!\n";
    delete[] gpuRes;
    delete[] cpuRes;
    delete[] distribRes;
    free_input(in);

    return EXIT_SUCCESS;
}
