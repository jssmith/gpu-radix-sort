#include "local.h"

#define DISTRIB_STEP_WIDTH 4
#define DISTRIB_NBUCKET (1 << DISTRIB_STEP_WIDTH)
#define DISTRIB_NSTEP (32 / DISTRIB_STEP_WIDTH)

bool checkSort(unsigned int *arr, size_t len)
{
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
int cmpArrays(unsigned int *a, unsigned int *b, size_t len)
{
  for(size_t i = 0; i < len; i++) {
    if(a[i] != b[i]) {
      return i;
    }
  }
  return -1;
}

void printArray(unsigned int *a, size_t len)
{
  for(size_t i = 0; i < len; i++) {
    printf("0x%8x, ", a[i]);
  }
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

bool runTests(int nelem)
{
    int diff;

    unsigned int* in = generateInput(nelem);
    unsigned int* gpuRes = new unsigned int[nelem];
    unsigned int* cpuRes = new unsigned int[nelem];
    unsigned int* distribRes = new unsigned int[nelem];
    unsigned int* gpuPartialRes = new unsigned int[nelem];

    memcpy(gpuPartialRes, in, nelem*sizeof(unsigned int));
    if(!testGpuPartial(gpuPartialRes, nelem)) {
        std::cerr << "GPU Partial Test Failed!\n";
        return false;
    }

    /* printArray(in, nelem); */
    memcpy(gpuRes, in, nelem*sizeof(unsigned int));
    if(!providedGpu(gpuRes, nelem)) {
        std::cerr << "Failure! Local GPU sorted had an internal error!\n";
        return false;
    }
    if(!checkSort(gpuRes, nelem)) {
        std::cerr << "Failure! Local GPU sorted array sorted wrong!\n";
        return false;
    }

    memcpy(cpuRes, in, nelem*sizeof(unsigned int));
    if(!providedCpu(cpuRes, nelem)) {
        std::cerr << "Failure! Local CPU sorted array sorted wrong!\n";
        return false;
    }
    if(!checkSort(cpuRes, nelem)) {
        std::cerr << "Failure! Local CPU sorted array sorted wrong!\n";
        return false;
    }

    memcpy(distribRes, in, nelem*sizeof(unsigned int));
    if(!distribSort(distribRes, nelem)) {
        std::cerr << "Failure! Distributed sorted array sorted wrong!\n";
        return false;
    }
    if(!checkSort(distribRes, nelem)) {
        std::cerr << "Failure! Distributed sorted array sorted wrong!\n";
        return false;
    }

    //Directly compare CPU and GPU based sorts (they ought to agree)
    diff = cmpArrays(gpuRes, cpuRes, nelem);
    if(diff != -1) {
      std::cerr << "CPU and GPU results disagree!";
      fprintf(stderr, "gpuRes[%d]=%u cpuRes[%d]=%u\n", diff, gpuRes[diff], diff, cpuRes[diff]);
      /* printArray(gpuRes, nelem); */
      return false;
    }

    //Directly compare Distrib and GPU based sorts (they ought to agree)
    diff = cmpArrays(gpuRes, distribRes, nelem);
    if(diff != -1) {
      std::cerr << "Distributed and Single Threaded results disagree!";
      fprintf(stderr, "single threaded Res[%d]=%u distribRes[%d]=%u\n", diff, gpuRes[diff], diff, distribRes[diff]);
      /* printArray(distribRes, nelem); */
      return false;
    }

    std::cout << "Success!\n";
    delete[] gpuRes;
    delete[] cpuRes;
    delete[] distribRes;
    freeInput(in);

    return true;
}
