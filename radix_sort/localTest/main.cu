#include <stdio.h>
#include <iostream>
#include <ctime>

#include "../libsort/libsort.h"
//#include "pyplover.h"

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
    printf("%u, ", a[i]);
  }
}

unsigned int* generate_input(size_t nelem) {
    unsigned int* in = new unsigned int[nelem];

    srand(1);
    for (unsigned int j = 0; j < nelem; j++)
    {
        in[j] = rand() % nelem;
    }
    return in;
}

// Free the output of generate_input
void free_input(unsigned int *in) {
    delete[] in;
}

int main()
{
    size_t nelem = 1024;

    unsigned int* in = generate_input(nelem);
    unsigned int* gpuRes = new unsigned int[nelem];
    unsigned int* cpuRes = new unsigned int[nelem];

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

    //Directly compare CPU and GPU based sorts (they ought to agree)
    int diff = cmpArrays(gpuRes, cpuRes, nelem);
    if(diff != -1) {
      std::cerr << "CPU and GPU results disagree!";
      fprintf(stderr, "gpuRes[%d]=%u cpuRes[%d]=%u\n", diff, gpuRes[diff], diff, cpuRes[diff]);
      printArray(gpuRes, nelem);
      return EXIT_FAILURE;
    }
    /* printArray(gpuRes, nelem); */

    std::cout << "Success!\n";
    delete[] gpuRes;
    delete[] cpuRes;
    free_input(in);

    return EXIT_SUCCESS;
}
