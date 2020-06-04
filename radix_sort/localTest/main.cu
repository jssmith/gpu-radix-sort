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
    }
    return true;
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
    unsigned int* tmp = new unsigned int[nelem];

    memcpy(tmp, in, nelem*sizeof(unsigned int));
    if(!providedGpu(tmp, nelem)) {
        std::cerr << "Failure! Local GPU sorted had an internal error!\n";
        return EXIT_FAILURE;
    }
    if(!checkSort(tmp, nelem)) {
        std::cerr << "Failure! Local GPU sorted array sorted wrong!\n";
        return EXIT_FAILURE;
    }

    memcpy(tmp, in, nelem*sizeof(unsigned int));
    if(!providedCpu(tmp, nelem)) {
        std::cerr << "Failure! Local CPU sorted array sorted wrong!\n";
        return EXIT_FAILURE;
    }
    if(!checkSort(tmp, nelem)) {
        std::cerr << "Failure! Local CPU sorted array sorted wrong!\n";
        return EXIT_FAILURE;
    }

    std::cout << "Success!\n";
    free_input(in);

    return EXIT_SUCCESS;
}
