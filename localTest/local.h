#ifndef __LOCAL_H
#define __LOCAL_H

#include <stdio.h>
#include <iostream>
#include <ctime>
#include <string.h>
#include "../libsort/libsort.h"

#define group_bits(val, pos, width) ((val >> pos) & ((1 << width) - 1));

/*============================================
 * Tests
 *============================================
 */

// Check that arr is in order
bool checkSort(unsigned int *arr, size_t len); 

// Compare two arrays for equality
int cmpArrays(unsigned int *a, unsigned int *b, size_t len);

// pretty print a in hex
void printArray(unsigned int *a, size_t len);

// Generate a random array with nelem elements
unsigned int* generate_input(size_t nelem);

// Free an array created by generate_input
void free_input(unsigned int *in);

// Unit test for partial sort
bool testGpuPartial(uint32_t *data, size_t len);

// Runs a full suite of tests on an n element array
bool runTests(int nelem);

/*============================================
 * Benchmarks
 *============================================
 */

// This simulates a distributed sort with two partitions of the array being
// sorted independently and combined at each step. Really it's just another
// layer of standard radix sort, but it uses libsort the same way a real
// distributed sort would. Len is the number of ints in data.
bool distribSort(uint32_t *data, size_t len);

// Time how long it takes to generate n ints
bool benchGenerate(size_t n);

// Run a full suite of benchmarks. Results are printed to stdout.
bool runBenches(void);
#endif
