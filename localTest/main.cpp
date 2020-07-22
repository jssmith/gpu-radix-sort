#include "local.h"

// The maximum number of elements that can be handled by one GPU at once
// Our devices have 4GiB of memory and need two copies of the data so 2GB is
// max size
#define NMAX_PER_DEV (256*1024*1024)

// Number of devices on the system
#define NDEV 2

int main()
{
    if(!initLibSort()) {
      std::cerr << "Failed to initialize libsort";
      return EXIT_FAILURE;
    }

    if(!runBenches(NMAX_PER_DEV * NDEV)) {
      return EXIT_FAILURE;
    }

    // if(!runTests(4091)) {
    //   return EXIT_FAILURE;
    // }

    // if(!benchGenerate(NMAX_PER_DEV * NDEV)) {
    //   return EXIT_FAILURE;
    // }

    return EXIT_SUCCESS;
}
