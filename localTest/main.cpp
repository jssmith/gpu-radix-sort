#include "local.h"

int main()
{
    if(!initLibSort()) {
      std::cerr << "Failed to initialize libsort";
      return EXIT_FAILURE;
    }

    if(!runBenches()) {
      return EXIT_FAILURE;
    }

    // if(!runTests(1111)) {
    //   return EXIT_FAILURE;
    // }

    // if(!benchGenerate(NMAX_PER_DEV * NDEV)) {
    //   return EXIT_FAILURE;
    // }

    return EXIT_SUCCESS;
}
