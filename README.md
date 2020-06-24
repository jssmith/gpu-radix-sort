Lambdafication of the gpu-based radix sort from
https://github.com/mark-poscablo/gpu-radix-sort. The core GPU-based sort
algorithm is in libsort.so. localTest has a c-native test to make sure
libsort.so is working.

# Quickstart

    pushd libsort && make && popd
    cd localTest && make
    LD_LIBRARY_PATH=../libsort ./radix_sort
