# CUDA_PATH ?= "/usr/local/cuda-7.5"
# NVCC := $(CUDA_PATH)/bin/nvcc
NVCC := nvcc
NVCC_OPTS=-O3 -arch=compute_35 -Xcompiler -Wall -Xcompiler -Wextra -m64
LIBS=-L../libsort/ -lsort

radix_sort: main.cu Makefile ../libsort/libsort.so
	$(NVCC) -o radix_sort main.cu $(NVCC_OPTS) $(LIBS)

clean:
	rm -f *.o radix_sort
