# CUDA_PATH ?= "/usr/local/cuda-7.5"
# NVCC := $(CUDA_PATH)/bin/nvcc
NVCC := nvcc
NVCC_OPTS=-O3 -arch=compute_35 -Xcompiler -Wall -Xcompiler -Wextra -Xcompiler -fPIC -m64

libsort.so: sort.o scan.o
	nvcc --shared -Xcompiler -fPIC -shared $^ -o $@

sort.o: sort.cu
	$(NVCC) -c sort.cu $(NVCC_OPTS)

scan.o: scan.cu
	$(NVCC) -c scan.cu $(NVCC_OPTS)

clean:
	rm -f *.o *.so 
