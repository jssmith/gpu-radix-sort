# CUDA_PATH ?= "/usr/local/cuda-7.5"
# NVCC := $(CUDA_PATH)/bin/nvcc
# PYPLOVER_DIR ?= ../../../PyPlover

NVCC := nvcc
NVCC_OPTS := -O3 -arch=compute_35 -Xcompiler -Wall -Xcompiler -Wextra -Xcompiler -fPIC -m64 -std=c++14
# NVCC_OPTS := -O0 -g -arch=compute_35 -Xcompiler -Wall -Xcompiler -Wextra -Xcompiler -fPIC -m64 -std=c++14

libsort.so: sort.o scan.o invokers.o utils.o
	$(NVCC) --shared $(NVCC_OPTS) $^ -o $@

%.o: %.cu
	$(NVCC) -c $< $(NVCC_OPTS) $(INCLUDES)

clean:
	rm -f *.o *.so 
