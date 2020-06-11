# CUDA_PATH ?= "/usr/local/cuda-7.5"
# NVCC := $(CUDA_PATH)/bin/nvcc
PYPLOVER_DIR ?= ../../../PyPlover

NVCC := nvcc
NVCC_OPTS := -O3 -arch=compute_35 -Xcompiler -Wall -Xcompiler -Wextra -Xcompiler -fPIC -m64
INCLUDES := -I$(PYPLOVER_DIR)/lib/include

libsort.so: sort.o scan.o invokers.o
	nvcc --shared -Xcompiler -fPIC -shared $^ -o $@

%.o: %.cu
	$(NVCC) -c $< $(NVCC_OPTS) $(INCLUDES)

clean:
	rm -f *.o *.so 
