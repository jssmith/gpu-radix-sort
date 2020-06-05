PYPLOVER_DIR ?= $(abspath ../../../PyPlover)
SRK_DIR ?= $(abspath ../../../srk)

install:
	mkdir -p ${PYPLOVER_DIR}/handler/workloads/radixsort
	cp ../libsort/libsort.so ${PYPLOVER_DIR}/handler/workloads/radixsort/radixsort.so
	make -C ${PYPLOVER_DIR} install
