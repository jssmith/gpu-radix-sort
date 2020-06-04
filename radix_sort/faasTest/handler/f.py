import subprocess as sp
import sys
import ctypes
import os
import random

if os.path.exists('/handler'):
    cobj = "/handler/libsort.so"
else:
    cobj = "../../libsort/libsort.so"


def checkOrder(a):
    prev = a[0]
    for cur in a:
        if cur < prev:
            return False
        pref = cur
    return True

def checkSortFull(new, orig):
    origSorted = sorted(orig)
    for i in range(len(orig)):
        if new[i] != origSorted[i]:
            return i
    return -1

def f(event):
    N = event['test-size']

    # Execution in OL
    sortLib = ctypes.cdll.LoadLibrary(cobj)

    # randomIn = bytearray(map(random.getrandbits, (8,)*N))
    nElem = int(N / 32)
    randomIn = [random.randint(0, 2**32 - 1) for i in range(nElem)]

    cIn = (ctypes.c_uint * nElem)(*randomIn)
    # "nm -D vadd.so" which lists the contents of a shared library.
    res = sortLib.providedGpu(cIn, ctypes.c_size_t(nElem))

    checkRes = checkSortFull(cIn, randomIn)
    if checkRes >= 0:
        print("Sort failed! Arrays differ at ",checkRes)
        return "Failure\n"

    if res:
        return "Success\n"
    else:
        return "Failure\n"

if __name__ == "__main__":
    print(f({ "test-size" : 1024*32 }))