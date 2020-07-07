import ctypes
import ctypes.util
import sys

def bytesToInts(barr):
    """Convert bytes to a list of python integers"""
    nInt = int(len(barr) / 4)
    respInts = []
    for i in range(nInt):
        respInts.append(int.from_bytes(barr[i*4:i*4+4], sys.byteorder))
    return respInts


def checkOrder(arr):
    """Verify that arr is in-order"""
    prev = arr[0]
    for i in range(len(arr)):
        if arr[i] < prev:
            return (False, i)
        prev = arr[i]
    return (True, 0)


def checkSortFull(new, orig):
    """Verify that new is a correctly sorted copy of orig"""
    origSorted = sorted(orig)
    for i in range(len(orig)):
        if new[i] != origSorted[i]:
            return (False, i)
    return (True, 0)


def sortFromBytes(buf):
    """Interpret buf (a bytearray) as an array of C uint32s and sort it"""
    nElem = int(len(buf) / 4)
    cRaw = (ctypes.c_uint8 * len(buf))(*buf)
    cInt = ctypes.cast(cRaw, ctypes.POINTER(ctypes.c_uint))

    # Not sure what is wrong here, ctypes.util.find_library is the right
    # function to call but it doesn't find libsort. The internal function
    # here does find it. The problem is in ctypes.util._get_soname which has
    # some error about the dynamic section when calling objdump -p -j .dynamic
    # libsort.so. This hack works for now.
    libsortPath = ctypes.util._findLib_ld("sort")
    if libsortPath is None:
        raise RuntimeError("libsort could not be located, be sure libsort.so is on your library search path (e.g. with LD_LIBRARY_PATH)")

    sortLib = ctypes.cdll.LoadLibrary(libsortPath)

    # Sort cIn in-place
    res = sortLib.providedGpu(cInt, ctypes.c_size_t(nElem))

    if not res:
        raise RuntimeError("Libsort had an internal error")

    return cRaw

