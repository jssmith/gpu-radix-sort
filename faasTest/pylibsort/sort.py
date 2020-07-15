import ctypes
import ctypes.util
import sys

# from memory_profiler import profile
# import cProfile

from . import __state


# Returns the group ID of integer v for width group bits starting at pos
def groupBits(v, pos, width):
    return ((v >> pos) & ((1 << width) - 1))


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


def sortFull(buf: bytearray):
    """Interpret buf as an array of C uint32s and sort it in place."""
    nElem = int(len(buf) / 4)
    cRaw = (ctypes.c_uint8 * len(buf)).from_buffer(buf)
    cInt = ctypes.cast(cRaw, ctypes.POINTER(ctypes.c_uint))

    # Sort cIn in-place
    res = __state.sortLib.providedGpu(cInt, ctypes.c_size_t(nElem))

    if not res:
        raise RuntimeError("Libsort had an internal error")


# @profile
def sortPartial(buf: bytearray, offset, width):
    """Perform a partial sort of buf in place (width bits starting at bit
    offset) and return a list of the boundaries between each radix group."""

    nElem = int(len(buf) / 4)
    cRaw = (ctypes.c_uint8 * len(buf)).from_buffer(buf)
    cInt = ctypes.cast(cRaw, ctypes.POINTER(ctypes.c_uint32))

    boundaries = (ctypes.c_uint32 * (1 << width))()

    res = __state.sortLib.gpuPartial(cInt, boundaries,
        ctypes.c_size_t(nElem),
        ctypes.c_uint32(offset),
        ctypes.c_uint32(width))

    if not res:
        raise RuntimeError("Libsort had an internal error")

    return list(boundaries)
