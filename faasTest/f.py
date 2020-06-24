import subprocess as sp
import sys
import ctypes
import os
import random
import json
import base64

if os.path.exists('/handler'):
    cobj = "/handler/libsort.so"
else:
    cobj = "../../libsort/libsort.so"


def bytesToInts(barr):
    nInt = int(len(respBytes) / 4)
    respInts = []
    for i in range(nInt):
        respInts.append(int.from_bytes(respBytes[i*4:i*4+4], sys.byteorder))
    return respInts


def checkOrder(arr):
    prev = arr[0]
    for i in range(len(arr)):
        if arr[i] < prev:
            return (False, i)
        prev = arr[i]
    return (True, 0)


def checkSortFull(new, orig):
    origSorted = sorted(orig)
    for i in range(len(orig)):
        if new[i] != origSorted[i]:
            return (False, i)
    return (True, 0)


def f(event):
    if event['requestType'] == "generate":
        nBytes = event['test-size']
        nElem = int(nBytes / 4)
        rawBytes = bytearray(map(random.getrandbits, (8,)*nBytes))
    elif event['requestType'] == "provided":
        rawBytes = base64.b64decode(event['data'])
        nElem = int(len(rawBytes) / 4)

    cRaw = (ctypes.c_uint8 * len(rawBytes))(*rawBytes)
    cInt = ctypes.cast(cRaw, ctypes.POINTER(ctypes.c_uint))

    # Execution in OL
    sortLib = ctypes.cdll.LoadLibrary(cobj)

    # Sort cIn in-place
    res = sortLib.providedGpu(cInt, ctypes.c_size_t(nElem))

    if res:
        return {
                "success" : True,
                "result" : base64.b64encode(cRaw).decode('utf-8')
               }
    else:
        return {
                "success" : False,
                "result" : ""
               }

if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as argf:
            arg = json.loads(argf.read())
    else:
        arg = { 
                "requestType" : "generate",
                "test-size" : 1024*4,
              }

    resp = f(arg)
    if not resp['success']:
        print("Invocation failed")
    else:
        respBytes = base64.b64decode(resp['result'])
        respInts = bytesToInts(respBytes)
        if arg['requestType'] == 'generate':
            ret = checkOrder(respInts)
            if not ret[0]:
                badx = ret[1]
                print("Sort failure at index ",badx)
        else:
            origInts = bytesToInts(base64.b64decode(arg['data']))
            ret = checkSortFull(respInts, origInts)
            if not ret[0]:
                print("Sorted wrong at index ",ret[1])

    print("Success!")
    # print(json.dumps(resp))
