import subprocess as sp
import sys
import ctypes
import os
import random
import json
import base64
import pathlib

import pylibsort

def f(event):
    # Ideally this would be set somewhere else (e.g. in AWS lambda you can put
    # it in /var) but for now this works.
    if pathlib.Path('/handler/libsort.so').exists():
        os.environ['LD_LIBRARY_PATH'] = '/handler'

    rawBytes = base64.b64decode(event['data'])
    try:
        cSorted = pylibsort.sortFromBytes(rawBytes)
    except Exception as e:
        return {
                "success" : False,
                "result" : str(e)
               }

    return {
            "success" : True,
            "result" : base64.b64encode(cSorted).decode('utf-8')
           }

if __name__ == "__main__":
    """Main only used for testing purposes"""
    nbyte = 1021
    inBuf = bytes([random.getrandbits(8) for _ in range(nbyte)])
    inInts = pylibsort.bytesToInts(inBuf)

    arg = {
            "data" : base64.b64encode(inBuf).decode('utf-8')
          }

    resp = f(arg)
    if not resp['success']:
        print("FAILURE: Function returned error: " + resp['result'])
        exit(1)

    respBytes = base64.b64decode(resp['result'])
    respInts = pylibsort.bytesToInts(respBytes)

    success, idx = pylibsort.checkSortFull(respInts, inInts)
    if not success:
        print("FAILURE: Sorted Incorrectly at index " + str(idx))
        exit(1)

    print("PASS")
    exit(0)
