import subprocess as sp
import sys
import ctypes
import os
import random
import json
import base64
import pathlib
import tempfile

import pylibsort

def f(event):
    # Ideally this would be set somewhere else (e.g. in AWS lambda you can put
    # it in /var) but for now this works.
    if pathlib.Path('/handler/libsort.so').exists():
        os.environ['LD_LIBRARY_PATH'] = '/handler'

    # Temporary limitation for testing
    if (event['offset'] != 0 or event['width'] != 32 or 
        event['arrType'] != 'file' or len(event['input']) != 1):
        return {
                "success" : False,
                "err" : "Function currently only supports full sort"
                }

    ref = pylibsort.getPartRefs(event)[0]
    rawBytes = ref.read()

    try:
        cSorted = pylibsort.sortFromBytes(rawBytes)
    except Exception as e:
        return {
                "success" : False,
                "err" : str(e)
               }

    outArr = pylibsort.getOutputArray(event)
    part = outArr.getPart(0)
    part.write(cSorted)

    return {
            "success" : True,
            "err" : "" 
           }

if __name__ == "__main__":
    """Main only used for testing purposes"""
    nbyte = 1021
    inBuf = bytes([random.getrandbits(8) for _ in range(nbyte)])
    inInts = pylibsort.bytesToInts(inBuf)

    with tempfile.TemporaryDirectory() as tDir:
        tDir = pathlib.Path(tDir)
        pylibsort.SetDistribMount(tDir)

        inArrName = "faasSortTestIn"
        outArrName = "faasSortTestOut"

        # Write source array
        inArr = pylibsort.fileDistribArray(tDir / inArrName, npart=1)
        part = inArr.getPart(0)
        part.write(inBuf)

        req = {
                "offset" : 0,
                "width" : 32,
                "arrType" : "file",
                "input" : [
                    {
                        'arrayName': inArrName,
                        'partID' : 0,
                        'start' : 0,
                        'nbyte' : -1
                    }
                ],
                "output" : outArrName
            }

        resp = f(req)
        if not resp['success']:
            print("FAILURE: Function returned error: " + resp['err'])
            exit(1)

        outArr = pylibsort.fileDistribArray(tDir / outArrName, exist_ok=True)
        outBytes = outArr.getPart(0).read()

    outInts = pylibsort.bytesToInts(outBytes)

    success, idx = pylibsort.checkSortFull(outInts, inInts)
    if not success:
        print("Test sorted wrong")
        exit(1)

    print("PASS")
    exit(0)
