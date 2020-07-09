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
    if event['arrType'] != 'file':
        return {
                "success" : False,
                "err" : "Function currently only supports file distributed arrays"
                }

    if len(event['input']) != 1:
        return {
                "success" : False,
                "err" : "Function currently only supports a single input (no gather support yet)"
                }

    ref = pylibsort.getPartRefs(event)[0]
    rawBytes = ref.read()

    try:
        boundaries = pylibsort.sortPartial(rawBytes, event['offset'], event['width'])
    except Exception as e:
        return {
                "success" : False,
                "err" : str(e)
               }

    # Convert boundaries to byte addresses and add a boundary after the last group
    boundaries = list(map(lambda x: x*4, boundaries))
    boundaries.append(len(rawBytes))

    outArr = pylibsort.getOutputArray(event)
    for i, p in enumerate(outArr.getParts()):
        p.write(rawBytes[boundaries[i]:boundaries[i+1]])

    return {
            "success" : True,
            "err" : "" 
           }

if __name__ == "__main__":
    """Main only used for testing purposes"""
    nElem = 1021
    nbyte = nElem*4
    offset = 0
    width = 4

    inBuf = bytearray([random.getrandbits(8) for _ in range(nbyte)])
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
                "offset" : offset,
                "width" : width,
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

        groupOuts = []
        parts = outArr.getParts()
        for p in parts:
            groupOuts.append(p.read())

    if len(groupOuts) != (1 << width):
        print("FAILURE: Too few groups. Expected {}, Got {}".format(1 << width, len(groupOuts)))
        exit(1)

    retNElem = 0
    for i, g in enumerate(groupOuts):
        gInts = pylibsort.bytesToInts(g)
        retNElem += len(gInts)

        badGroups = filter(lambda x: pylibsort.groupBits(x, offset, width) != i, gInts)
        firstBad = next(badGroups, None)
        if firstBad is not None:
            print("FAILURE: Group {} has element with groupID {}".format(
                i, pylibsort.groupBits(firstBad, offset, width)))
            exit(1)

    if retNElem != nElem:
        print("FAILURE: Not enough elements returned. Expected {}, Got {}".format(nElem, retNElem))
        exit(1)

    print("PASS")
    exit(0)
