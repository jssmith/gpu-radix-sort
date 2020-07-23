import subprocess as sp
import sys
import ctypes
import os
import random
import json
import base64
import pathlib
import tempfile
import functools
import operator
import time

from memory_profiler import profile
# import cProfile

# Ideally this would be set somewhere else (e.g. in AWS lambda you can put
# it in /var) but for now this works.
if pathlib.Path('/handler/libsort.so').exists():
    print("Running in OpenLambda")
    os.environ['LD_LIBRARY_PATH'] = '/handler'
import pylibsort

@profile
def f(event):
    # Temporary limitation for testing
    if event['arrType'] != 'file':
        return {
                "success" : False,
                "err" : "Function currently only supports file distributed arrays"
                }

    refs = pylibsort.getPartRefs(event)
    rawBytes = functools.reduce(operator.iconcat,
                    map(operator.methodcaller('read'), refs))

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

# @profile
def main():
    """Main only used for testing purposes"""
    # nElem = 4000
    nElem = 256*(1024*1024)
    nbyte = nElem*4
    offset = 0
    width = 4
    narr = 2
    npart = 2
    bytesPerPart = int(nbyte / (narr * npart))

    inBuf = pylibsort.generateInputs(nElem)

    with tempfile.TemporaryDirectory() as tDir:
        tDir = pathlib.Path(tDir)
        pylibsort.SetDistribMount(tDir)

        inArrName = "faasSortTestIn"
        outArrName = "faasSortTestOut"

        # Write source arrays
        refs = []
        for arrX in range(narr):
            arrName = inArrName + str(arrX)
            inArr = pylibsort.fileDistribArray(tDir / arrName, npart=npart)
            for partX in range(npart):
                start = ((arrX*npart) + partX)*bytesPerPart
                part = inArr.getPart(partX)
                part.write(inBuf[start:start+bytesPerPart])

                refs.append({
                    'arrayName': arrName,
                    'partID' : partX,
                    'start' : 0,
                    'nbyte' : -1
                })

        req = {
                "offset" : offset,
                "width" : width,
                "arrType" : "file",
                "input" : refs,
                "output" : outArrName
        }

        del inBuf

        start = time.time()
        resp = f(req)
        print(time.time() - start)
        if not resp['success']:
            print("FAILURE: Function returned error: " + resp['err'])
            exit(1)

    #     inInts = pylibsort.bytesToInts(inBuf)
    #     outArr = pylibsort.fileDistribArray(tDir / outArrName, exist_ok=True)
    #
    #     groupOuts = []
    #     parts = outArr.getParts()
    #     for p in parts:
    #         groupOuts.append(p.read())
    #
    # if len(groupOuts) != (1 << width):
    #     print("FAILURE: Too few groups. Expected {}, Got {}".format(1 << width, len(groupOuts)))
    #     exit(1)
    #
    # retNElem = 0
    # for i, g in enumerate(groupOuts):
    #     gInts = pylibsort.bytesToInts(g)
    #     retNElem += len(gInts)
    #
    #     badGroups = filter(lambda x: pylibsort.groupBits(x, offset, width) != i, gInts)
    #     firstBad = next(badGroups, None)
    #     if firstBad is not None:
    #         print("FAILURE: Group {} has element with groupID {}".format(
    #             i, pylibsort.groupBits(firstBad, offset, width)))
    #         exit(1)
    #
    # if retNElem != nElem:
    #     print("FAILURE: Not enough elements returned. Expected {}, Got {}".format(nElem, retNElem))
    #     exit(1)
    #
    print("PASS")


# @profile
def testGenerate():
    # sz = 256*1024*1024
    sz = 1024
    testIn = pylibsort.generateInputs(sz)

    with tempfile.TemporaryFile() as f:
        f.write(testIn[:])


if __name__ == "__main__":
    # cProfile.run('main()', sort='cumulative')
    main()
    # testGenerate()

