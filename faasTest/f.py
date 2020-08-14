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
import numpy

# ol-install: numpy

# from memory_profiler import profile
import cProfile
import pstats
import io

def printCSV(pr, path):
    result = io.StringIO()
    # pstats.Stats(pr,stream=result).print_stats()
    pstats.Stats(pr,stream=result).sort_stats(pstats.SortKey.CUMULATIVE).print_stats()
    result=result.getvalue()
    # chop the string into a csv-like buffer
    result='ncalls'+result.split('ncalls')[-1]
    result='\n'.join([','.join(line.rstrip().split(None,5)) for line in result.split('\n')])
    
    with open(path, 'w') as f:
        f.write(result)

# Ideally this would be set somewhere else (e.g. in AWS lambda you can put
# it in /var) but for now this works.
if pathlib.Path('/handler/libsort.so').exists():
    print("Running in OpenLambda")
    os.environ['LD_LIBRARY_PATH'] = '/handler'
import pylibsort

# @profile
def f(event):
    # Temporary limitation for testing
    if event['arrType'] != 'file':
        return {
                "success" : False,
                "err" : "Function currently only supports file distributed arrays"
                }

    # p = sp.run("ls -l /shared/initial", shell=True, stdout=sp.PIPE, universal_newlines=True)
    # return {
    #         "success" : False,
    #         "err" : p.stdout 
    #         }

    refs = pylibsort.getPartRefs(event)
    rawBytes = pylibsort.readPartRefs(refs)

    try:
        boundaries = pylibsort.sortPartial(rawBytes, event['offset'], event['width'])
    except Exception as e:
        return {
                "success" : False,
                "err" : str(e)
               }

    pylibsort.writeOutput(event, rawBytes, boundaries)
    
    return {
            "success" : True,
            "err" : "" 
           }

# @profile
def main():
    """Main only used for testing purposes"""
    # nElem = 256 
    nElem = 256*(1024*1024)
    # nElem = 1024*1024
    nbyte = nElem*4
    offset = 0
    width = 8
    # width = 16
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
        inShape = pylibsort.ArrayShape.fromUniform(bytesPerPart, npart)
        refs = []
        for arrX in range(narr):
            arrName = inArrName + str(arrX)
            inArr = pylibsort.fileDistribArray.Create(tDir / arrName, inShape)

            start = (arrX*npart)*bytesPerPart
            end = start + (bytesPerPart*npart)
            inArr.WriteAll(inBuf[start:end])
            for partX in range(npart):
                refs.append({
                    'arrayName': arrName,
                    'partID' : partX,
                    'start' : 0,
                    'nbyte' : -1
                })
            inArr.Close()

        req = {
                "offset" : offset,
                "width" : width,
                "arrType" : "file",
                "input" : refs,
                "output" : outArrName
        }

        start = time.time()
        pr = cProfile.Profile()
        pr.enable()
        resp = f(req)
        pr.disable()
        printCSV(pr, "./faas{}b.csv".format(width))
        pr.dump_stats("./faas{}b.prof".format(width))

        # cProfile.runctx("f(req)", globals=globals(), locals=locals(), sort="cumulative", filename="16b.prof")
        print(time.time() - start)
        if not resp['success']:
            print("FAILURE: Function returned error: " + resp['err'])
            exit(1)

        outArr = pylibsort.fileDistribArray.Open(tDir / outArrName)
        outBuf = outArr.ReadAll()
        boundaries = outArr.shape.starts

        outArr.Destroy()

    pylibsort.checkPartial(inBuf, outBuf, outArr.shape.caps, offset, width)

    print("PASS")


# @profile
def testGenerate():
    # sz = 256*1024*1024
    sz = 1024*1024
    testIn = pylibsort.generateInputs(sz)

    start = time.time()
    ints = pylibsort.bytesToInts(testIn)
    print(time.time() - start)
    # with tempfile.TemporaryFile() as f:
    #     f.write(testIn[:])


if __name__ == "__main__":
    # cProfile.run('main()', sort='cumulative')
    main()
    # testGenerate()
