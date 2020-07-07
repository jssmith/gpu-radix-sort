import pathlib
import tempfile
import collections.abc
import random

import pylibsort

class testException(Exception):
    def __init__(self, tname, msg):
        self.tname = tname
        self.msg = msg

    def __str__(self):
        return('Test "{}" Failure: {}'.format(self.tname, self.msg))

def fillPart(part, nbyte):
    """Fill a partition with nbyte random numbers, returns the bytes object
    used to fill"""
    inBuf = bytes([random.getrandbits(8) for _ in range(nbyte)])
    part.write(inBuf)
    return inBuf


def fillArr(arr, partNByte):
    """Fill the partitions of an array with partNByte bytes. partNByte can be a
    scalar (in which case all parts get the same size), or an iterable with one
    entry per partition for heterogenous sizes."""
    parts = arr.getParts()
    npart = arr.nParts()
    inBufs = []
    
    if not isinstance(partNByte, collections.abc.Iterable):
        partNByte = [partNByte]*npart

    for part, nbyte in zip(parts, partNByte):
        inBufs.append(fillPart(part, nbyte))

    return inBufs


def testFileDistribPart():
    with tempfile.TemporaryDirectory() as aDir:
        pname = pathlib.Path(aDir) / "p1.dat"
        p = pylibsort.fileDistribPart(pname)

        inBuf = fillPart(p, 20)

        outBuf = p.read()
        if outBuf != inBuf:
            raise testException("FileDistribPart", 
                    "Read returned wrong data. Expected {}, Got {}".format(inBuf.hex(), outBuf.hex()))
        
        partialOutBuf = p.read(start=4, nbyte=4)
        if partialOutBuf != inBuf[4:8]:
            raise testException("FileDistribPart",
                    "Partial read returned wrong data. Expected {}, Got {}".format(inBuf[4:9].hex(), partialOutBuf.hex()))

        appendBuf = fillPart(p, 20)
        outAppend = p.read()
        if outAppend != inBuf + appendBuf:
            raise testException("FileDistribPart",
                    "Appended partition returned wrong data. Expected {}, Got {}".format(
                        (inBuf + appendBuf).hex(), outAppend.hex()))


def checkParts(parts, inBufs, label):
    for i in range(len(parts)):
        outBuf = parts[i].read()
        if outBuf != inBufs[i]:
            raise testException(label[0],
                    label[1] + ": part{} read returned wrong data. Expected {}, Got {}".format(
                        i, inBufs[i].hex(), outBuf.hex()
                     )
                  )


def testFileDistribArray():
    nparts = 2
    partSz = 10

    with tempfile.TemporaryDirectory() as tDir:
        aDir = pathlib.Path(tDir) / "distribArrayTest"
        arr = pylibsort.fileDistribArray(aDir, npart=nparts)

        retNParts = arr.nParts()
        if retNParts != nparts:
            raise testException("FileDistribArray",
                "nParts gave wrong answer. Expected {}, Got {}".format(nparts,retNParts))

        parts = arr.getParts()
        if len(parts) != nparts:
            raise testException("FileDistribArray",
                label + ": Wrong number of parts. Expected {}, Got {}".format(nparts, len(parts)))

        inBufs = fillArr(arr, partSz) 

        checkParts(parts, inBufs, ("FileDistribArray", "initalArray,getParts"))

        parts = [ arr.getPart(i) for i in range(nparts) ]
        checkParts(parts, inBufs, ("FileDistribArray","initalArray,getPart"))

        arrExisting = pylibsort.fileDistribArray(aDir, exist_ok=True)
        parts = arrExisting.getParts()
        checkParts(parts, inBufs, ("FileDistribArray","ArrExisting"))
        

def checkPartRef(label, ref, expected):
    out = ref.read()
    if out != expected:
        raise testException(label[0], 
                "{}: Partref returned wrong data. Expected {}, Got {}".format(
                label[1], expected.hex(), out.hex()))


def testFilePartRef():
    nparts = 2
    partSz = 10

    with tempfile.TemporaryDirectory() as tDir:
        aDir = pathlib.Path(tDir) / "distribArrayTest"
        arr = pylibsort.fileDistribArray(aDir, npart=nparts)
        inBufs = fillArr(arr, partSz)

        ref = pylibsort.partRef(arr, partID=0, start=0, nbyte=5)
        checkPartRef(("FilePartRef", "part0"), ref, inBufs[0][:5])

        ref = pylibsort.partRef(arr, partID=1, start=2, nbyte=6)
        checkPartRef(("FilePartRef", "part1"), ref, inBufs[1][2:8])

def testPartRefReq():
    nparts = 2
    partSz = 10

    with tempfile.TemporaryDirectory() as tDir:
        aDir = pathlib.Path(tDir) / "distribArrayTest"
        arr = pylibsort.fileDistribArray(aDir, npart=nparts)
        inBufs = fillArr(arr, partSz)

        # offset and width aren't actually used by this test
        req = {
                "offset" : 0,
                "width" : 2,
                "arrType" : "file"
              }
        
        req['input'] = [
                {"arrayPath" : aDir,
                "partID" : 0,
                "start" : 0,
                "nbyte" : 5},

                {"arrayPath" : aDir,
                 "partID" : 1,
                 "start" : 2,
                 "nbyte" : 6
                }
            ]

        refs = pylibsort.getPartRefs(req)
        checkPartRef(("FilePartRef", "part0"), refs[0], inBufs[0][:5])
        checkPartRef(("FilePartRef", "part1"), refs[1], inBufs[1][2:8])


testFileDistribPart()
testFileDistribArray()
testFilePartRef()
testPartRefReq()

print("PASS")
