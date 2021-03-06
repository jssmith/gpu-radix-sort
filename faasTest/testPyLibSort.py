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


def fillArr(arr, szs=None):
    """Fill arr with random bytes. You may optionally provide a list of lengths
    to partially fill the array."""
    npart = arr.shape.npart
    inBufs = []
    
    if szs is None:
        szs = arr.shape.caps

    for partId in range(npart):
        partBuf = bytes([random.getrandbits(8) for _ in range(szs[partId])])
        arr.WritePart(partId, partBuf)
        inBufs.append(partBuf)

    return inBufs


def testFileDistribPart():
    shape = pylibsort.ArrayShape.fromUniform(40, 2)

    with tempfile.TemporaryDirectory() as tDir:
        tDir = pathlib.Path(tDir)
        arr = pylibsort.fileDistribArray.Create(tDir / "partTest0", shape=shape)

        inBufs = fillArr(arr, szs=[20, 20])
        inBuf = inBufs[0]

        # Full Read
        outBuf = arr.ReadPart(0)
        if outBuf != inBuf:
            raise testException("FileDistribPart", 
                    "Read returned wrong data. Expected {}, Got {}".format(inBuf.hex(), outBuf.hex()))
        
        # Partial Read
        partialOutBuf = arr.ReadPart(0, start=4, nbyte=4)
        if partialOutBuf != inBuf[4:8]:
            raise testException("FileDistribPart",
                    "Partial read returned wrong data. Expected {}, Got {}".format(inBuf[4:9].hex(), partialOutBuf.hex()))


        # Append
        appendBufs = fillArr(arr, szs=[20, 20])
        appendBuf = appendBufs[0]
        outAppend = arr.ReadPart(0)
        if outAppend != inBuf + appendBuf:
            raise testException("FileDistribPart",
                    "Appended partition returned wrong data. Expected {}, Got {}".format(
                        (inBuf + appendBuf).hex(), outAppend.hex()))

        # ReadAll
        inFull = inBufs[0] + appendBufs[0] + inBufs[1] + appendBufs[1]
        outFull = arr.ReadAll()
        if outFull != inFull:
            raise testException("FileDistribPart",
                    "ReadAll returned wrong data. Expected {}, Got {}".format(
                        (fullBuf).hex(), outFull.hex()))

        # WriteAll
        newArr = pylibsort.fileDistribArray.Create(tDir / "partTest1", shape)
        newArr.WriteAll(outFull)
        newOutFull = newArr.ReadAll()
        if newOutFull != outFull:
            raise testException("FileDistribPart",
                    "WriteAll returned wrong data. Expected {}, Got {}".format(
                        (outFull).hex(), newOutFull.hex()))


def checkArr(arr, shape, inBufs, label):

    if arr.shape.caps != shape.caps:
        raise testException(label[0], label[1] + ": capacities don't match expected {}, got {}".format(shape.caps, arr.shape.caps))

    for partId in range(arr.shape.npart):
        outBuf = arr.ReadPart(partId)
        if outBuf != inBufs[partId]:
            raise testException(label[0],
                    label[1] + ": part{} read returned wrong data. Expected {}, Got {}".format(
                        i, inBufs[partId].hex(), outBuf.hex()
                     )
                  )


def testFileDistribArray():
    nparts = 2
    partSz = 10
    shape = pylibsort.ArrayShape.fromUniform(partSz, nparts)

    with tempfile.TemporaryDirectory() as tDir:
        aDir = pathlib.Path(tDir) / "distribArrayTest"
        arr = pylibsort.fileDistribArray.Create(aDir, shape)

        retNParts = arr.shape.npart
        if retNParts != nparts:
            raise testException("FileDistribArray",
                "nParts gave wrong answer. Expected {}, Got {}".format(nparts,retNParts))

        inBufs = fillArr(arr) 

        checkArr(arr, shape, inBufs, ("FileDistribArray", "initalArray"))

        arr.Close()

        arrExisting = pylibsort.fileDistribArray.Open(aDir)
        checkArr(arrExisting, shape, inBufs, ("FileDistribArray","ArrExisting"))

        arrExisting.Destroy()
        successfullyFailed = False
        try:
            arrExisting = pylibsort.fileDistribArray.Open(aDir)
        except pylibsort.DistribArrayError as e:
            successfullyFailed = True

        if not successfullyFailed:
            raise testException("FileDistribArray", "Opened a destroyed array (should have failed)")


def checkPartRef(label, ref, expected):
    out = ref.read()
    if out != expected:
        raise testException(label[0], 
                "{}: Partref returned wrong data. Expected {}, Got {}".format(
                label[1], expected.hex(), out.hex()))


def testFilePartRef():
    nparts = 2
    partSz = 10
    shape = pylibsort.ArrayShape.fromUniform(partSz, nparts)

    with tempfile.TemporaryDirectory() as tDir:
        aDir = pathlib.Path(tDir) / "distribArrayTest"
        arr = pylibsort.fileDistribArray.Create(aDir, shape)
        inBufs = fillArr(arr)

        ref = pylibsort.partRef(arr, partID=0, start=0, nbyte=5)
        checkPartRef(("FilePartRef", "part0"), ref, inBufs[0][:5])

        ref = pylibsort.partRef(arr, partID=1, start=2, nbyte=6)
        checkPartRef(("FilePartRef", "part1"), ref, inBufs[1][2:8])

def testPartRefReq():
    nparts = 2
    partSz = 10
    shape = pylibsort.ArrayShape.fromUniform(partSz, nparts)

    with tempfile.TemporaryDirectory() as tDir:
        pylibsort.SetDistribMount(pathlib.Path(tDir))

        aDir = pathlib.Path(tDir) / "distribArrayTest"
        arr = pylibsort.fileDistribArray.Create(aDir, shape)
        inBufs = fillArr(arr)
        arr.Close()

        # offset and width aren't actually used by this test
        req = {
                "offset" : 0,
                "width" : 2,
                "arrType" : "file"
              }
        
        req['input'] = [
                {"arrayName" : aDir.name,
                "partID" : 0,
                "start" : 0,
                "nbyte" : 5},

                {"arrayName" : aDir.name,
                 "partID" : 1,
                 "start" : 2,
                 "nbyte" : 6
                }
            ]

        refs = pylibsort.getPartRefs(req)
        checkPartRef(("FilePartRef", "part0"), refs[0], inBufs[0][:5])
        checkPartRef(("FilePartRef", "part1"), refs[1], inBufs[1][2:8])


def testSortFull():
    nbyte = 1021
    inBuf = bytearray([random.getrandbits(8) for _ in range(nbyte)])
    inInts = pylibsort.bytesToInts(inBuf)

    try:
        pylibsort.sortFull(inBuf)
    except Exception as e:
        raise testException("SortFull", "PyLib sort error: " + str(e))

    respInts = pylibsort.bytesToInts(inBuf)

    try:
        pylibsort.checkSortFull(respInts, inInts)
    except Exception as e:
        raise testException("SortFull", str(e))


def testSortPartial():
    nElem = 1021
    nbyte = nElem*4
    pos = 4
    width = 8
    refBuf = bytearray([random.getrandbits(8) for _ in range(nbyte)])
    # refInts = pylibsort.bytesToInts(inBuf)

    testBuf = refBuf.copy()
    try:
        boundaries = pylibsort.sortPartial(testBuf, pos, width)
    except Exception as e:
        raise testException("SortPartial", "PyLib sort error") from e

    try:
        pylibsort.checkPartial(testBuf, refBuf, boundaries, pos, width)
    except Exception as e:
        raise testException("SortFromBytes", str(e))


try:
    testFileDistribArray()
    testFileDistribPart()
    testFilePartRef()
    testPartRefReq()
    testSortFull()
    testSortPartial()
except testException as e:
    print("TEST FAILURE")
    print(e)
    exit(1)

print("PASS")
