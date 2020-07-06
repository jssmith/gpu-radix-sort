import pathlib
import tempfile

import pylibsort

class testException(Exception):
    def __init__(self, tname, msg):
        self.tname = tname
        self.msg = msg

    def __str__(self):
        return('Test "{}" Failure: {}'.format(self.tname, self.msg))

def testFileDistribPart():
    with tempfile.TemporaryDirectory() as aDir:
        pname = pathlib.Path(aDir) / "p1.dat"
        p = pylibsort.fileDistribPart(pname)

        inBuf = bytes.fromhex("de ad be ef ca fe ba be ab ad da da 01 23 45 67 89 ab cd ef")
        
        p.write(inBuf)

        outBuf = p.read()
        if outBuf != inBuf:
            raise testException("FileDistribPart", 
                    "Read returned wrong data. Expected {}, Got {}".format(inBuf.hex(), outBuf.hex()))
        
        partialOutBuf = p.read(start=4, nbyte=4)
        if partialOutBuf != inBuf[4:8]:
            raise testException("FileDistribPart",
                    "Partial read returned wrong data. Expected {}, Got {}".format(inBuf[4:9].hex(), partialOutBuf.hex()))

        appendBuf = bytes.fromhex("42 17 af fa bd")
        p.write(appendBuf)
        outAppend = p.read()
        if outAppend != inBuf + appendBuf:
            raise testException("FileDistribPart",
                    "Appended partition returned wrong data. Expected {}, Got {}".format((inBuf + appendBuf).hex(), outAppend.hex()))


def checkParts(parts, inBuf, label):
    outBuf0 = parts[0].read()
    if outBuf0 != inBuf[:10]:
        raise testException("FileDistribArray",
                label + ": part0 read returned wrong data. Expected {}, Got {}".format(inBuf[:10].hex(), outBuf0.hex()))

    outBuf1 = parts[1].read()
    if outBuf1 != inBuf[10:]:
        raise testException("FileDistribArray",
            label + ": part1 read returned wrong data. Expected {}, Got {}".format(inBuf[10:].hex(), outBuf1.hex()))


def testFileDistribArray():
    inBuf = bytes.fromhex("de ad be ef ca fe ba be ba ad da db 01 23 45 67 89 ab cd ef")

    with tempfile.TemporaryDirectory() as tDir:
        aDir = pathlib.Path(tDir) / "distribArrayTest"
        arr = pylibsort.fileDistribArray(aDir, npart=2)

        nparts = arr.nParts()
        if nparts != 2:
            raise testException("FileDistribArray",
                "nParts gave wrong answer. Expected {}, Got {}".format(2,nparts))

        parts = arr.getParts()
        if len(parts) != 2:
            raise testException("FileDistribArray",
                label + ": Wrong number of parts. Expected {}, Got {}".format(2, len(parts)))

        parts[0].write(inBuf[:10])
        parts[1].write(inBuf[10:])

        checkParts(parts, inBuf, "initalArray,getParts")

        parts = [ arr.getPart(0), arr.getPart(1) ]
        checkParts(parts, inBuf, "initalArray,getPart")

        arrExisting = pylibsort.fileDistribArray(aDir, exist_ok=True)
        parts = arrExisting.getParts()
        checkParts(parts, inBuf, "ArrExisting")
        

testFileDistribPart()
testFileDistribArray()

print("PASS")
