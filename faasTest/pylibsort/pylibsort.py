import pathlib
import os
import re

class fileDistribPart():
    """A single partition of a file-based distributed array."""

    def __init__(self, pPath):
        self.pPath = pPath


    def read(self, start=0, nbyte=-1):
        with open(self.pPath, 'rb') as pf:
            pf.seek(start)
            b = pf.read(nbyte)
        return b
    

    def write(self, buf):
        with open(self.pPath, 'ab') as bf:
            bf.write(buf)


class fileDistribArray():
    """A distributed array that stores its data in the filesystem. If the
    provided path already exists, it is used directly, otherwise a directory is
    created for the new array. If the array already exists, the npart argument
    is ignored."""

    def __init__(self, rootPath, npart=1, exist_ok=False):
        self.rootPath = rootPath
        self.npart = npart
        
        if self.rootPath.exists():
            if not exist_ok:
                raise FileExistsError(rootPath)

            self.partPaths = list(self.rootPath.iterdir())

            pat = re.compile("p(.*)\.dat")
            def partPathKey(p):
                m = pat.match(str(p.name))
                return int(m.group(1))

            self.partPaths.sort(key=partPathKey)

        if not self.rootPath.exists():
            self.rootPath.mkdir(0o700)

            self.partPaths = [ self.rootPath / ("p" + str(partID) + ".dat") for partID in range(self.npart) ] 
            for ppath in self.partPaths:
                ppath.touch(mode=0o600)
    

    def nParts(self):
        return self.npart


    def getParts(self):
        return [ fileDistribPart(path) for path in self.partPaths ]
    

    def getPart(self, idx):
        return fileDistribPart(self.partPaths[idx])
