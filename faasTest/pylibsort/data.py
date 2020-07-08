import pathlib
import os
import re
import abc

FileDistribArrayMount = pathlib.Path("/shared")

def SetDistribMount(newRoot: pathlib.Path):
    """Change the default mount point for File distributed arrays to newRoot.
    It is not necessary to call this, the default is '/shared'"""
    global FileDistribArrayMount
    FileDistribArrayMount = newRoot


class DistribPart(abc.ABC):
    """A single partition of a distributed array"""
    @abc.abstractmethod
    def read(self, start=0, nbyte=-1):
        """Read nbyte bytes from the partition starting at byte 'start'"""
        pass


    @abc.abstractmethod
    def write(self, buf):
        """Append the contents of buf to the partition"""
        pass


class DistribArray(abc.ABC):
    """A generic distributed array. Distributed arrays supply partitioned data
    from a backend that may be remote."""

    @abc.abstractmethod
    def nParts(self):
        """Returns the number of partitions contained in this array"""
        pass


    @abc.abstractmethod
    def getParts(self):
        """Returns an iterable of the DistribParts contained in this array."""
        pass
    

    @abc.abstractmethod
    def getPart(self, idx):
        """Returns a single DistribPart from the array by index"""
        pass


class fileDistribPart(DistribPart):
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


class fileDistribArray(DistribArray):
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

class partRef():
    """Reference to a segment of a partition to read."""
    def __init__(self, arr: DistribArray, partID=0, start=0, nbyte=-1):
        self.arr = arr
        self.partID = partID
        self.start = start
        self.nbyte = nbyte 

    def read(self):
        part = self.arr.getPart(self.partID)
        return part.read(start=self.start, nbyte=self.nbyte)

def __filePartRefFromDict(req) -> partRef:
    """Return a partRef from an entry in the 'input' field of a req"""
    arr = fileDistribArray(FileDistribArrayMount / req['arrayName'], exist_ok=True)
    return partRef(arr, partID=req['partID'], start=req['start'], nbyte=req['nbyte'])

def getPartRefs(req: dict):
    """Returns a list of partRefs from a sort request dictionary."""

    if req['arrType'] == "file":
        return [__filePartRefFromDict(r) for r in req['input']]
    else:
        raise ValueError("Invalid request type: " + str(req['arrType']))
