# Function-as-a-Service Sorter
This is a FaaS interface to libsort for performing distributed radix sort.

## Arguments
Requests take the form of lists of partRefs to process and an output key to use
in an output DistributedArray. Sorters will output buckets as partitions of
this array. The handlers expect JSON-encoded arguments.

### Common Fields
  - "offset" - The starting bit index to start sorting
  - "width" - The number of radix bits to process
  - "arrType" - The type of distributed array used for exchanging data.
  - "input" - A list of JSON-encoded partRefs. The exact format of these arguments depends on "arrType" (see below).
  - "output" - An identifier to use for storing output. The meaning of this fields depends on "arrType" (see below).

### File Distributed Array
A file distributed array uses the filesystem to exchange data. The system must
ensure that the filesystem is shared between the requestor and the worker
(pylibsort does not handle mounting).

arrType:
"file"

input:
Each element of input is a partRef with the following fields:
  - "arrayName" - Directory name for this FileDistributedArray. The search path
      for this array depends on how the FaaS system was configured, but is
      assumed to be shared between the host and the FaaS executor.
  - "partID" - The numeric ID of the partition, this will be converted into "arrayPath/p${partID}.dat"
  - "start" - The byte index to start reading the partition from.
  - "nbyte" - The number of bytes to read. May be -1 to read the remainder of the partition (from start)

output:
This worker will store its outputs in the filesystem at the path indicated by
this field. As in partRefs, the exact interpretation of the path depends on
system configuration.

> Obsolete
> # Function-as-a-Service based test
> This test uses raw FaaS to perform the sort. It requires a GPU-enabled FaaS implementation.
>
> ## Event Args
> This function accepts two types of events: generate and provided.
>
> ### Generate
> For the generate type event, the faas handler will generate a random array of the given size to sort. The arguments are as follows:
>
>     {
>         "requestType" : "generate",
>         "test-size" : SIZE_IN_BYTES
>     }
>  
> where SIZE_IN_BYTES is an integer representing how large of an array (in bytes) to generate and sort.
>
> ### Provided
> For the 'provided' type event, the user explicitly provides an array to sort inline in the request. The provided array should be a string of base64 encoded bytes.
>
>     {
>         "requestType" : "provided",
>         "data" : "BYTES_IN_BASE64"
>     }
>
> Where BYTES_IN_BASE64 is a string of base64 encoded bytes, e.g.: AAAAB8QgAAXUMAAH1DAAB7QwAA.
>
> ## Data Plane
> The test currently uses an ASCII binary encoding of the input and output data. As more realistic dataplane's become available, we will use that.
