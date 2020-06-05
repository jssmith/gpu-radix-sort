# Function-as-a-Service based test
This test uses raw FaaS to perform the sort. It requires a GPU-enabled FaaS implementation.

## Event Args
This function accepts two types of events: generate and provided.

### Generate
For the generate type event, the faas handler will generate a random array of the given size to sort. The arguments are as follows:

    {
        "requestType" : "generate",
        "test-size" : SIZE_IN_BYTES
    }
 
where SIZE_IN_BYTES is an integer representing how large of an array (in bytes) to generate and sort.

### Provided
For the 'provided' type event, the user explicitly provides an array to sort inline in the request. The provided array should be a string of base64 encoded bytes.

    {
        "requestType" : "provided",
        "data" : "BYTES_IN_BASE64"
    }

Where BYTES_IN_BASE64 is a string of base64 encoded bytes, e.g.: AAAAB8QgAAXUMAAH1DAAB7QwAA.

## Data Plane
The test currently uses an ASCII binary encoding of the input and output data. As more realistic dataplane's become available, we will use that.
