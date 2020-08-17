export RADIXBENCH_ROOTPATH=$( realpath $(dirname ${BASH_SOURCE[0]}) )

# Normally this wouldn't be necessary but python's ctypes.util.find_library
# breaks if there is a leading colon (e.g. ":/path/to/lib") which happens if
# LD_LIBRARY_PATH isn't already set.
if [[ -z $LD_LIBRARY_PATH ]]; then
  export LD_LIBRARY_PATH=$(realpath ./libsort)
else
  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(realpath ./libsort)
fi
