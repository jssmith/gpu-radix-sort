#!/bin/bash

echo $SRK_DIR
if [[ ! -d $SRK_DIR ]]; then
  echo "Please define the SRK_DIR environment variable"
  exit 1
fi

ARG=$(cat exampleArg.json | tr -d '\n')

# This is needed because srk currently relies on PWD to function (this will get
# fixed eventually)
pushd $SRK_DIR
srk bench --benchmark one-shot --function-args "$ARG" --function-name pyplover 
popd
