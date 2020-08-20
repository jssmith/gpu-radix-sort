#!/bin/bash
set -e

USAGE="./copyResult.sh name path/to/dest"

if [[ $# < 2 ]]; then
  echo "Too few arguments:"
  echo $USAGE
fi

NAME=$1
DEST=$2

cp faasbenchLocalDistrib_step0_worker0_output.csv $DEST/faasFirst_$NAME.csv
cp faasbenchLocalDistrib_step1_worker0_output.csv $DEST/faasMiddle_$NAME.csv
