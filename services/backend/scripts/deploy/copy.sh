#!/bin/bash

##
# Output files concatenated for args
##

cd $(dirname $0)

files=$(./files.sh)

rsync $files "$1@$2:$3" -v -R