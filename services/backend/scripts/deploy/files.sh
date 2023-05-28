#!/bin/bash

##
# Output files concatenated for args
##

cd $(dirname $0)

file_path="../../deploy/files.conf"
files=()

while read -r line; 
do
    files+=" ../..//./${line//\"/}"
done < $file_path

echo $files