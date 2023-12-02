#!/bin/bash

##
# Retrive files to be deployed
##

cd $(dirname $0)

env_file="../../.env"
file_path="../../deploy/deploy-files.conf"

# Read .env file
source $env_file

declare -a files=()

while read -r line; 
do
    [ "${line:0:1}" = "#" ] && continue
    files+=("../..//./$line")
done < $file_path

echo "${files[@]}"