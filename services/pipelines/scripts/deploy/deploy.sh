#!/bin/bash

##
# Manual deployment of relevant files to remote server
##

cd $(dirname $0)

env_file="../../.env"
copy_script="./copy.sh"

# Read .env file
source $env_file

$copy_script $SSH_USER $SSH_HOST $SSH_DIR