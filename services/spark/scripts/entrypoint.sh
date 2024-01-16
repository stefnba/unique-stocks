#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
    # start ssh server
    echo "Starting SSH..."
    /usr/sbin/sshd 
    # set environment variables
    echo "Setting environment variables..."
    (env | sed 's/^/export /') > /root/environment_variables.sh
    # start spark master
    echo "Starting Spark Master..."
    start-master.sh -p $SPARK_MASTER_PORT
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  start-worker.sh spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT
elif [ "$SPARK_WORKLOAD" == "history" ]
then
  start-history-server.sh
fi