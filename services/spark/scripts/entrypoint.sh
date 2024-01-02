#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
    /usr/sbin/sshd # start ssh server
    echo "Starting Spark Master..."
    start-master.sh -p $SPARK_MASTER_PORT
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  start-worker.sh spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT
elif [ "$SPARK_WORKLOAD" == "history" ]
then
  start-history-server.sh
fi