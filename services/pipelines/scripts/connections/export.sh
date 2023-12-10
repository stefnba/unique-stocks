#!/bin/bash

##
# Export Airflow connections to yaml file
##

sink_file=/tmp/connections.yml
sink_file_host=deploy/conn.env.yml
container_name=uniquestocks-airflow-webserver

docker exec -it $container_name sh -c "airflow connections export $sink_file --file-format yaml"
docker cp $container_name:/$sink_file $sink_file_host
docker exec -it $container_name sh -c "rm $sink_file"