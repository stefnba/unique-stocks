#!/bin/bash

##
# Import Airflow connections
##


file_container=/tmp/connections.yml
file_host=deploy/conn.env.yml
container_name=uniquestocks-airflow-webserver

# copy file to container
docker exec -i $container_name sh -c "cat > $file_container" < $file_host

# import connections
docker exec -i $container_name sh -c "airflow connections import $file_container"

# remove file from container
docker exec -it $container_name sh -c "rm -f $file_container"

