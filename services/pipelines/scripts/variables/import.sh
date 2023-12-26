#!/bin/bash

##
# Import Airflow variables
##


file_container=/tmp/variables.yml
file_host=deploy/variables.env.yml
container_name=uniquestocks-airflow-webserver

# copy file to container
docker exec -i $container_name sh -c "cat > $file_container" < $file_host

# import variables
docker exec -i $container_name sh -c "airflow variables import $file_container"

# remove file from container
docker exec -it $container_name sh -c "rm -f $file_container"

