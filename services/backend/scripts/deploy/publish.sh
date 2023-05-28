#!/bin/bash

##
# Start up the new production containers on the remote server
# Executed on remote server
##

cd $(dirname $0)
cd ../..

# stop containers
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml -f docker/docker-compose.deploy.yml --project-directory . down

# pull latest image
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml -f docker/docker-compose.deploy.yml --project-directory . pull

# migrate db
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml -f docker/docker-compose.deploy.yml --project-directory . run --rm -it --entrypoint "npm run migrate up" app

# restart containers
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml -f docker/docker-compose.deploy.yml --project-directory . up -d --build --remove-orphans

docker ps