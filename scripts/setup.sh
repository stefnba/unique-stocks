#!/bin/bash

cwd=$(pwd)

network_name=unique-stocks-network
docker network inspect $network_name >/dev/null 2>&1 || \
		docker network create --driver bridge $network_name && echo "✅ Network $network_name created"

# Database service
cd ./services/database
echo "Spinning up database service ..."
docker compose up -d
make migrate-up
echo "✅ Database running and migrations completed"

# Pipelines service
cd $cwd
cd ./services/pipelines
echo "Spinning up pipelines service ..."
make docker-up
echo "✅ Pipelines service running"