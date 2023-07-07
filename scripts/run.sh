#!/bin/bash

cwd=$(pwd)

# Start docker network
network_name=unique-stocks-network
docker network inspect $network_name >/dev/null 2>&1 || \
		docker network create --driver bridge $network_name && echo "✅ Network $network_name created"

# Database service
cd ./services/database
echo "Spinning up database service ..."
make docker-up
make migrate-up
echo "✅ Database running and migrations completed"

# Pipelines service
cd $cwd
cd ./services/pipelines
echo "Spinning up pipelines service ..."
make docker-up
echo "✅ Pipelines service running"


# Backend service
cd $cwd
cd ./services/backend
npm run docker:dev
echo "✅ Backend service running"


# Log service
cd $cwd
cd ./services/log
npm run docker:dev
echo "✅ Log service running"


# Frontend service
cd $cwd
cd ./services/frontend
npm run docker:dev
echo "✅ Frontend service running"