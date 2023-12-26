#!/bin/bash

##
# Build docker production image with tag and push it to Docker Hub
##

cd $(dirname $0)

dockerfile="../../docker/Dockerfile.prod"
path="../.."
env_file="../../.env"

# Read .env file
source $env_file

echo "Build Docker Image"

read -p "Username: ($DOCKER_USER) " username

if [ -z "$username" ]
then
      username=$DOCKER_USER
fi

read -p "Image: ($DOCKER_IMAGE_NAME) " image_name

if [ -z "$image_name" ]
then
      image_name=$DOCKER_IMAGE_NAME
fi

read -p "Tag: (latest) " tag

if [ -z "$tag" ]
then
      tag="latest"
fi

read -p "Push image after build? (y/n) " push


complete_tag=$username/$image_name:$tag

# Run build command
docker build $path --platform="linux/amd64" -f $dockerfile -t $complete_tag --no-cache --build-arg AIRFLOW_HOME=$AIRFLOW_HOME_CONTAINER --build-arg AIRFLOW_PROJ_DIR=$AIRFLOW_PROJ_DIR


echo "Docker image build completed: $complete_tag"

# Run push command
if [ $push = "y" ]
then
    docker push $complete_tag
    echo "Image puhsed to Docker Hub"
fi