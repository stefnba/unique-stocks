#!/bin/bash

##
# Build docker production image with tag and push it to Docker Hub
##

cd $(dirname $0)

dockerfile="../../docker/Dockerfile.prod"
path="../.."
package_path="../../package.json"


echo "Build Docker Image"

npm_version=$(node -p -e "require('$package_path').version")
npm_name=$(node -p -e "require('$package_path').name")


read -p "Username: " username

if [ -z "$username" ]
then
      echo "Username mustn't be empty!"
      exit
fi

read -p "Image: ($npm_name) " image_name

if [ -z "$image_name" ]
then
      image_name=$npm_name
fi

read -p "Tag: ($npm_version) " tag

if [ -z "$tag" ]
then
      tag=$npm_version
fi

read -p "Push image after build? (y/n) " push


complete_tag=$username/$image_name:$tag

# Run build command
docker build $path -f $dockerfile -t $complete_tag


echo "Docker image build completed: $complete_tag"

# Run push command
if [ $push = "y" ]
then
    docker push $complete_tag
    echo "Image puhsed to Docker Hub"
fi
