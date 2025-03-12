#!/bin/bash

if docker ps | grep "beatporter$"; then
    docker stop beatporter
    docker rm beatporter --force
elif docker ps -a | grep "beatporter$"; then
    docker rm beatporter --force
else
    echo "Container does not exist"
fi

if docker image ls -a | grep "beatporter"; then
    docker image rm beatporter --force
fi
