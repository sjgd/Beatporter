#!/bin/bash

: "${PROJECT_NAME:=beatporter}"

CONTAINER_NAME="${PROJECT_NAME}"

if docker ps -a -q --filter="ancestor=${PROJECT_NAME}"; then
    # With ancestor will remove both
    docker rm $(docker ps -a -q --filter="ancestor=${PROJECT_NAME}") --force
else
    echo "Container not running."
fi

CONTAINER_NAME=beatporter

if docker ps -a | grep "${CONTAINER_NAME}" | awk '{ print $1 }'; then
    # With ancestor will remove both
    docker rm $(docker ps -a | grep "${CONTAINER_NAME}" | awk '{ print $1 }') --force
else
    echo "Container not running."
fi
