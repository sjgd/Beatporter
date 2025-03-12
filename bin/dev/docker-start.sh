#!/bin/bash

if docker ps | grep "beatporter$"; then
    echo "Container already started. Do nothing"
elif docker ps -a | grep "beatporter$"; then
    docker start beatporter
else
    docker run -i \
        \
        -p 65000:65000 \
        --name beatporter \
        -t beatporter:latest

fi
