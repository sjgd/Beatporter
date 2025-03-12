#!/bin/bash

docker exec -i -t beatporter $(echo "${@:1}")
