#!/bin/bash

docker-compose -f .\spark\docker-compose.yml down
docker-compose -f .\kafka-pinot\docker-compose.yml down
