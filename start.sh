#!/bin/bash
# Build the application first and copy jar to location.
sbt package
mkdir apps
cp ./target/scala-2.12/akka-websockets-spark-cassandra_2.12-0.1.jar ./apps/

# Next, build the base images
# then run docker-compose to startup containers
docker build -f ./spark/Dockerfile -t bitnamispark/withcryptocompare:1.0 .
docker-compose -f ./kafka-pinot/docker-compose.yml up -d
docker-compose -f ./spark/docker-compose.yml up -d

docker exec -e STREAM_TIMEOUT=20 -e CRYPTOCOMPARE_API_KEY= -it spark-worker-1 spark-submit --master spark://spark-master:7077 akka-websockets-spark-cassandra_2.12-0.1.jar
