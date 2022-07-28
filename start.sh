#!/bin/bash

# Build the base images
# then run docker-compose to startup images
sbt package
cp ./target/scala-2.12/akka-websockets-spark-cassandra_2.12-0.1.jar ./apps/
docker build -f .\spark\Dockerfile -t bitnamispark/withcryptocompare:1.0 .
docker-compose -f ./kafka-pinot/docker-compose.yml up -d
docker-compose -f ./spark/docker-compose.yml up -d

docker exec -e STREAM_TIMEOUT=20 -e CRYPTOCOMPARE_API_KEY=fbc1f339b1bbcb357cfe9e65a28b97e9dd574036c42a0ccbcd955c5578acdef5 -it spark-worker-1 spark-submit --master spark://spark-master:7077 akka-websockets-spark-cassandra_2.12-0.1.jar
