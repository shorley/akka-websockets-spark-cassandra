# Spark Streaming CryptoCompare Project
This project demonstrates the use of using Spark Structured streaming available on Datastax Cassandra platform 6.8.15 to consume crypto trade information made available from wss://streamer.cryptocompare.com/v2 through Websockets consumed using Akka.

The project can be ran in 2 modes: memory, where the data streamed is consumed and aggregrations are displayed on console, or by using Kafka as a messaging bus. In the Kafka mode, data consumed from the websockets is first sent to the Kafka broker and Spark is used to consume the data, process it, and the results are sent to a Cassandra database.


## Prerequisites
- add prereqs such as sbt
- etc

## Setup

1. Create a free account on CryptoCompare by going to the site below.
``` 
https://min-api.cryptocompare.com/ 
```
 
2. Follow the instructions to setup your API_KEY. This API_KEY is what will be used to stream live messages from the websocket to our Spark application.

3. Build this project by running
```
sbt clean package
```

4. Next, transfer the jar file to your spark cluster environment.


## Running the App: Memory Mode
1. To run the application in memory mode, copy and paste the command as shown below to submit your application. Be sure to add your API_KEY into the designated placeholder.

```bash
dse spark-submit --packages "com.typesafe.akka:akka-stream_2.11:2.5.32,com.typesafe.akka:akka-actor_2.11:2.5.32,\
com.typesafe.akka:akka-http_2.11:10.1.15,com.typesafe.akka:akka-http-jackson_2.11:10.1.15,com.typesafe.akka:akka-http-spray-json_2.11:10.1.15"\
--master 'local[*]' --conf spark.sql.shuffle.partitions=8\
akka-websockets-spark-cassandra_2.11-0.1.jar --mode memory --timeout 150 <API_KEY>
```
This should start your streaming application and you should see results on your console.

![img.png](img.png)

## Running the App: Kafka Mode
To run in kafka mode, you will need to perform some additional steps.

1. Create your kafka broker or run docker-compose up on docker-compose.yml file already provided.
```
from your terminal window, switch to the src folder
run: docker-compose up
```
2. Create the necessary kafka topic and take note of it.
```
If running kafka on docker,
>> docker exec -it kafka /bin/bash 
>> cd opt/bitnami/kafka/bin/
>> ./kafka-topics.sh --create --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092 --topic cryptocompare
```
3. Log into CQLSH and create a new keyspace
```
create keyspace if not exists cryptocompare with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
```
4. Create the needed table: 
```bash
CREATE TABLE cryptocompare.trademsgs1minutewindow (
	date timestamp,
	window_start timestamp,
	window_end timestamp,
	market text,
	direction text,
	fromcoin text,
	tocurrency text,
	avgprice double,
	totalquantity double,
	totalvol double,
	counttxns bigint,
	uuid timeuuid,
	PRIMARY KEY ((date), window_end, window_start, direction, market)
) WITH CLUSTERING ORDER BY (window_end DESC, window_start DESC, direction ASC, market ASC);
```
5. Finally, Submit your application to the cluster by entering the command below. Be sure to update your Kafka Broker, Kafka Topic, Cassandra Hostname, and API_KEY in the designated spots.
```bash
dse spark-submit --packages "com.typesafe.akka:akka-stream_2.11:2.5.32,com.typesafe.akka:akka-actor_2.11:2.5.32,\
com.typesafe.akka:akka-http_2.11:10.1.15,com.typesafe.akka:akka-http-jackson_2.11:10.1.15,com.typesafe.akka:akka-http-spray-json_2.11:10.1.15,\
org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,\
org.apache.kafka:kafka_2.11:2.2.1,org.apache.kafka:kafka-streams:2.2.1"\
--master 'local[*]' --conf spark.sql.shuffle.partitions=8\
akka-websockets-spark-cassandra_2.11-0.1.jar --mode kafka --timeout 300\
--kafkabroker <KAFKA_BROKER>:9092 --kafkatopic <KAFKA_TOPIC> --cassandraurl <Cassandra_Host>\
<API_KEY>
```
6. Log into CQLSH to view data streamed in real time.
```
select * from cryptocompare.trademsgs1minutewindow where date = '2021-01-08 23:00:00+0000';
```
![img_1.png](img_1.png)

### _Improvements are welcomed! Please feel free to submit a pull request!_


