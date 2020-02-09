# cqrs-kafka-golang

## Overview
![CQRS](Cqrs.jpeg)

## Start Kafka
- zookeeper

C:\kafka_2.13-2.4.0\bin\windows/zookeeper-server-start.bat C:\kafka_2.13-2.4.0\config/zookeeper.properties

- kafka

C:\kafka_2.13-2.4.0\bin\windows/kafka-server-start.bat C:\kafka_2.13-2.4.0\config/server.properties

## Create Topic
From user.go:
- createUserTopic   = "user-create"
- updateUserTopic   = "user-update"
- deleteUserTopic   = "user-delete"

C:\kafka_2.13-2.4.0\bin\windows/kafka-topics.bat –create –zookeeper kafka-zookeeper:2181 –replication-factor 1 –partitions 1 –topic user-create
C:\kafka_2.13-2.4.0\bin\windows/kafka-topics.bat –create –zookeeper kafka-zookeeper:2181 –replication-factor 1 –partitions 1 –topic user-update
C:\kafka_2.13-2.4.0\bin\windows/kafka-topics.bat –create –zookeeper kafka-zookeeper:2181 –replication-factor 1 –partitions 1 –topic user-delete

## To see your messages

C:\kafka_2.13-2.4.0\bin\windows/kafka-console-consumer.bat -bootstrap-server kafka-zookeeper:9092 -topic user-create -from-beginning

## Run Redis

docker run -d -p 6379:6379 redis

## Run Postgresql
docker run -d -p 5432:5432 --name postgresql -e POSTGRES_PASSWORD=password postgres

docker exec -it postgresql /bin/sh

psql -h localhost -U postgres

CREATE DATABASE cqrs;

\connect cqrs

CREATE TABLE cqrs_user(
   userid integer PRIMARY KEY,
   firstname VARCHAR (50) NOT NULL,
   lastname VARCHAR (50) NOT NULL,
   age integer NOT NULL
);

INSERT INTO cqrs_user VALUES(0, 'jonny','love',33);
INSERT INTO cqrs_user VALUES(1, 'john','doe',33);

SELECT * FROM cqrs_user

## Test
http://localhost:8080/api/user/create/12/firstname/myfirst/lastname/mylast/age/32/
http://localhost:8080/api/user/read/0/
http://localhost:8080/api/user/update/12/firstname/myfirst/lastname/mylast/age/32/
http://localhost:8080/api/user/delete/12/