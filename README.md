# cqrs-kafka-golang

## Start Kafka
- zookeeper

C:\kafka_2.13-2.4.0\bin\windows/zookeeper-server-start.bat C:\kafka_2.13-2.4.0\config/zookeeper.properties

- kafka

C:\kafka_2.13-2.4.0\bin\windows/kafka-server-start.bat C:\kafka_2.13-2.4.0\config/server.properties

## Create Topic

		createUserTopic   = "user-create"
		updateUserTopic   = "user-update"
		deleteUserTopic   = "user-delete"

C:\kafka_2.13-2.4.0\bin\windows/kafka-topics.bat –create –zookeeper kafka-zookeeper:2181 –replication-factor 1 –partitions 1 –topic user-create
C:\kafka_2.13-2.4.0\bin\windows/kafka-topics.bat –create –zookeeper kafka-zookeeper:2181 –replication-factor 1 –partitions 1 –topic user-update
C:\kafka_2.13-2.4.0\bin\windows/kafka-topics.bat –create –zookeeper kafka-zookeeper:2181 –replication-factor 1 –partitions 1 –topic user-delete