#!/bin/bash
KAFKA_BROKERS_VAL="wellcare-cluster-kafka-brokers.dev-app-sup-services:9092"
ZOOKEEPER_BROKERS_VAL="wellcare-cluster-zookeeper-client.dev-app-sup-services:2181"
sed -e 's/%KAFKA_BROKERS%/$KAFKA_BROKERS_VAL/g' -e 's/%ZOOKEEPER_BROKERS%/$ZOOKEEPER_BROKERS_VAL/g' config/xinfra-monitor.properties > config/xinfra-monitor.properties
exec ./bin/xinfra-monitor-start.sh config/xinfra-monitor.properties  &
python -m http.server 8000