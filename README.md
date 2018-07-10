# Kafka Monitor

[![Build Status](https://travis-ci.org/linkedin/kafka-monitor.svg?branch=master)](https://travis-ci.org/linkedin/kafka-monitor)

Kafka Monitor is a framework to implement and execute long-running kafka
system tests in a real cluster. It complements Kafka’s existing system
tests by capturing potential bugs or regressions that are only likely to occur
after prolonged period of time or with low probability. Moreover, it allows you to monitor Kafka
cluster using end-to-end pipelines to obtain a number of derived vital stats
such as end-to-end latency, service availability and message loss rate. You can easily
deploy Kafka Monitor to test and monitor your Kafka cluster without requiring
any change to your application.

Kafka Monitor can automatically create the monitor topic with the specified config
and increase partition count of the monitor topic to ensure partition# >=
broker#. It can also reassign partition and trigger preferred leader election
to ensure that each broker acts as leader of at least one partition of the
monitor topic. This allows Kafka Monitor to detect performance issue on every
broker without requiring users to manually manage the partition assignment of
the monitor topic.

## Getting Started

### Prerequisites
Kafka Monitor requires Gradle 2.0 or higher. Java 7 should be used for
building in order to support both Java 7 and Java 8 at runtime.

Kafka Monitor supports Apache Kafka 0.8 to 0.11:
- Use branch 0.8.2.2 to work with Apache Kafka 0.8
- Use branch 0.9.0.1 to work with Apache Kafka 0.9
- Use branch 0.10.2.1 to work with Apache Kafka 0.10
- Use branch 0.11.x to work with Apache Kafka 0.11
- Use master branch to work with Apache Kafka 1.0


### Configuration Tips

- We advise advanced users to run Kafka Monitor with
`./bin/kafka-monitor-start.sh config/kafka-monitor.properties`. The default
kafka-monitor.properties in the repo provides an simple example of how to
monitor a single cluster. You probably need to change the value of
`zookeeper.connect` and `bootstrap.servers` to point to your cluster.

- The full list of configs and their documentation can be found in the code of
Config class for respective service, e.g. ProduceServiceConfig.java and
ConsumeServiceConfig.java.

- You can specify multiple SingleClusterMonitor in the kafka-monitor.properties to
monitor multiple Kafka clusters in one Kafka Monitor process. As another
advanced use-case, you can point ProduceService and ConsumeService to two
different Kafka clusters that are connected by MirrorMaker to monitor their
end-to-end latency.

- Kafka Monitor by default will automatically create the monitor topic based on
the e.g.  `topic-management.replicationFactor` and `topic-management.partitionsToBrokersRatio`
specified in the config. replicationFactor is 1 by default and you probably
want to change it to the same replication factor as used for your existing
topics. You can disable auto topic creation by setting `produce.topic.topicCreationEnabled` to false.

- Kafka Monitor can automatically increase partition count of the monitor topic
to ensure partition# >= broker#. It can also reassign partition and trigger
preferred leader election to ensure that each broker acts as leader of at least
one partition of the monitor topic. To use this feature, use either
EndToEndTest or TopicManagementService in the properties file.


### Build Kafka Monitor
```
$ git clone https://github.com/linkedin/kafka-monitor.git
$ cd kafka-monitor 
$ ./gradlew jar
```

### Start KafkaMonitor to run tests/services specified in the config file
```
$ ./bin/kafka-monitor-start.sh config/kafka-monitor.properties
```

### Run Kafka Monitor with arbitrary producer/consumer configuration (e.g. SASL enabled client)
Edit `config/kafka-monitor.properties` to specify custom configurations for producer in the key/value map `produce.producer.props` in
`config/kafka-monitor.properties`. Similarly specify configurations for
consumer as well. The documentation for producer and consumer in the key/value maps can be found in the Apache Kafka wiki.

```
$ ./bin/kafka-monitor-start.sh config/kafka-monitor.properties
```

### Run SingleClusterMonitor app to monitor kafka cluster

Metrics `produce-availability-avg` and `consume-availability-avg` demonstrate
whether messages can be properly produced to and consumed from this cluster.
See Service Overview wiki for how these metrics are derived.

```
$ ./bin/single-cluster-monitor.sh --topic test --broker-list localhost:9092 --zookeeper localhost:2181
```

### Run MultiClusterMonitor app to monitor a pipeline of Kafka clusters connected by MirrorMaker
Edit `config/multi-cluster-monitor.properties` to specify the right broker and
zookeeper url as suggested by the comment in the properties file

Metrics `produce-availability-avg` and `consume-availability-avg` demonstrate
whether messages can be properly produced to the source cluster and consumed
from the destination cluster. See config/multi-cluster-monitor.properties for
the full jmx path for these metrics.

```
$ ./bin/kafka-monitor-start.sh config/multi-cluster-monitor.properties
```

### Get metric values (e.g. service availability, message loss rate) in real-time as time series graphs
Open ```localhost:8000/index.html``` in your web browser

You can edit webapp/index.html to easily add new metrics to be displayed.

### Query metric value (e.g. produce availability and consume availability) via HTTP request
```
curl localhost:8778/jolokia/read/kmf.services:type=produce-service,name=*/produce-availability-avg

curl localhost:8778/jolokia/read/kmf.services:type=consume-service,name=*/consume-availability-avg
```

You can query other JMX metric value as well by substituting object-name and
attribute-name of the JMX metric in the query above.

### Run checkstyle on the java code
```
./gradlew checkstyleMain checkstyleTest
```

### Build IDE project
```
./gradlew idea
./gradlew eclipse
```

## Wiki

- [Motivation](https://github.com/linkedin/kafka-monitor/wiki/Motivation)
- [Design Overview](https://github.com/linkedin/kafka-monitor/wiki/Design-Overview)
- [Service Design](https://github.com/linkedin/kafka-monitor/wiki/Service-Design)
- [Future Work](https://github.com/linkedin/kafka-monitor/wiki/Future-Work)



