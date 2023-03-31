<p align="center">
  <img src="/docs/images/xinfra_monitor.png" width="510"/>
</p>

# Xinfra Monitor
[![Build Status](https://travis-ci.org/linkedin/kafka-monitor.svg?branch=master)](https://travis-ci.org/linkedin/kafka-monitor)
![Greetings](https://github.com/linkedin/kafka-monitor/workflows/Greetings/badge.svg)
![Mark stale issues and pull requests](https://github.com/linkedin/kafka-monitor/workflows/Mark%20stale%20issues%20and%20pull%20requests/badge.svg)
![Pull Request Labeler](https://github.com/linkedin/kafka-monitor/workflows/Pull%20Request%20Labeler/badge.svg)

Xinfra Monitor (formerly Kafka Monitor) is a framework to implement and execute long-running kafka
system tests in a real cluster. It complements Kafka’s existing system
tests by capturing potential bugs or regressions that are only likely to occur
after prolonged period of time or with low probability. Moreover, it allows you to monitor Kafka
cluster using end-to-end pipelines to obtain a number of derived vital stats
such as

<ol>
 <li> 
  End-to-end latency
 </li>
  <li> 
  Service availability
</li>
  <li> 
  Produce and Consume availability
    </li>
  <li> 
  Consumer offset commit availability
    </li>
  <li> 
  Consumer offset commit latency
    </li>
  <li> 
  Kafka message loss rate
    </li>
  <li> 
  And many, many more.
  </li>
  </ol>
  
You can easily
deploy Xinfra Monitor to test and monitor your Kafka cluster without requiring
any change to your application.

Xinfra Monitor can automatically create the monitor topic with the specified config
and increase partition count of the monitor topic to ensure partition# >=
broker#. It can also reassign partition and trigger preferred leader election
to ensure that each broker acts as leader of at least one partition of the
monitor topic. This allows Xinfra Monitor to detect performance issue on every
broker without requiring users to manually manage the partition assignment of
the monitor topic.

Xinfra Monitor is used in conjunction with different middle-layer services such as li-apache-kafka-clients in order to monitor single clusters, pipeline desination clusters, and other types of clusters as done in Linkedin engineering for real-time cluster healthchecks.

These are some of the metrics emitted from a Xinfra Monitor instance.

```
kmf:type=kafka-monitor:offline-runnable-count
kmf.services:type=produce-service,name=*:produce-availability-avg
kmf.services:type=consume-service,name=*:consume-availability-avg
kmf.services:type=produce-service,name=*:records-produced-total
kmf.services:type=consume-service,name=*:records-consumed-total
kmf.services:type=produce-service,name=*:records-produced-rate
kmf.services:type=produce-service,name=*:produce-error-rate
kmf.services:type=consume-service,name=*:consume-error-rate
kmf.services:type=consume-service,name=*:records-lost-total
kmf.services:type=consume-service,name=*:records-lost-rate
kmf.services:type=consume-service,name=*:records-duplicated-total
kmf.services:type=consume-service,name=*:records-delay-ms-avg
kmf.services:type=commit-availability-service,name=*:offsets-committed-avg
kmf.services:type=commit-availability-service,name=*:offsets-committed-total
kmf.services:type=commit-availability-service,name=*:failed-commit-offsets-avg
kmf.services:type=commit-availability-service,name=*:failed-commit-offsets-total
kmf.services:type=commit-latency-service,name=*:commit-offset-latency-ms-avg
kmf.services:type=commit-latency-service,name=*:commit-offset-latency-ms-max
kmf.services:type=commit-latency-service,name=*:commit-offset-latency-ms-99th
kmf.services:type=commit-latency-service,name=*:commit-offset-latency-ms-999th
kmf.services:type=commit-latency-service,name=*:commit-offset-latency-ms-9999th
```

## Getting Started

### Prerequisites
Xinfra Monitor requires Gradle 2.0 or higher. Java 7 should be used for
building in order to support both Java 7 and Java 8 at runtime.

Xinfra Monitor supports Apache Kafka 0.8 to 2.0:
- Use branch 0.8.2.2 to work with Apache Kafka 0.8
- Use branch 0.9.0.1 to work with Apache Kafka 0.9
- Use branch 0.10.2.1 to work with Apache Kafka 0.10
- Use branch 0.11.x to work with Apache Kafka 0.11
- Use branch 1.0.x to work with Apache Kafka 1.0
- Use branch 1.1.x to work with Apache Kafka 1.1
- Use master branch to work with Apache Kafka 2.0


### Configuration Tips

<ol>
<li> We advise advanced users to run Xinfra Monitor with
<code>./bin/xinfra-monitor-start.sh config/xinfra-monitor.properties</code>. The default
xinfra-monitor.properties in the repo provides an simple example of how to
monitor a single cluster. You probably need to change the value of
<code>bootstrap.servers</code> to point to your cluster.
  </li>
  <br />
<li> The full list of configs and their documentation can be found in the code of
Config class for respective service, e.g. ProduceServiceConfig.java and
ConsumeServiceConfig.java.</li>
<br />
<li> You can specify multiple SingleClusterMonitor in the xinfra-monitor.properties to
monitor multiple Kafka clusters in one Xinfra Monitor process. As another
advanced use-case, you can point ProduceService and ConsumeService to two different Kafka clusters that are connected by MirrorMaker to monitor their end-to-end latency.</li>
<br />  
<li> Xinfra Monitor by default will automatically create the monitor topic based on
the e.g.  <code>topic-management.replicationFactor</code> and <code>topic-management.partitionsToBrokersRatio</code>
specified in the config. replicationFactor is 1 by default and you probably
want to change it to the same replication factor as used for your existing
topics. You can disable auto topic creation by setting <code>produce.topic.topicCreationEnabled</code> to false.
</li>
<br />
<li> Xinfra Monitor can automatically increase partition count of the monitor topic
to ensure partition# >= broker#. It can also reassign partition and trigger
preferred leader election to ensure that each broker acts as leader of at least
one partition of the monitor topic. To use this feature, use either
EndToEndTest or TopicManagementService in the properties file. </li>
<br />
  <li> When using <code>Secure Sockets Layer</code> (SSL) or any non-plaintext security protocol for AdminClient, please configure the following entries in the <code>single-cluster-monitor</code> props, <code>produce.producer.props</code>, as well as <code>consume.consumer.props</code>. https://docs.confluent.io/current/installation/configuration/admin-configs.html 
<ol>
  <li> ssl.key.password	</li>
  <li> ssl.keystore.location</li>
  <li> ssl.keystore.password </li>
  <li> ssl.truststore.location</li>
  <li> ssl.truststore.password</li>
</ol>
</ol>


### Build Xinfra Monitor
```
$ git clone https://github.com/linkedin/kafka-monitor.git
$ cd kafka-monitor 
$ ./gradlew jar
```

### Start XinfraMonitor to run tests/services specified in the config file
```
$ ./bin/xinfra-monitor-start.sh config/xinfra-monitor.properties
```

### Run Xinfra Monitor with arbitrary producer/consumer configuration (e.g. SASL enabled client)
Edit `config/xinfra-monitor.properties` to specify custom configurations for producer in the key/value map `produce.producer.props` in
`config/xinfra-monitor.properties`. Similarly specify configurations for
consumer as well. The documentation for producer and consumer in the key/value maps can be found in the Apache Kafka wiki.

```
$ ./bin/xinfra-monitor-start.sh config/xinfra-monitor.properties
```

### Run SingleClusterMonitor app to monitor kafka cluster

Metrics `produce-availability-avg` and `consume-availability-avg` demonstrate
whether messages can be properly produced to and consumed from this cluster.
See Service Overview wiki for how these metrics are derived.

```
$ ./bin/single-cluster-monitor.sh --topic test --broker-list localhost:9092
```

### Run MultiClusterMonitor app to monitor a pipeline of Kafka clusters connected by MirrorMaker
Edit `config/multi-cluster-monitor.properties` to specify the right broker
as suggested by the comment in the properties file

Metrics `produce-availability-avg` and `consume-availability-avg` demonstrate
whether messages can be properly produced to the source cluster and consumed
from the destination cluster. See config/multi-cluster-monitor.properties for
the full jmx path for these metrics.

```
$ ./bin/xinfra-monitor-start.sh config/multi-cluster-monitor.properties
```

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
- [Service and App Overview](https://github.com/linkedin/kafka-monitor/wiki)
- [Future Work](https://github.com/linkedin/kafka-monitor/wiki/Future-Work)
- [Application Configuration](https://github.com/linkedin/kafka-monitor/wiki/App-Configuration)
