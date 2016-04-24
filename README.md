# Kafka Monitor
Kafka Monitor is a framework to implement and execute long-running kafka
system tests in a real cluster. It complements Kafka’s existing system
tests by capturing potential bugs or regressions that are only likely to occur
after prolonged period of time or with low probability. Moreover, it allows you to monitor Kafka
cluster using end-to-end pipelines to obtain a number of derived vital stats
such as end-to-end latency, service availability and message loss rate. You can easily
deploy Kafka Monitor to test and monitor your Kafka cluster without requiring
any change to your application.

## Getting Started

### Prerequisites
Kafka Monitor requires Gradle 2.0 or higher. Java 7 should be used for
building in order to support both Java 7 and Java 8 at runtime.

### Build Kafka Monitor
```
$ go get github.com/linkedin/kafka-monitor
$ cd kafka-monitor 
$ ./gradlew jar
```

### Start KafkaMonitor to run tests/services specified in the config file
```
$ ./bin/kafka-monitor-start.sh config/kafka-monitor.properties
```

### Run BasicEndToEndTest to monitor kafka cluster
```
$ ./bin/end-to-end-test.sh --topic test --broker-list localhost:9092 --zookeeper localhost:2181
```

### Get metric values (e.g. service availability, message loss rate) in real-time as time series graphs
Open ```localhost:8000/index.html``` in your web browser

You can edit webapp/index.html to easily add new metrics to be displayed.

### Query metric value (e.g. availability) via HTTP request
```
curl localhost:8778/jolokia/read/kmf.services:type=produce-metrics/produce-availability-avg
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

## Motivation

Kafka has become a standard messaging system for large scale, streaming data.
In companies like LinkedIn it is used as backbone for various data pipelines
and is relied on by a variety of services. This makes Kafka a critical
component of a company’s infrastructure that should be extremely robust, i.e.
bug-free and fault-tolerant.

Kafka has relied on unit tests and system tests in virtual machines to
detect bugs before it is deployed in a real cluster. Yet we still see
occasional bugs that go undetected until Kafka has been deployed in a real
cluster for days or even weeks. These bugs have caused a lot of operational
overhead or even service disruption -- SREs need to rollback Kafka to an
earlier version and developers need to reproduce and investigate the bug. In
fact, many of these bugs could have been detected earlier if we had run Kafka’s
system tests for a long time with production traffic. In LinkedIn we have
relied on developers to manually run a variety of Kafka admin commands and
trigger system failure in order to validate Kafka’s operation before its
release, which is inconvenient. Kafka Monitor is designed to provide a
framework under which system tests can be created and continuously run in real
cluster. This allows us to use it as a vehicle for release validation by
letting it run against a test cluster over a prolonged duration of time.

It is important for users to be able to monitor the availability and
performance of its service. We can monitor Kafka server’s operation by reading
its JMX metrics or tracking CPU/memory/network usage on the hosts. But
currently there is no easy way to monitor Kafka from user’s perspective, e.g.
end-to-end latency or service availability. Doing so requires modification of
client application to do extra work, which may have undesirable performance
overhead, and is usually inconvenient for existing users of Kafka. Kafka Monitor
addresses this need by monitoring Kafka using an end-to-end pipeline without
requiring any change to existing deployment. Users should be able to simply run
Kafka Monitor against their existing deployment to obtain some very useful
metrics, e.g. end-to-end latency, service availability and message loss rate.

## Design

The goal of adding Kafka Monitor framework is to make it as easy as possible to
1) develop and execute long-running kafka-specific system tests in real
clusters, and 2) monitor existing Kafka deployment from user’s perspective.
Developers should be able to easily create new tests by composing reusable
modules to take actions and collect metrics. And users should be able to run
Kafka Monitor tests to perform actions at a user-defined schedule on the test cluster,
e.g. broker hard kill and cluster bounce, and validate that Kafka still works
well in accordance to its design.

A typical test may start some producers/consumers, take predefined sequence of
actions periodically, report metrics, and validate metrics against some
assertions. For example, Kafka Monitor can start one producer, one consumer,
and bounce a random broker (say if it is monitoring a test cluster) every five
minutes; the availability and message loss rate can be exposed via JMX metrics
that can be collected and displayed on a health dashboard in real-time; and an
alert is triggered if message loss rate is larger than 0.

To allow tests to be composed from reusable modules, we implement the logic of
periodic/long-running actions in services. A service will execute the action in
its own thread and export metrics. We have the following services to start
with:

- Produce service, which produces message to kafka and export produce rate and availability.
- Consume service, which consumes message from kafka and export message loss
rate, message duplicate rate and end-to-end latency. This service depends on
the produce service to provide messages that encode certain information.
- Broker bounce service, which bounce a given broker at the given interval.

A test will be composed of services and validate certain assertions either
continuously or periodically. For example, we can create a test that includes
one produce service, one consume service, and one broker bounce service. The
produce service and consume service will be configured to use the same topic.
And the test can validate that the message loss rate is constantly 0.

Finally, a given Kafka Monitor instance runs on a single physical machine and
multiple tests can run in one Kafka Monitor instance. The diagram below
demonstrates the relations between services, test and Kafka Monitor instance, as
well as how Kafka Monitor interacts with Kafka and user.

While all services in the same Kafka Monitor instance must run on the same
physical machine, we can start multiple Kafka Monitor instances in different
clusters that coordinate together to orchestrate a single end-to-end test. In
the test described by the diagram below, we start two Kafka Monitor instances
in two clusters. The first Kafka Monitor instance contains one produce service
that produces to Kafka cluster 1. The message is then mirrored from cluster 1
to cluster 2. Finally the consume service in the second Kafka Monitor instance
consumes messages from the same topic and export end-to-end latency of this
cross-cluster pipeline.


### Design of produce/consume service
Producer/consumer service can be extended to plugin custom implementation of
producer/consumer. Users can use their custom implementation of
producer/consumer to produce and consume messages in Kafka Monitor. A
configurable number of threads produces messages to a topic dedicated to Kafka
Monitor, and one thread consumes messages from the topic.

To measure the message loss/duplicated rate, producer produces messages with
integer index in the message payload. This integer index is incremented by 1
for every successful send per partition. Consumer parses the message to obtain
the index, and compares the index with the last index observed from the same
partition, to determine whether there is loss or duplicated message. Note that
producer produces messages in sync mode.

To measure end-to-end latency, message payload will also contain timestamp at
the time the message is constructed. Consumer parses the message to obtain the
timestamp, and determines the end-to-end latency by subtracting current time by
this timestamp.

To measure availability of produce service, producer keeps track of the message
produce_rate and error_rate. Error_rate will be positive if an exception is
thrown and caught when producer produces messages. Availability is measured as
average of per-partition availability. per-partition availability will be
measured as produce_rate/(produce_rate + error_rate), if produce_rate > 0;
otherwise per-partition availability is 0, since no message is produced in the
time interval used to measure the rate. By default this time interval is 30
seconds.

Here are a few example JMX metrics provided by ProduceService and ConsumerService:

- ConsumeByteRate
- ConsumeRecordRate
- ProduceRecordRate
- RecordDuplicateRate
- RecordLossRate
- RecordDelayAvg
- RecordDelayMax
- RecordDelay99thPercentile
- RecordDelay999thPercentile
- ProduceAvailability


## Future Work
Here are a few things that we plan to work on to make Kafka Monitor more
useful.

### Integration with Graphite and similar frameworks
It is useful for users to be able to view all kakfa-related metrics from one
web service in their organization. Graphite is one of the most popular open
source solutions that allow users to store metrics and view metrics as
time-series graphs. We plan to improve the existing DefaultMetricsReporterService so
that users can export Kafka Monitor metrics to Graphite and other metrics
storage services that they choose.

### Various improvements to test scheduling
Users should have the ability to schedule custom
actions (e.g. broker bounce, broker hard kill) to be executed at
regular interval. This can be used together with other services to make
assertions (e.g. no message loss, no message reorder) about kafka's performance
while these actions are taking place. This can be deployed your private
kafka cluster to test kafka's performance and fault tolerance.

### Automatic cluster deployment
Another future work is to provide capability to deploy kafka cluster using
Apache Kafka with the user-specified git hash value. This allows us to
automatically test a range of kafka commits to capture bugs that may be missed
by Apache Kafka's unit tests or system tests.



