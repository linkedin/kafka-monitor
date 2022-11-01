/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.apps;

import com.linkedin.xinfra.monitor.services.ConsumeService;
import com.linkedin.xinfra.monitor.services.ConsumerFactory;
import com.linkedin.xinfra.monitor.services.ConsumerFactoryImpl;
import com.linkedin.xinfra.monitor.services.DefaultMetricsReporterService;
import com.linkedin.xinfra.monitor.services.JolokiaService;
import com.linkedin.xinfra.monitor.services.ProduceService;
import com.linkedin.xinfra.monitor.services.Service;
import com.linkedin.xinfra.monitor.services.TopicManagementService;
import com.linkedin.xinfra.monitor.services.configs.ConsumeServiceConfig;
import com.linkedin.xinfra.monitor.services.configs.DefaultMetricsReporterServiceConfig;
import com.linkedin.xinfra.monitor.services.configs.MultiClusterTopicManagementServiceConfig;
import com.linkedin.xinfra.monitor.services.configs.ProduceServiceConfig;
import com.linkedin.xinfra.monitor.services.configs.TopicManagementServiceConfig;
import com.linkedin.xinfra.monitor.services.metrics.ClusterTopicManipulationMetrics;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.xinfra.monitor.common.Utils.prettyPrint;

/*
 * The SingleClusterMonitor app is intended to monitor the performance and availability of a given Kafka cluster. It creates
 * one producer and one consumer with the given configuration, produces messages with increasing integer in the
 * message payload, consumes messages, and keeps track of number of lost messages, duplicate messages, end-to-end latency etc.
 *
 * SingleClusterMonitor app exports these metrics via JMX. It also periodically report metrics if INFO level logging
 * is enabled. This information can be used by other application to trigger alert when availability of the Kafka cluster drops.
 */

public class SingleClusterMonitor implements App {
  private static final Logger LOG = LoggerFactory.getLogger(SingleClusterMonitor.class);

  private static final int SERVICES_INITIAL_CAPACITY = 4;
  private final TopicManagementService _topicManagementService;
  private final String _clusterName;
  private final List<Service> _allServices;
  private final boolean _isTopicManagementServiceEnabled;

  public SingleClusterMonitor(Map<String, Object> props, String clusterName) throws Exception {
    ConsumerFactory consumerFactory = new ConsumerFactoryImpl(props);
    _clusterName = clusterName;
    LOG.info("SingleClusterMonitor properties: {}", prettyPrint(props));
    TopicManagementServiceConfig config = new TopicManagementServiceConfig(props);
    _isTopicManagementServiceEnabled =
        config.getBoolean(TopicManagementServiceConfig.TOPIC_MANAGEMENT_ENABLED_CONFIG);
    _allServices = new ArrayList<>(SERVICES_INITIAL_CAPACITY);
    CompletableFuture<Void> topicPartitionResult;
    if (_isTopicManagementServiceEnabled) {
      String topicManagementServiceName = String.format("Topic-management-service-for-%s", clusterName);
      _topicManagementService = new TopicManagementService(props, topicManagementServiceName);
      topicPartitionResult = _topicManagementService.topicPartitionResult();

      // block on the MultiClusterTopicManagementService to complete.
      topicPartitionResult.get();

      _allServices.add(_topicManagementService);
    } else {
      _topicManagementService = null;
      topicPartitionResult = new CompletableFuture<>();
      topicPartitionResult.complete(null);
    }
    ProduceService produceService = new ProduceService(props, clusterName);
    ConsumeService consumeService = new ConsumeService(clusterName, topicPartitionResult, consumerFactory);
    _allServices.add(produceService);
    _allServices.add(consumeService);
  }

  @Override
  public void start() throws Exception {
    if (_isTopicManagementServiceEnabled) {
      _topicManagementService.start();
      CompletableFuture<Void> topicPartitionResult = _topicManagementService.topicPartitionResult();

      try {
      /* Delay 2 second to reduce the chance that produce and consumer thread has race condition
      with TopicManagementService and MultiClusterTopicManagementService */
        long threadSleepMs = TimeUnit.SECONDS.toMillis(2);
        Thread.sleep(threadSleepMs);
      } catch (InterruptedException e) {
        throw new Exception("Interrupted while sleeping the thread", e);
      }
      CompletableFuture<Void> topicPartitionFuture = topicPartitionResult.thenRun(() -> {
        for (Service service : _allServices) {
          if (!service.isRunning()) {
            LOG.debug("Now starting {}", service.getServiceName());
            service.start();
          }
        }
      });

      try {
        topicPartitionFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new Exception("Exception occurred while getting the TopicPartitionFuture", e);
      }

    } else {
      for (Service service : _allServices) {
        if (!service.isRunning()) {
          LOG.debug("Now starting {}", service.getServiceName());
          service.start();
        }
      }
    }

    LOG.info(_clusterName + "/SingleClusterMonitor started!");
  }

  @Override
  public void stop() {
    for (Service service : _allServices) {
      service.stop();
    }
    LOG.info(_clusterName + "/SingleClusterMonitor stopped.");
  }

  @Override
  public boolean isRunning() {
    boolean isRunning = true;

    for (Service service : _allServices) {
      if (!service.isRunning()) {
        isRunning = false;
        LOG.info("{} is not running.", service.getServiceName());
      }
    }

    return isRunning;
  }

  @Override
  public void awaitShutdown() {
    for (Service service : _allServices) {
      service.awaitShutdown(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
  }

  /** Get the command-line argument parser. */
  private static ArgumentParser argParser() {
    ArgumentParser parser = ArgumentParsers
      .newArgumentParser("")
      .defaultHelp(true)
      .description("");

    parser.addArgument("--topic")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .metavar("TOPIC")
      .dest("topic")
      .help("Produce messages to this topic and consume message from this topic");

    parser.addArgument("--producer-id")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .dest("producerId")
      .help("The producerId will be used by producer client and encoded in the messages to the topic");

    parser.addArgument("--broker-list")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(true)
      .type(String.class)
      .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
      .dest("brokerList")
      .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

    parser.addArgument("--record-size")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .metavar("RECORD_SIZE")
      .dest("recordSize")
      .help("The size of each record.");

    parser.addArgument("--producer-class")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .metavar("PRODUCER_CLASS_NAME")
      .dest("producerClassName")
      .help("Specify the class of producer. Available choices include newProducer or class name");

    parser.addArgument("--consumer-class")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .metavar("CONSUMER_CLASS_NAME")
      .dest("consumerClassName")
      .help("Specify the class of consumer. Available choices include oldConsumer, newConsumer, or class name");

    parser.addArgument("--producer.config")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .metavar("PRODUCER_CONFIG")
      .dest("producerConfig")
      .help("Producer config properties file.");

    parser.addArgument("--consumer.config")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .metavar("CONSUMER_CONFIG")
      .dest("consumerConfig")
      .help("Consumer config properties file.");

    parser.addArgument("--report-interval-sec")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .metavar("REPORT_INTERVAL_SEC")
      .dest("reportIntervalSec")
      .help("Interval in sec with which to export stats");

    parser.addArgument("--record-delay-ms")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .metavar("RECORD_DELAY_MS")
      .dest("recordDelayMs")
      .help("The delay in ms before sending next record to the same partition");

    parser.addArgument("--latency-percentile-max-ms")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .metavar("LATENCY_PERCENTILE_MAX_MS")
      .dest("latencyPercentileMaxMs")
      .help("The maximum value in ms expected for latency percentile metric. " +
            "The percentile will be reported as Double.POSITIVE_INFINITY if its value exceeds the max value.");

    parser.addArgument("--latency-percentile-granularity-ms")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(String.class)
      .metavar("LATENCY_PERCENTILE_GRANULARITY_MS")
      .dest("latencyPercentileGranularityMs")
      .help("The granularity in ms of latency percentile metric. This is the width of the bucket used in percentile calculation.");

    parser.addArgument("--topic-creation-enabled")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(Boolean.class)
      .metavar("AUTO_TOPIC_CREATION_ENABLED")
      .dest("autoTopicCreationEnabled")
      .help(TopicManagementServiceConfig.TOPIC_CREATION_ENABLED_DOC);

    parser.addArgument("--topic-add-partition-enabled")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(Boolean.class)
      .metavar("TOPIC_ADD_PARTITION_ENABLED")
      .dest("topicAddPartitionEnabled")
      .help(TopicManagementServiceConfig.TOPIC_ADD_PARTITION_ENABLED_DOC);

    parser.addArgument("--topic-reassign-partition-and-elect-leader-enabled")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(Boolean.class)
      .metavar("TOPIC_REASSIGN_PARTITION_AND_ELECT_LEADER_ENABLED")
      .dest("topicReassignPartitionAndElectLeaderEnabled")
      .help(TopicManagementServiceConfig.TOPIC_REASSIGN_PARTITION_AND_ELECT_LEADER_ENABLED_DOC);

    parser.addArgument("--replication-factor")
        .action(net.sourceforge.argparse4j.impl.Arguments.store())
        .required(false)
        .type(Integer.class)
        .metavar("REPLICATION_FACTOR")
        .dest("replicationFactor")
        .help(TopicManagementServiceConfig.TOPIC_REPLICATION_FACTOR_DOC);

    parser.addArgument("--topic-rebalance-interval-ms")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(Integer.class)
      .metavar("REBALANCE_MS")
      .dest("rebalanceMs")
      .help(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_DOC);

    parser.addArgument("--topic-preferred-leader-election-interval-ms")
      .action(net.sourceforge.argparse4j.impl.Arguments.store())
      .required(false)
      .type(Integer.class)
      .metavar("PREFERED_LEADER_ELECTION_INTERVAL_MS")
      .dest("preferredLeaderElectionIntervalMs")
      .help(MultiClusterTopicManagementServiceConfig.PREFERRED_LEADER_ELECTION_CHECK_INTERVAL_MS_DOC);

    return parser;
  }

  public static void main(String[] args) throws Exception {
    ArgumentParser parser = argParser();
    if (args.length == 0) {
      System.out.println(parser.formatHelp());
      System.exit(-1);
    }

    Namespace res = parser.parseArgs(args);
    Map<String, Object> props = new HashMap<>();
    // produce service config
    props.put(ProduceServiceConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("brokerList"));
    if (res.getString("producerClassName") != null)
      props.put(ProduceServiceConfig.PRODUCER_CLASS_CONFIG, res.getString("producerClassName"));
    if (res.getString("topic") != null)
      props.put(ProduceServiceConfig.TOPIC_CONFIG, res.getString("topic"));
    if (res.getString("producerId") != null)
      props.put(ProduceServiceConfig.PRODUCER_ID_CONFIG, res.getString("producerId"));
    if (res.getString("recordDelayMs") != null)
      props.put(ProduceServiceConfig.PRODUCE_RECORD_DELAY_MS_CONFIG, res.getString("recordDelayMs"));
    if (res.getString("recordSize") != null)
      props.put(ProduceServiceConfig.PRODUCE_RECORD_SIZE_BYTE_CONFIG, res.getString("recordSize"));
    if (res.getString("producerConfig") != null)
      props.put(ProduceServiceConfig.PRODUCER_PROPS_CONFIG, Utils.loadProps(res.getString("producerConfig")));

    props.put(ProduceServiceConfig.PRODUCE_THREAD_NUM_CONFIG, 5);

    // consume service config
    if (res.getString("consumerConfig") != null)
      props.put(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG, Utils.loadProps(res.getString("consumerConfig")));
    if (res.getString("consumerClassName") != null)
      props.put(ConsumeServiceConfig.CONSUMER_CLASS_CONFIG, res.getString("consumerClassName"));
    if (res.getString("latencyPercentileMaxMs") != null)
      props.put(ConsumeServiceConfig.LATENCY_PERCENTILE_MAX_MS_CONFIG, res.getString("latencyPercentileMaxMs"));
    if (res.getString("latencyPercentileGranularityMs") != null)
      props.put(ConsumeServiceConfig.LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG, res.getString("latencyPercentileGranularityMs"));

    // topic management service config
    if (res.getBoolean("autoTopicCreationEnabled") != null)
      props.put(TopicManagementServiceConfig.TOPIC_CREATION_ENABLED_CONFIG, res.getBoolean("autoTopicCreationEnabled"));
    if (res.getBoolean("topicAddPartitionEnabled") != null)
      props.put(TopicManagementServiceConfig.TOPIC_ADD_PARTITION_ENABLED_CONFIG, res.getBoolean("topicAddPartitionEnabled"));
    if (res.getBoolean("topicReassignPartitionAndElectLeaderEnabled") != null)
      props.put(TopicManagementServiceConfig.TOPIC_REASSIGN_PARTITION_AND_ELECT_LEADER_ENABLED_CONFIG, res.getBoolean("topicReassignPartitionAndElectLeaderEnabled"));
    if (res.getInt("replicationFactor") != null)
      props.put(TopicManagementServiceConfig.TOPIC_REPLICATION_FACTOR_CONFIG, res.getInt("replicationFactor"));
    if (res.getInt("rebalanceMs") != null)
      props.put(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG, res.getInt("rebalanceMs"));
    if (res.getLong("preferredLeaderElectionIntervalMs") != null)
      props.put(MultiClusterTopicManagementServiceConfig.PREFERRED_LEADER_ELECTION_CHECK_INTERVAL_MS_CONFIG, res.getLong("preferredLeaderElectionIntervalMs"));
    SingleClusterMonitor app = new SingleClusterMonitor(props, "single-cluster-monitor");
    app.start();

    // metrics export service config
    props = new HashMap<>();
    if (res.getString("reportIntervalSec") != null)
      props.put(DefaultMetricsReporterServiceConfig.REPORT_INTERVAL_SEC_CONFIG, res.getString("reportIntervalSec"));
    List<String> metrics = Arrays.asList(
      "kmf.services:type=consume-service,name=*:topic-partitions-count",
      "kmf.services:type=produce-service,name=*:produce-availability-avg",
      "kmf.services:type=consume-service,name=*:consume-availability-avg",
      "kmf.services:type=produce-service,name=*:records-produced-total",
      "kmf.services:type=consume-service,name=*:records-consumed-total",
      "kmf.services:type=consume-service,name=*:records-lost-total",
      "kmf.services:type=consume-service,name=*:records-lost-rate",
      "kmf.services:type=consume-service,name=*:records-duplicated-total",
      "kmf.services:type=consume-service,name=*:records-delay-ms-avg",
      "kmf.services:type=produce-service,name=*:records-produced-rate",
      "kmf.services:type=produce-service,name=*:produce-error-rate",
      "kmf.services:type=consume-service,name=*:consume-error-rate",
      "kmf.services:type=commit-availability-service,name=*:offsets-committed-total",
      "kmf.services:type=commit-availability-service,name=*:offsets-committed-avg",
      "kmf.services:type=commit-availability-service,name=*:failed-commit-offsets-total",
      "kmf.services:type=commit-availability-service,name=*:failed-commit-offsets-avg",
      "kmf.services:type=commit-latency-service,name=*:commit-offset-latency-ms-avg",
      "kmf.services:type=commit-latency-service,name=*:commit-offset-latency-ms-max",
      "kmf.services:type=commit-latency-service,name=*:commit-offset-latency-ms-99th",
      "kmf.services:type=commit-latency-service,name=*:commit-offset-latency-ms-999th",
      "kmf.services:type=commit-latency-service,name=*:commit-offset-latency-ms-9999th",
      "kmf.services:type=offset-commit-service,name=*:offset-commit-availability-avg",
      "kmf.services:type=offset-commit-service,name=*:offset-commit-service-success-rate",
      "kmf.services:type=offset-commit-service,name=*:offset-commit-service-success-total",
      "kmf.services:type=offset-commit-service,name=*:offset-commit-service-failure-rate",
      "kmf.services:type=offset-commit-service,name=*:offset-commit-service-failure-total",

      "kmf.services:type=" + ClusterTopicManipulationMetrics.METRIC_GROUP_NAME
          + ",name=*:topic-creation-metadata-propagation-ms-avg",
      "kmf.services:type=" + ClusterTopicManipulationMetrics.METRIC_GROUP_NAME
          + ",name=*:topic-creation-metadata-propagation-ms-max",
      "kmf.services:type=" + ClusterTopicManipulationMetrics.METRIC_GROUP_NAME
          + ",name=*:topic-deletion-metadata-propagation-ms-avg",
      "kmf.services:type=" + ClusterTopicManipulationMetrics.METRIC_GROUP_NAME
          + ",name=*:topic-deletion-metadata-propagation-ms-max"
    );

    props.put(DefaultMetricsReporterServiceConfig.REPORT_METRICS_CONFIG, metrics);

    DefaultMetricsReporterService metricsReporterService = new DefaultMetricsReporterService(props, "end-to-end");
    metricsReporterService.start();

    JolokiaService jolokiaService = new JolokiaService(new HashMap<>(), "end-to-end");
    jolokiaService.start();
  }
}
