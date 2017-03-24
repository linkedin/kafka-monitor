/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.apps;

import com.linkedin.kmf.apps.configs.MirrorPipelineMonitorConfig;
import com.linkedin.kmf.services.ConsumeService;
import com.linkedin.kmf.services.DefaultMetricsReporterService;
import com.linkedin.kmf.services.JettyService;
import com.linkedin.kmf.services.JolokiaService;
import com.linkedin.kmf.services.ProduceService;
import com.linkedin.kmf.services.TopicManagementService;
import com.linkedin.kmf.services.configs.CommonServiceConfig;
import com.linkedin.kmf.services.configs.ConsumeServiceConfig;
import com.linkedin.kmf.services.configs.DefaultMetricsReporterServiceConfig;
import com.linkedin.kmf.services.configs.ProduceServiceConfig;
import com.linkedin.kmf.services.configs.TopicManagementServiceConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/*
 * The MirrorPipelineMonitor app is intended to monitor the health and performance of kafka brokers across a mirror maker
 * pipeline. It creates one producer and one consumer with the given configuration, produces messages with increasing
 * integer in the message String, consumes messages, and keeps track of number of lost messages, duplicate messages, e2e
 * latency, throughput, etc. As well, MirrorPipelineMonitor creates a topic management service for every kafka and zookeeper
 * broker in the pipeline.
 *
 * MirrorPipelineMonitor app exports these metrics via JMX. It also periodically report metrics if DEBUG level logging
 * is enabled. This information can be used by other application to trigger alert when kafka brokers fail. It also
 * allows users to track pipeline performance, e.g. latency and throughput.
 */

public class MirrorPipelineMonitor implements App {
  private static final Logger LOG = LoggerFactory.getLogger(MirrorPipelineMonitor.class);

  private final ProduceService _produceService;
  private final ConsumeService _consumeService;
  private final List<TopicManagementService> _topicManagementServices;
  private final String _name;

  public MirrorPipelineMonitor(Map<String, Object> props, String name) throws Exception {
    _name = name;
    MirrorPipelineMonitorConfig config = new MirrorPipelineMonitorConfig(props);
    List<String> zookeeperUrls = config.getList(MirrorPipelineMonitorConfig.ZOOKEEPER_CONNECT_LIST_CONFIG);
    String topic = config.getString(CommonServiceConfig.TOPIC_CONFIG);
    _produceService = new ProduceService(createProduceServiceProps(props, topic), name);
    _consumeService = new ConsumeService(createConsumeServiceProps(props, topic), name);
    _topicManagementServices = createTopicManagementServices(props, zookeeperUrls, topic, name);
  }

  private List<TopicManagementService> createTopicManagementServices(Map<String, Object> props,
                                                                     List<String> zookeeperUrls,
                                                                     String topic,
                                                                     String name) throws Exception {
    Map<String, Object> topicManagementProps = createTopicManagementServiceProps(props, topic);
    topicManagementProps.put(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG, "");
    TopicManagementServiceConfig config = new TopicManagementServiceConfig(topicManagementProps);
    double partitionsToBrokerRatio = config.getDouble(TopicManagementServiceConfig.PARTITIONS_TO_BROKER_RATIO_THRESHOLD);

    // Mirrored topics must have the same number of partitions but may have a different
    // number of brokers. Separate partitionToBrokerRatios must be maintained to achieve this requirement.
    // TODO: Allow TopicManagementService to handle changing number of brokers
    Map<String, Integer> zookeeperBrokerCount = new HashMap<>();
    int maxNumPartitions = 0;
    for (String zookeeper: zookeeperUrls) {
      if (zookeeperBrokerCount.containsKey(zookeeper)) {
        throw new RuntimeException(_name + ": Duplicate zookeeper connections specified for topic management.");
      }

      int brokerCount = com.linkedin.kmf.common.Utils.getBrokerCount(zookeeper);
      int numPartitions = (int) Math.ceil(partitionsToBrokerRatio * brokerCount);
      maxNumPartitions = Math.max(maxNumPartitions, numPartitions);
      zookeeperBrokerCount.put(zookeeper, brokerCount);
    }

    List<TopicManagementService> topicManagementServices = new ArrayList<>();
    for (String zookeeper: zookeeperUrls) {
      topicManagementProps.put(ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
      double partitionToBrokerRatio = maxNumPartitions / zookeeperBrokerCount.get(zookeeper);
      topicManagementProps.put(TopicManagementServiceConfig.PARTITIONS_TO_BROKER_RATIO_CONFIG, partitionToBrokerRatio);
      topicManagementServices.add(new TopicManagementService(topicManagementProps, name + ":" + zookeeper));
    }

    return topicManagementServices;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> createProduceServiceProps(Map<String, Object> props, String topic) {
    Map<String, Object> produceServiceProps = props.containsKey(MirrorPipelineMonitorConfig.PRODUCE_SERVICE_CONFIG)
      ? (Map) props.get(MirrorPipelineMonitorConfig.PRODUCE_SERVICE_CONFIG) : new HashMap<>();
    produceServiceProps.put(CommonServiceConfig.TOPIC_CONFIG, props.get(CommonServiceConfig.TOPIC_CONFIG));

    return produceServiceProps;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> createConsumeServiceProps(Map<String, Object> props, String topic) {
    Map<String, Object> consumeServiceProps = props.containsKey(MirrorPipelineMonitorConfig.CONSUME_SERVICE_CONFIG)
      ? (Map) props.get(MirrorPipelineMonitorConfig.CONSUME_SERVICE_CONFIG) : new HashMap<>();
    consumeServiceProps.put(CommonServiceConfig.TOPIC_CONFIG, props.get(CommonServiceConfig.TOPIC_CONFIG));

    return consumeServiceProps;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> createTopicManagementServiceProps(Map<String, Object> props, String topic) {
    Map<String, Object> topicManagementServiceProps = props.containsKey(MirrorPipelineMonitorConfig.TOPIC_MANAGEMENT_SERVICE_CONFIG)
      ? (Map) props.get(MirrorPipelineMonitorConfig.TOPIC_MANAGEMENT_SERVICE_CONFIG) : new HashMap<>();
    topicManagementServiceProps.put(CommonServiceConfig.TOPIC_CONFIG, topic);
    return topicManagementServiceProps;
  }

  @Override
  public void start() {
    _produceService.start();
    _consumeService.start();
    for (TopicManagementService topicManagementService : _topicManagementServices) {
      topicManagementService.start();
    }
    LOG.info(_name + "/MirrorPipelineMonitor started");
  }

  @Override
  public void stop() {
    for (TopicManagementService topicManagementService : _topicManagementServices) {
      topicManagementService.stop();
    }
    _produceService.stop();
    _consumeService.stop();
    LOG.info(_name + "/MirrorPipelineMonitor stopped");
  }

  @Override
  public boolean isRunning() {
    for (TopicManagementService topicManagementService : _topicManagementServices) {
      if (!topicManagementService.isRunning()) {
        return false;
      }
    }

    return _produceService.isRunning() && _consumeService.isRunning();
  }

  @Override
  public void awaitShutdown() {
    for (TopicManagementService topicManagementService : _topicManagementServices) {
      topicManagementService.awaitShutdown();
    }
    _produceService.awaitShutdown();
    _consumeService.awaitShutdown();
  }

  /** Get the command-line argument parser. */
  private static ArgumentParser argParser() {
    ArgumentParser parser = ArgumentParsers
      .newArgumentParser("")
      .defaultHelp(true)
      .description("");

    parser.addArgument("--topic")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("TOPIC")
      .dest("topic")
      .help("Produce messages to this topic and consume message from this topic");

    parser.addArgument("--producer-id")
      .action(store())
      .required(false)
      .type(String.class)
      .dest("producerId")
      .help("The producerId will be used by producer client and encoded in the messages to the topic");

    parser.addArgument("--producer-broker-list")
      .action(store())
      .required(true)
      .type(String.class)
      .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
      .dest("producerBrokerList")
      .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

    parser.addArgument("--consumer-broker-list")
      .action(store())
      .required(true)
      .type(String.class)
      .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
      .dest("consumerBrokerList")
      .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

    parser.addArgument("--producer-zookeeper")
      .action(store())
      .required(true)
      .type(String.class)
      .metavar("HOST:PORT")
      .dest("producerZKConnect")
      .help("The connection string for the zookeeper connection in the form host:port");

    parser.addArgument("--consumer-zookeeper")
      .action(store())
      .required(true)
      .type(String.class)
      .metavar("HOST:PORT")
      .dest("consumerZKConnect")
      .help("The connection string for the zookeeper connection in the form host:port");

    parser.addArgument("--topic-management-zookeeper-list")
      .action(store())
      .required(true)
      .type(String.class)
      .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
      .dest("topicManagementZKConnect")
      .help("Comma-separated list of zookeeper connections in the form HOST1:PORT1,HOST2:PORT2,...");

    parser.addArgument("--record-size")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("RECORD_SIZE")
      .dest("recordSize")
      .help("The size of each record.");

    parser.addArgument("--producer-class")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("PRODUCER_CLASS_NAME")
      .dest("producerClassName")
      .help("Specify the class of producer. Available choices include newProducer or class name");

    parser.addArgument("--consumer-class")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("CONSUMER_CLASS_NAME")
      .dest("consumerClassName")
      .help("Specify the class of consumer. Available choices include oldConsumer, newConsumer, or class name");

    parser.addArgument("--producer.config")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("PRODUCER_CONFIG")
      .dest("produceServiceConfig")
      .help("Producer config properties file.");

    parser.addArgument("--consumer.config")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("CONSUMER_CONFIG")
      .dest("consumerConfig")
      .help("Consumer config properties file.");

    parser.addArgument("--report-interval-sec")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("REPORT_INTERVAL_SEC")
      .dest("reportIntervalSec")
      .help("Interval in sec with which to export stats");

    parser.addArgument("--record-delay-ms")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("RECORD_DELAY_MS")
      .dest("recordDelayMs")
      .help("The delay in ms before sending next record to the same partition");

    parser.addArgument("--latency-percentile-max-ms")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("LATENCY_PERCENTILE_MAX_MS")
      .dest("latencyPercentileMaxMs")
      .help("The maximum value in ms expected for latency percentile metric. "
        + "The percentile will be reported as Double.POSITIVE_INFINITY if its value exceeds the max value.");

    parser.addArgument("--latency-percentile-granularity-ms")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("LATENCY_PERCENTILE_GRANULARITY_MS")
      .dest("latencyPercentileGranularityMs")
      .help("The granularity in ms of latency percentile metric. This is the width of the bucket used in percentile calculation.");

    parser.addArgument("--topic-creation-enabled")
      .action(store())
      .required(false)
      .type(Boolean.class)
      .metavar("AUTO_TOPIC_CREATION_ENABLED")
      .dest("autoTopicCreationEnabled")
      .help(TopicManagementServiceConfig.TOPIC_CREATION_ENABLED_DOC);

    parser.addArgument("--topic-rebalance-interval-ms")
      .action(store())
      .required(false)
      .type(Integer.class)
      .metavar("REBALANCE_MS")
      .dest("rebalanceMs")
      .help(TopicManagementServiceConfig.REBALANCE_INTERVAL_MS_DOC);

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
    Map<String, Object> produceServiceProps = new HashMap<>();
    Map<String, Object> consumeServiceProps = new HashMap<>();
    Map<String, Object> topicManagementServiceProps = new HashMap<>();

    if (res.getString("topic") != null)
      props.put(CommonServiceConfig.TOPIC_CONFIG, res.getString("topic"));

    // produce service config
    produceServiceProps.put(ProduceServiceConfig.ZOOKEEPER_CONNECT_CONFIG, res.getString("producerZKConnect"));
    produceServiceProps.put(ProduceServiceConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("producerBrokerList"));
    if (res.getString("producerClassName") != null)
      produceServiceProps.put(ProduceServiceConfig.PRODUCER_CLASS_CONFIG, res.getString("producerClassName"));
    if (res.getString("producerId") != null)
      produceServiceProps.put(ProduceServiceConfig.PRODUCER_ID_CONFIG, res.getString("producerId"));
    if (res.getString("recordDelayMs") != null)
      produceServiceProps.put(ProduceServiceConfig.PRODUCE_RECORD_DELAY_MS_CONFIG, res.getString("recordDelayMs"));
    if (res.getString("recordSize") != null)
      produceServiceProps.put(ProduceServiceConfig.PRODUCE_RECORD_SIZE_BYTE_CONFIG, res.getString("recordSize"));
    if (res.getString("producerConfig") != null)
      produceServiceProps.put(ProduceServiceConfig.PRODUCER_PROPS_CONFIG, Utils.loadProps(res.getString("producerConfig")));
    if (res.getInt("rebalanceMs") != null)
      produceServiceProps.put(TopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG, res.getInt("rebalanceMs"));

    props.put(ProduceServiceConfig.PRODUCE_THREAD_NUM_CONFIG, 5);

    // consume service config
    consumeServiceProps.put(ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG, res.getString("consumerZKConnect"));
    consumeServiceProps.put(ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("consumerBrokerList"));
    if (res.getString("consumerConfig") != null)
      consumeServiceProps.put(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG, Utils.loadProps(res.getString("consumerConfig")));
    if (res.getString("consumerClassName") != null)
      consumeServiceProps.put(ConsumeServiceConfig.CONSUMER_CLASS_CONFIG, res.getString("consumerClassName"));
    if (res.getString("latencyPercentileMaxMs") != null)
      consumeServiceProps.put(ConsumeServiceConfig.LATENCY_PERCENTILE_MAX_MS_CONFIG, res.getString("latencyPercentileMaxMs"));
    if (res.getString("latencyPercentileGranularityMs") != null)
      consumeServiceProps.put(ConsumeServiceConfig.LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG, res.getString("latencyPercentileGranularityMs"));

    // topic management service config
    String[] topicManagementZKConnect = res.getString("topicManagementZKConnect").split(",");
    topicManagementServiceProps.put(MirrorPipelineMonitorConfig.ZOOKEEPER_CONNECT_LIST_CONFIG, Arrays.asList(topicManagementZKConnect));
    if (res.getBoolean("autoTopicCreationEnabled") != null)
      topicManagementServiceProps.put(TopicManagementServiceConfig.TOPIC_CREATION_ENABLED_CONFIG, res.getBoolean("autoTopicCreationEnabled"));

    props.put(MirrorPipelineMonitorConfig.PRODUCE_SERVICE_CONFIG, produceServiceProps);
    props.put(MirrorPipelineMonitorConfig.CONSUME_SERVICE_CONFIG, consumeServiceProps);
    props.put(MirrorPipelineMonitorConfig.TOPIC_MANAGEMENT_SERVICE_CONFIG, topicManagementServiceProps);

    MirrorPipelineMonitor app = new MirrorPipelineMonitor(props, "pipeline");
    app.start();

    // metrics export service config
    props = new HashMap<>();
    if (res.getString("reportIntervalSec") != null)
      props.put(DefaultMetricsReporterServiceConfig.REPORT_INTERVAL_SEC_CONFIG, res.getString("reportIntervalSec"));
    List<String> metrics = Arrays.asList(
      "kmf.services:type=produce-service,name=*:produce-availability-avg",
      "kmf.services:type=produce-service,name=*:records-produced-total",
      "kmf.services:type=consume-service,name=*:records-consumed-total",
      "kmf.services:type=consume-service,name=*:records-lost-total",
      "kmf.services:type=consume-service,name=*:records-duplicated-total",
      "kmf.services:type=consume-service,name=*:records-delay-ms-avg",
      "kmf.services:type=produce-service,name=*:records-produced-rate",
      "kmf.services:type=produce-service,name=*:produce-error-rate",
      "kmf.services:type=consume-service,name=*:consume-error-rate");
    props.put(DefaultMetricsReporterServiceConfig.REPORT_METRICS_CONFIG, metrics);


    DefaultMetricsReporterService metricsReporterService = new DefaultMetricsReporterService(props, "pipeline");
    metricsReporterService.start();

    JolokiaService jolokiaService = new JolokiaService(new HashMap<String, Object>(), "pipeline");
    jolokiaService.start();

    JettyService jettyService = new JettyService(new HashMap<String, Object>(), "pipeline");
    jettyService.start();

    app.awaitShutdown();
  }
}
