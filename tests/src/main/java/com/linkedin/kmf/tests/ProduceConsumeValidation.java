/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.tests;

import com.linkedin.kmf.services.ConsumeService;
import com.linkedin.kmf.services.JettyService;
import com.linkedin.kmf.services.JolokiaService;
import com.linkedin.kmf.services.MetricsExportService;
import com.linkedin.kmf.services.ProduceService;
import com.linkedin.kmf.services.ServiceConfig;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/*
 * The ProduceConsumeValidation test is intended to monitor the health and performance of kafka brokers. It creates
 * one producer and one consumer with the given configuration, produces messages with increasing integer in the
 * message String, consumes messages, and keeps track of number of lost messages, duplicate messages, e2e latency,
 * throughput, etc.
 *
 * ProduceConsumeValidation test exports these metrics via JMX. It also periodically report metrics if DEBUG level logging
 * is enabled. This information can be used by other application to trigger alert when kafka brokers fail. It also
 * allows users to track end-to-end performance, e.g. latency and throughput.
 */

public class ProduceConsumeValidation implements Test {
  private static final Logger LOG = LoggerFactory.getLogger(ProduceConsumeValidation.class);

  private final ProduceService _produceService;
  private final ConsumeService _consumeService;
  private final MetricsExportService _metricsExportService;

  public ProduceConsumeValidation(Properties props) throws Exception {
    _produceService = new ProduceService(props);
    _consumeService = new ConsumeService(props);
    _metricsExportService = new MetricsExportService(props);
  }

  @Override
  public void awaitShutdown() {
    _produceService.awaitShutdown();
    _consumeService.awaitShutdown();
    _metricsExportService.awaitShutdown();
  }

  @Override
  public void start() {
    _produceService.start();
    _consumeService.start();
    _metricsExportService.start();
    LOG.info("ProduceConsumeValidation test started");
  }

  @Override
  public void stop() {
    _produceService.stop();
    _consumeService.stop();
    _metricsExportService.stop();
    LOG.info("ProduceConsumeValidation test stopped");
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

    parser.addArgument("--broker-list")
      .action(store())
      .required(true)
      .type(String.class)
      .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
      .dest("brokerList")
      .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

    parser.addArgument("--zookeeper")
      .action(store())
      .required(true)
      .type(String.class)
      .metavar("HOST:PORT")
      .dest("zkConnect")
      .help("The connection string for the zookeeper connection in the form host:port");

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
      .dest("producerConfig")
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
      .help("The maximum value in ms expected for latency percentile metric. " +
            "The percentile will be reported as Double.POSITIVE_INFINITY if its value exceeds the max value.");

    parser.addArgument("--latency-percentile-granularity-ms")
      .action(store())
      .required(false)
      .type(String.class)
      .metavar("LATENCY_PERCENTILE_GRANULARITY_MS")
      .dest("latencyPercentileGranularityMs")
      .help("The granularity in ms of latency percentile metric. This is the width of the bucket used in percentile calculation.");

    return parser;
  }

  public static void main(String[] args) throws Exception {
    ArgumentParser parser = argParser();
    Namespace res = parser.parseArgs(args);

    Properties props = new Properties();

    // produce service config
    props.put(ServiceConfig.ZOOKEEPER_CONNECT_CONFIG, res.getString("zkConnect"));
    props.put(ServiceConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("brokerList"));
    if (res.getString("producerClassName") != null)
      props.put(ServiceConfig.PRODUCER_CLASS_CONFIG, res.getString("producerClassName"));
    if (res.getString("topic") != null)
      props.put(ServiceConfig.TOPIC_CONFIG, res.getString("topic"));
    if (res.getString("producerId") != null)
      props.put(ServiceConfig.PRODUCER_ID_CONFIG, res.getString("producerId"));
    if (res.getString("recordDelayMs") != null)
      props.put(ServiceConfig.PRODUCE_RECORD_DELAY_MS_CONFIG, res.getString("recordDelayMs"));
    if (res.getString("recordSize") != null)
      props.put(ServiceConfig.PRODUCE_RECORD_SIZE_BYTE_CONFIG, res.getString("recordSize"));
    if (res.getString("producerConfig") != null)
      props.put(ServiceConfig.PRODUCER_PROPS_FILE_CONFIG, res.getString("producerConfig"));
    props.put(ServiceConfig.PRODUCE_THREAD_NUM_CONFIG, 5);

    // consume service config
    if (res.getString("consumerConfig") != null)
      props.put(ServiceConfig.CONSUMER_PROPS_FILE_CONFIG, res.getString("consumerConfig"));
    if (res.getString("consumerClassName") != null)
      props.put(ServiceConfig.CONSUME_CLASS_CONFIG, res.getString("consumerClassName"));
    if (res.getString("latencyPercentileMaxMs") != null)
      props.put(ServiceConfig.LATENCY_PERCENTILE_MAX_MS_CONFIG, res.getString("latencyPercentileMaxMs"));
    if (res.getString("latencyPercentileGranularityMs") != null)
      props.put(ServiceConfig.LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG, res.getString("latencyPercentileGranularityMs"));

    // metrics export service config
    if (res.getString("reportIntervalSec") != null)
      props.put(ServiceConfig.EXPORT_METRICS_REPORT_INTERVAL_SEC_CONFIG, res.getString("reportIntervalSec"));

    props.put(ServiceConfig.EXPORT_METRICS_NAMES_CONFIG,
              "kmf.services:type=produce-metrics:produce-availability-avg" +
              ",kmf.services:type=produce-metrics:records-produced-total" +
              ",kmf.services:type=consume-metrics:records-lost-total" +
              ",kmf.services:type=consume-metrics:records-duplicated-total" +
              ",kmf.services:type=consume-metrics:records-delay-avg");

    ProduceConsumeValidation test = new ProduceConsumeValidation(props);
    test.start();

    JolokiaService jolokiaService = new JolokiaService(new Properties());
    jolokiaService.start();

    JettyService jettyService = new JettyService(new Properties());
    jettyService.start();

    test.awaitShutdown();
  }
}
