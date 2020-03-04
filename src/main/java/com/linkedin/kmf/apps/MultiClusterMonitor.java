/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.apps;

import com.linkedin.kmf.apps.configs.MultiClusterMonitorConfig;
import com.linkedin.kmf.services.ConsumeService;
import com.linkedin.kmf.services.ConsumerFactoryImpl;
import com.linkedin.kmf.services.MultiClusterTopicManagementService;
import com.linkedin.kmf.services.ProduceService;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * The MultiClusterMonitor app is intended to monitor the performance and availability of a pipeline of one
 * or more Kafka clusters. It creates one producer and one consumer with the given configuration, produces messages with increasing
 * integer in the message payload, consumes messages, and keeps track of the number of lost messages, duplicate messages, end-to-end
 * latency etc. Furthermore, MultiClusterMonitor use MultiClusterTopicManagementService to manage the monitor topics
 * across Kafka clusters and make sure they have the same number of partitions.
 */

public class MultiClusterMonitor implements App {
  private static final Logger LOG = LoggerFactory.getLogger(MultiClusterMonitor.class);

  private final MultiClusterTopicManagementService _multiClusterTopicManagementService;
  private final ProduceService _produceService;
  private final ConsumeService _consumeService;
  private final String _name;

  public MultiClusterMonitor(Map<String, Object> props, String name) throws Exception {
    _name = name;
    MultiClusterMonitorConfig config = new MultiClusterMonitorConfig(props);
    _multiClusterTopicManagementService = new MultiClusterTopicManagementService(createMultiClusterTopicManagementServiceProps(props, config), name);
    CompletableFuture<Void> topicPartitionReady = _multiClusterTopicManagementService.topicPartitionResult();
    _produceService = new ProduceService(createProduceServiceProps(props, config), name);
    ConsumerFactoryImpl consumerFactory = new ConsumerFactoryImpl(createConsumeServiceProps(props, config));
    _consumeService = new ConsumeService(name, topicPartitionReady, consumerFactory);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> createProduceServiceProps(Map<String, Object> props, MultiClusterMonitorConfig config) {
    Map<String, Object> serviceProps = props.containsKey(MultiClusterMonitorConfig.PRODUCE_SERVICE_CONFIG)
        ? (Map) props.get(MultiClusterMonitorConfig.PRODUCE_SERVICE_CONFIG) : new HashMap<>();
    serviceProps.put(MultiClusterMonitorConfig.TOPIC_CONFIG, config.getString(MultiClusterMonitorConfig.TOPIC_CONFIG));

    return serviceProps;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> createConsumeServiceProps(Map<String, Object> props, MultiClusterMonitorConfig config) {
    Map<String, Object> serviceProps = props.containsKey(MultiClusterMonitorConfig.CONSUME_SERVICE_CONFIG)
        ? (Map) props.get(MultiClusterMonitorConfig.CONSUME_SERVICE_CONFIG) : new HashMap<>();
    serviceProps.put(MultiClusterMonitorConfig.TOPIC_CONFIG, config.getString(MultiClusterMonitorConfig.TOPIC_CONFIG));

    return serviceProps;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> createMultiClusterTopicManagementServiceProps(Map<String, Object> props, MultiClusterMonitorConfig config) {
    Map<String, Object> serviceProps = new HashMap<>();
    serviceProps.put(MultiClusterMonitorConfig.TOPIC_MANAGEMENT_SERVICE_CONFIG, props.get(MultiClusterMonitorConfig.TOPIC_MANAGEMENT_SERVICE_CONFIG));
    serviceProps.put(MultiClusterMonitorConfig.TOPIC_CONFIG, config.getString(MultiClusterMonitorConfig.TOPIC_CONFIG));
    return serviceProps;
  }

  @Override
  public void start() {
    _multiClusterTopicManagementService.start();
    CompletableFuture<Void> topicPartitionResult = _multiClusterTopicManagementService.topicPartitionResult();
    topicPartitionResult.thenRun(() -> {
      _produceService.start();
      _consumeService.start();
    });
    LOG.info(_name + "/MultiClusterMonitor started.");
  }

  @Override
  public void stop() {
    _multiClusterTopicManagementService.stop();
    _produceService.stop();
    _consumeService.stop();
    LOG.info(_name + "/MultiClusterMonitor stopped");
  }

  @Override
  public boolean isRunning() {
    return _multiClusterTopicManagementService.isRunning() && _produceService.isRunning() && _consumeService.isRunning();
  }

  @Override
  public void awaitShutdown() {
    _multiClusterTopicManagementService.awaitShutdown();
    _produceService.awaitShutdown();
    _consumeService.awaitShutdown();
  }
}
