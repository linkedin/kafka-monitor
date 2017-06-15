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

import com.linkedin.kmf.apps.configs.MultiClusterMonitorConfig;
import com.linkedin.kmf.services.ConsumeService;
import com.linkedin.kmf.services.MultiClusterTopicManagementService;
import com.linkedin.kmf.services.ProduceService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
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

  private final MultiClusterTopicManagementService _topicManagementService;
  private final ProduceService _produceService;
  private final ConsumeService _consumeService;
  private final String _name;

  public MultiClusterMonitor(Config serviceConfig, String name) throws Exception {
    _name = name;
    MultiClusterMonitorConfig config = new MultiClusterMonitorConfig(serviceConfig);
    _topicManagementService = new MultiClusterTopicManagementService(createMultiClusterTopicManagementServiceProps(serviceConfig, config), name);
    _produceService = new ProduceService(createProduceServiceProps(serviceConfig, config), name);
    _consumeService = new ConsumeService(createConsumeServiceProps(serviceConfig, config), name);
  }

  @SuppressWarnings("unchecked")
  private Config createProduceServiceProps(Config serviceConfig, MultiClusterMonitorConfig config) {
    Config produceServiceConfig = serviceConfig.hasPath(MultiClusterMonitorConfig.PRODUCE_SERVICE_CONFIG)
        ? serviceConfig.getConfig(MultiClusterMonitorConfig.PRODUCE_SERVICE_CONFIG) : ConfigFactory.empty();
    
    return produceServiceConfig.withValue(MultiClusterMonitorConfig.TOPIC_CONFIG, ConfigValueFactory.fromAnyRef(config.getString(MultiClusterMonitorConfig.TOPIC_CONFIG)));
  }

  @SuppressWarnings("unchecked")
  private Config createConsumeServiceProps(Config serviceConfig, MultiClusterMonitorConfig config) {
    Config consumeServiceConfig = serviceConfig.hasPath(MultiClusterMonitorConfig.CONSUME_SERVICE_CONFIG)
        ? serviceConfig.getConfig(MultiClusterMonitorConfig.CONSUME_SERVICE_CONFIG) : ConfigFactory.empty();
    return consumeServiceConfig.withValue(MultiClusterMonitorConfig.TOPIC_CONFIG, ConfigValueFactory.fromAnyRef(config.getString(MultiClusterMonitorConfig.TOPIC_CONFIG)));
  }

  @SuppressWarnings("unchecked")
  private Config createMultiClusterTopicManagementServiceProps(Config serviceConfig, MultiClusterMonitorConfig config) {
    return ConfigFactory.empty()
        .withValue(MultiClusterMonitorConfig.TOPIC_MANAGEMENT_SERVICE_CONFIG, serviceConfig.getValue(MultiClusterMonitorConfig.TOPIC_MANAGEMENT_SERVICE_CONFIG))
        .withValue(MultiClusterMonitorConfig.TOPIC_CONFIG, ConfigValueFactory.fromAnyRef(config.getString(MultiClusterMonitorConfig.TOPIC_CONFIG)));
  }

  @Override
  public void start() {
    _topicManagementService.start();
    _produceService.start();
    _consumeService.start();
    LOG.info(_name + "/MultiClusterMonitor started");
  }

  @Override
  public void stop() {
    _topicManagementService.stop();
    _produceService.stop();
    _consumeService.stop();
    LOG.info(_name + "/MultiClusterMonitor stopped");
  }

  @Override
  public boolean isRunning() {
    return _topicManagementService.isRunning() && _produceService.isRunning() && _consumeService.isRunning();
  }

  @Override
  public void awaitShutdown() {
    _topicManagementService.awaitShutdown();
    _produceService.awaitShutdown();
    _consumeService.awaitShutdown();
  }
}
