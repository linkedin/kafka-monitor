/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services;

import com.linkedin.kmf.services.configs.MultiClusterTopicManagementServiceConfig;
import com.linkedin.kmf.services.configs.TopicManagementServiceConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * This service periodically checks and rebalances the monitor topic across brokers so that
 * leadership of partitions of the monitor topic is distributed evenly across brokers in the cluster
 */
public class TopicManagementService implements Service {
  private final MultiClusterTopicManagementService _multiClusterTopicManagementService;

  public TopicManagementService(Config serviceConfig, String serviceName) throws Exception {
    Config config = createMultiClusterTopicManagementServiceProps(serviceConfig, serviceName);
    _multiClusterTopicManagementService = new MultiClusterTopicManagementService(config, serviceName);
  }

  /**
   * @param config a Config of key/value pair used for configuring TopicManagementService
   * @param serviceName service name
   * @return a map of the following format:
   *
   * {
   *   "topic.management.props.per.cluster" : {
   *     // all key/value pair from props except the one with key "topic"
   *   }
   *   "topic": topic
   * }
   */
  private Config createMultiClusterTopicManagementServiceProps(Config config, String serviceName) {
    Config propsWithoutTopic = config.withoutPath(TopicManagementServiceConfig.TOPIC_CONFIG);
    Config configPerCluster = ConfigFactory.empty().withValue(serviceName, propsWithoutTopic.root());
    Config serviceConfig = ConfigFactory.empty()
            .withValue(MultiClusterTopicManagementServiceConfig.PROPS_PER_CLUSTER_CONFIG, configPerCluster.root())
            .withValue(MultiClusterTopicManagementServiceConfig.TOPIC_CONFIG, config.getValue(TopicManagementServiceConfig.TOPIC_CONFIG));
    if (config.hasPath(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG))
      serviceConfig = serviceConfig.withValue(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG, config.getValue(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG));
    return serviceConfig;
  }

  @Override
  public synchronized void start() {
    _multiClusterTopicManagementService.start();
  }

  @Override
  public synchronized void stop() {
    _multiClusterTopicManagementService.stop();
  }

  @Override
  public boolean isRunning() {
    return _multiClusterTopicManagementService.isRunning();
  }

  @Override
  public void awaitShutdown() {
    _multiClusterTopicManagementService.awaitShutdown();
  }
}

