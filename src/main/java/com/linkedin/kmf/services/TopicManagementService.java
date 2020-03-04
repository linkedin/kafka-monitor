/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * This service periodically checks and rebalances the monitor topic across brokers so that
 * leadership of partitions of the monitor topic is distributed evenly across brokers in the cluster
 */
public class TopicManagementService implements Service {
  private final MultiClusterTopicManagementService _multiClusterTopicManagementService;

  public TopicManagementService(Map<String, Object> props, String serviceName) throws Exception {
    Map<String, Object> serviceProps = createMultiClusterTopicManagementServiceProps(props, serviceName);
    _multiClusterTopicManagementService = new MultiClusterTopicManagementService(serviceProps, serviceName);
  }

  public CompletableFuture<Void> topicPartitionResult() {
    return _multiClusterTopicManagementService.topicPartitionResult();
  }

  /**
   * @param props a map of key/value pair used for configuring TopicManagementService
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
  private Map<String, Object> createMultiClusterTopicManagementServiceProps(Map<String, Object> props, String serviceName) {
    Map<String, Object> propsWithoutTopic = new HashMap<>();
    for (Map.Entry<String, Object> entry: props.entrySet()) {
      if (!entry.getKey().equals(TopicManagementServiceConfig.TOPIC_CONFIG)) {
        propsWithoutTopic.put(entry.getKey(), entry.getValue());
      }
    }
    Map<String, Object> configPerCluster = new HashMap<>();
    configPerCluster.put(serviceName, propsWithoutTopic);
    Map<String, Object> serviceProps = new HashMap<>();
    serviceProps.put(MultiClusterTopicManagementServiceConfig.PROPS_PER_CLUSTER_CONFIG, configPerCluster);
    serviceProps.put(MultiClusterTopicManagementServiceConfig.TOPIC_CONFIG, props.get(TopicManagementServiceConfig.TOPIC_CONFIG));
    if (props.containsKey(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG))
      serviceProps.put(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG, props.get(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG));
    return serviceProps;
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

