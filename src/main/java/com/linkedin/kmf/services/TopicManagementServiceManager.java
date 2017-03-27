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

import com.linkedin.kmf.apps.configs.MirrorPipelineMonitorConfig;
import com.linkedin.kmf.services.configs.CommonServiceConfig;
import com.linkedin.kmf.services.configs.ConsumeServiceConfig;
import com.linkedin.kmf.services.configs.TopicManagementServiceConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TopicManagementServiceManager implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(TopicManagementServiceManager.class);

  private double _partitionsToBrokerRatio;
  private String _name;
  private List<String> _zookeeperUrls;
  private AtomicBoolean _running;
  private final ScheduledExecutorService _managementExecutor;
  private Map<String, TopicManagementService> _services;

  public TopicManagementServiceManager(Map<String, Object> props, List<String> zookeeperUrls, String topic,
                                        String name) throws Exception {
    Map<String, Object> topicManagementProps = createTopicManagementServiceProps(props, topic);
    topicManagementProps.put(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG, "");
    TopicManagementServiceConfig config = new TopicManagementServiceConfig(topicManagementProps);
    _partitionsToBrokerRatio = config.getDouble(TopicManagementServiceConfig.PARTITIONS_TO_BROKER_RATIO_THRESHOLD);
    _name = name;
    _zookeeperUrls = zookeeperUrls;
    _running = new AtomicBoolean(false);
    _managementExecutor = Executors.newSingleThreadScheduledExecutor(new TopicManagementServiceManagerThreadFactory());
    Map<String, Integer> brokerCounts = getBrokerCounts(zookeeperUrls);
    int maxNumPartitions = getMaxNumberOfPartitions(brokerCounts);

    _services = new HashMap<>();
    for (String zookeeper: _zookeeperUrls) {
      topicManagementProps.put(ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
      double individualPartitionToBrokerRatio = maxNumPartitions / brokerCounts.get(zookeeper);
      topicManagementProps.put(TopicManagementServiceConfig.PARTITIONS_TO_BROKER_RATIO_CONFIG, individualPartitionToBrokerRatio);
      _services.put(zookeeper, new TopicManagementService(topicManagementProps, name + ":" + zookeeper));
    }

    LOG.info(_name + ": topic management service manager is initialized.");
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> createTopicManagementServiceProps(Map<String, Object> props, String topic) {
    Map<String, Object> topicManagementServiceProps = props.containsKey(MirrorPipelineMonitorConfig.TOPIC_MANAGEMENT_SERVICE_CONFIG)
      ? (Map) props.get(MirrorPipelineMonitorConfig.TOPIC_MANAGEMENT_SERVICE_CONFIG) : new HashMap<>();
    topicManagementServiceProps.put(CommonServiceConfig.TOPIC_CONFIG, topic);
    return topicManagementServiceProps;
  }


  public Map<String, Integer> getBrokerCounts(List<String> zookeeperUrls) {
    Map<String, Integer> zookeeperBrokerCount = new HashMap<>();
    for (String zookeeper: zookeeperUrls) {
      if (zookeeperBrokerCount.containsKey(zookeeper)) {
        throw new RuntimeException(_name + ": Duplicate zookeeper connections specified for topic management.");
      }

      int brokerCount = com.linkedin.kmf.common.Utils.getBrokerCount(zookeeper);
      zookeeperBrokerCount.put(zookeeper, brokerCount);
    }

    return zookeeperBrokerCount;
  }

  public int getMaxNumberOfPartitions(Map<String, Integer> brokerCounts) {
    int maxNumPartitions = 0;
    for (String zookeeper: brokerCounts.keySet()) {
      int brokerCount = brokerCounts.get(zookeeper);
      int numPartitions = (int) Math.ceil(_partitionsToBrokerRatio * brokerCount);
      maxNumPartitions = Math.max(maxNumPartitions, numPartitions);
    }

    return maxNumPartitions;
  }

  @Override
  public void start() {
    if (_running.compareAndSet(false, true)) {
      for (TopicManagementService service : _services.values()) {
        service.start();
      }

      _managementExecutor.scheduleWithFixedDelay(new TopicManagementServiceManagerRunnable(), 40_000, 40_000, TimeUnit.MILLISECONDS);
      LOG.info(_name + "/TopicManagementServiceManager started");
    }
  }

  @Override
  public void stop() {
    if (_running.compareAndSet(true, false)) {
      _managementExecutor.shutdown();
      for (TopicManagementService service: _services.values()) {
        service.stop();
      }
      LOG.info(_name + "/TopicManagementServiceManager stopped");
    }
  }

  @Override
  public boolean isRunning() {
    return _running.get();
  }

  @Override
  public void awaitShutdown() {
    try {
      _managementExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
    LOG.info(_name + "/TopicManagementServiceManager shutdown completed");
  }

  private class TopicManagementServiceManagerRunnable implements Runnable {

    @Override
    public void run() {
      Map<String, Integer> brokerCounts = getBrokerCounts(_zookeeperUrls);
      int maxNumPartitions = getMaxNumberOfPartitions(brokerCounts);

      for (String zookeeper: _zookeeperUrls) {
        double individualPartitionToBrokerRatio = maxNumPartitions / brokerCounts.get(zookeeper);
        _services.get(zookeeper).setPartitionsToBrokerRatio(individualPartitionToBrokerRatio);
      }
    }
  }

  private class TopicManagementServiceManagerThreadFactory implements ThreadFactory {
    public Thread newThread(Runnable r) {
      return new Thread(r, _name + "-topic-management-service-manager");
    }
  }
}
