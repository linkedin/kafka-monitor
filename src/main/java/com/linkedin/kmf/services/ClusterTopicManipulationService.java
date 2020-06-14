/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.services;

import com.linkedin.kmf.XinfraMonitorConstants;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Service monitoring the creations and deletions of Kafka Cluster's Topic.
 */
public class ClusterTopicManipulationService implements Service {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTopicManipulationService.class);
  private final String _configDefinedServiceName;
  private final int _reportIntervalSecond;
  private final ScheduledExecutorService _executor;
  private final AdminClient _adminClient;
  private volatile boolean _isOngoingTopicCreationDone;
  private String _currentlyOngoingTopic;
  private CreateTopicsResult _createTopicsResult;

  public ClusterTopicManipulationService(String name, AdminClient adminClient) {
    LOGGER.info("ClusterTopicManipulationService constructor initiated {}", this.getClass().getName());

    _isOngoingTopicCreationDone = true;
    _adminClient = adminClient;
    _executor = Executors.newSingleThreadScheduledExecutor();
    _reportIntervalSecond = 1;
    _configDefinedServiceName = name;
  }

  /**
   * The start logic must only execute once.  If an error occurs then the implementer of this class must assume that
   * stop() will be called to clean up.  This method must be thread safe and must assume that stop() may be called
   * concurrently. This can happen if the monitoring application's life cycle is being managed by a container.  Start
   * will only be called once.
   */
  @Override
  public void start() {
    LOGGER.info("ClusterTopicManipulationService started for {} - {}", _configDefinedServiceName,
        this.getClass().getCanonicalName());
    Runnable clusterTopicManipulationServiceRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          ClusterTopicManipulationService.this.createDeleteClusterTopic();
        } catch (Exception e) {
          LOGGER.error("{} {} failed to run createDeleteClusterTopic()", _configDefinedServiceName,
              ClusterTopicManipulationService.this.getClass().getSimpleName(), e);
        }
      }
    };
    _executor.scheduleAtFixedRate(clusterTopicManipulationServiceRunnable, _reportIntervalSecond, _reportIntervalSecond,
        TimeUnit.SECONDS);
  }

  private void createDeleteClusterTopic() {

    if (_isOngoingTopicCreationDone) {

      int random = ThreadLocalRandom.current().nextInt();
      _currentlyOngoingTopic = "xinfra-monitor-cluster-topic-manipulation-service-topic-" + Math.abs(random);

      try {
        int brokerCount = _adminClient.describeCluster().nodes().get().size();
        _createTopicsResult = _adminClient.createTopics(Collections.singleton(
            new NewTopic(_currentlyOngoingTopic, XinfraMonitorConstants.TOPIC_MANIPULATION_TOPIC_NUM_PARTITIONS,
                (short) brokerCount)));

        _isOngoingTopicCreationDone = false;
        LOGGER.info("Initiated a new topic creation. topic information - topic: {}, cluster broker count: {}",
            _currentlyOngoingTopic, brokerCount);
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error("Exception occurred while retrieving the brokers count: ", e);
      }
    }

    try {
      LOGGER.trace("cluster id: {}", _adminClient.describeCluster().clusterId().get());
      if (_createTopicsResult.all().isDone() && doesClusterContainTopic(_currentlyOngoingTopic)) {
        _adminClient.deleteTopics(Collections.singleton(_currentlyOngoingTopic)).all();
        LOGGER.info("Initiated topic deletion on {}.", _currentlyOngoingTopic);
        _isOngoingTopicCreationDone = true;
      }
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Exception occurred while creating cluster topic in {}: ", _configDefinedServiceName, e);
    }
  }

  private boolean doesClusterContainTopic(String topic) throws ExecutionException, InterruptedException {
    for (Node broker : _adminClient.describeCluster().nodes().get()) {
      LOGGER.trace("broker log directories: {}",
          _adminClient.describeLogDirs(Collections.singleton(broker.id())).all().get());
    }

    for (Node broker : _adminClient.describeCluster().nodes().get()) {
      Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> logDirectoriesResponseMap =
          _adminClient.describeLogDirs(Collections.singleton(broker.id())).all().get();

      Map<String, DescribeLogDirsResponse.LogDirInfo> logDirInfoMap = logDirectoriesResponseMap.get(broker.id());
      DescribeLogDirsResponse.LogDirInfo logDirInfo = logDirInfoMap.get(XinfraMonitorConstants.KAFKA_LOG_DIRECTORY);
      Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> topicPartitionReplicaInfoMap = logDirInfo.replicaInfos;

      if (!doesBrokerContainTopic(topicPartitionReplicaInfoMap, topic, broker)) {
        return false;
      }
    }
    return true;
  }

  private boolean doesBrokerContainTopic(
      Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> topicPartitionReplicaInfoMap, String topic,
      Node broker) {
    for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> topicPartitionReplicaInfoEntry : topicPartitionReplicaInfoMap
        .entrySet()) {

      TopicPartition topicPartition = topicPartitionReplicaInfoEntry.getKey();
      DescribeLogDirsResponse.ReplicaInfo replicaInfo = topicPartitionReplicaInfoEntry.getValue();

      if (topicPartition.topic().equals(topic)) {
        return true;
      }

      LOGGER.debug("broker information: {}", broker);
      LOGGER.trace("logDirInfo for kafka-logs: topicPartition = {}, replicaInfo = {}", topicPartition, replicaInfo);
    }

    return false;
  }

  /**
   * This may be called multiple times.  This method must be thread safe and must assume that start() may be called
   * concurrently.  This can happen if the monitoring application's life cycle is being managed by a container.
   * Implementations must be non-blocking and should release the resources acquired by the service during start().
   */
  @Override
  public void stop() {
    _executor.shutdown();
  }

  /**
   * Implementations of this method must be thread safe as it can be called at any time.  Implementations must be
   * non-blocking.
   * @return true if this start() has returned successfully else this must return false.  This must also return false if
   * the service can no longer perform its function.
   */
  @Override
  public boolean isRunning() {

    return !_executor.isShutdown();
  }

  /**
   * Implementations of this method must be thread safe and must be blocking.
   */
  @Override
  public void awaitShutdown() {

    try {
      _executor.awaitTermination(3, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOGGER.info("Thread interrupted when waiting for {} to shutdown", _configDefinedServiceName);
    }
    LOGGER.info("{} shutdown completed", _configDefinedServiceName);
  }
}
