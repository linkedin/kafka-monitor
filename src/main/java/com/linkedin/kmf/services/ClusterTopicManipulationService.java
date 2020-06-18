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
import com.linkedin.kmf.services.metrics.ClusterTopicManipulationMetrics;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
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
  private final Duration _reportIntervalSecond;
  private final ScheduledExecutorService _executor;
  private final AdminClient _adminClient;
  private boolean _isOngoingTopicCreationDone;
  private final AtomicBoolean _running;
  private String _currentlyOngoingTopic;
  int _expectedPartitionsCount;
  // TODO -- ClusterTopicManipulationMetrics implementation in progress!
  private ClusterTopicManipulationMetrics _clusterTopicManipulationMetrics;

  public ClusterTopicManipulationService(String name, AdminClient adminClient) {
    LOGGER.info("ClusterTopicManipulationService constructor initiated {}", this.getClass().getName());

    _isOngoingTopicCreationDone = true;
    _adminClient = adminClient;
    _executor = Executors.newSingleThreadScheduledExecutor();
    _reportIntervalSecond = Duration.ofSeconds(1);
    _running = new AtomicBoolean(false);
    _configDefinedServiceName = name;
    // TODO: instantiate a new instance of ClusterTopicManipulationMetrics(..) here.
  }

  /**
   * The start logic must only execute once.  If an error occurs then the implementer of this class must assume that
   * stop() will be called to clean up.  This method must be thread safe and must assume that stop() may be called
   * concurrently. This can happen if the monitoring application's life cycle is being managed by a container.  Start
   * will only be called once.
   */
  @Override
  public void start() {
    if (_running.compareAndSet(false, true)) {
      LOGGER.info("ClusterTopicManipulationService started for {} - {}", _configDefinedServiceName,
          this.getClass().getCanonicalName());
      Runnable clusterTopicManipulationServiceRunnable = new ClusterTopicManipulationServiceRunnable();

      _executor.scheduleAtFixedRate(clusterTopicManipulationServiceRunnable, _reportIntervalSecond.getSeconds(),
          _reportIntervalSecond.getSeconds(), TimeUnit.SECONDS);
    }
  }

  private class ClusterTopicManipulationServiceRunnable implements Runnable {

    private ClusterTopicManipulationServiceRunnable() {
      // unaccessed.
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see     Thread#run()
     */
    @Override
    public void run() {
      try {
        ClusterTopicManipulationService.this.createDeleteClusterTopic();
      } catch (Exception e) {
        LOGGER.error("{} {} failed to run createDeleteClusterTopic()", _configDefinedServiceName,
            ClusterTopicManipulationService.this.getClass().getSimpleName(), e);
      }
    }
  }

  /**
   * 1 - Iterates through all the brokers in the cluster.
   * 2 - checks the individual log directories of each broker
   * 3 - checks how many topic partition of the ongoing topic there are and compares it against the expected value
   * The RF is set to the brokerCount currently to enable maximize assigning the many
   * partitions and replicas across all the brokers in the clusters as possible.
   */
  private void createDeleteClusterTopic() {

    if (_isOngoingTopicCreationDone) {

      int random = ThreadLocalRandom.current().nextInt();
      _currentlyOngoingTopic = XinfraMonitorConstants.TOPIC_MANIPULATION_SERVICE_TOPIC + Math.abs(random);

      try {
        int brokerCount = _adminClient.describeCluster().nodes().get().size();
        CreateTopicsResult createTopicsResult = _adminClient.createTopics(Collections.singleton(
            new NewTopic(_currentlyOngoingTopic, XinfraMonitorConstants.TOPIC_MANIPULATION_TOPIC_NUM_PARTITIONS,
                (short) brokerCount)));
        createTopicsResult.all().get();
        _expectedPartitionsCount = brokerCount * XinfraMonitorConstants.TOPIC_MANIPULATION_TOPIC_NUM_PARTITIONS;
        _isOngoingTopicCreationDone = false;
        LOGGER.debug("Initiated a new topic creation. topic information - topic: {}, cluster broker count: {}",
            _currentlyOngoingTopic, brokerCount);
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error("Exception occurred while retrieving the brokers count: ", e);
      }
    }

    try {
      LOGGER.trace("cluster id: {}", _adminClient.describeCluster().clusterId().get());
      Collection<Node> brokers = _adminClient.describeCluster().nodes().get();
      if (this.doesClusterContainTopic(_currentlyOngoingTopic, brokers, _adminClient, _expectedPartitionsCount)) {
        _adminClient.deleteTopics(Collections.singleton(_currentlyOngoingTopic)).all();
        LOGGER.debug("clusterTopicManipulationServiceRunnable: Initiated topic deletion on {}.",
            _currentlyOngoingTopic);
        _isOngoingTopicCreationDone = true;
        LOGGER.trace("{}-clusterTopicManipulationServiceRunnable successful!", this.getClass().getSimpleName());
      }
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Exception occurred while creating cluster topic in {}: ", _configDefinedServiceName, e);
    }
  }

  /**
   * for all brokers, checks if the topic exists in the cluster by iterating through the log dirs of individual brokers.
   * @param topic current ongoing topic
   * @param brokers brokers to check log dirs from
   * @param adminClient Admin Client
   * @return true if the cluster contains the topic.
   * @throws ExecutionException when attempting to retrieve the result of a task
   * that aborted by throwing an exception.
   * @throws InterruptedException when a thread is waiting, sleeping, or occupied,
   * and the thread is interrupted, either before or during the activity.
   */
  private boolean doesClusterContainTopic(String topic, Collection<Node> brokers, AdminClient adminClient, int expected)
      throws ExecutionException, InterruptedException {
    int totalPartitionsInCluster = 0;
    for (Node broker : brokers) {
      LOGGER.trace("broker log directories: {}",
          adminClient.describeLogDirs(Collections.singleton(broker.id())).all().get());
      Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> logDirectoriesResponseMap =
          adminClient.describeLogDirs(Collections.singleton(broker.id())).all().get();

      totalPartitionsInCluster += this.processBroker(logDirectoriesResponseMap, broker, topic);
    }
    return totalPartitionsInCluster == expected && ClusterTopicManipulationService.isTopicDescribeSuccessful(
        adminClient, Collections.singleton(topic));
  }

  /**
   * Waits if necessary for this future to complete, and then returns its result.
   * returns true if the future succeeds, which occurs only if all the topic descriptions are successful.
   * @param adminClient administrative client for Kafka, supporting managing and inspecting topics, brokers, configurations and ACLs.
   * @param topicNames Collection of topic names
   * @return boolean value if describe topic succeeds.
   */
  private static boolean isTopicDescribeSuccessful(AdminClient adminClient, Collection<String> topicNames) {
    KafkaFuture<Map<String, TopicDescription>> mapKafkaFuture = adminClient.describeTopics(topicNames).all();
    try {
      Map<String, TopicDescription> topicDescriptionMap = mapKafkaFuture.get();
      LOGGER.trace("topicDescriptionMap = {}", topicDescriptionMap);
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Exception occurred within describeTopicsFinished method for topics {}", topicNames, e);
      return false;
    }

    return true;
  }

  /**
   * iterates through the broker's log directories and checks for the ongoing topic partitions and replica's existence.
   * @param logDirectoriesResponseMap map of log directories response in the broker
   * @param broker broker to process the log dirs in
   * @param topic ongoing kmf manipulation topic
   */
  int processBroker(Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> logDirectoriesResponseMap,
      Node broker, String topic) {
    int totalPartitionsInBroker = 0;
    LOGGER.trace("logDirectoriesResponseMap: {}", logDirectoriesResponseMap);
    Map<String, DescribeLogDirsResponse.LogDirInfo> logDirInfoMap = logDirectoriesResponseMap.get(broker.id());
    String logDirectoriesKey = logDirInfoMap.keySet().iterator().next();
    LOGGER.trace("logDirInfoMap: {}", logDirInfoMap.get(logDirectoriesKey));
    DescribeLogDirsResponse.LogDirInfo logDirInfo = logDirInfoMap.get(logDirectoriesKey);

    if (logDirInfo != null && !logDirectoriesResponseMap.isEmpty()) {
      Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> topicPartitionReplicaInfoMap = logDirInfo.replicaInfos;
      totalPartitionsInBroker += this.processLogDirsWithinBroker(topicPartitionReplicaInfoMap, topic, broker);
    }

    return totalPartitionsInBroker;
  }

  private int processLogDirsWithinBroker(
      Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> topicPartitionReplicaInfoMap, String topic,
      Node broker) {
    int totalPartitionsInBroker = 0;
    for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> topicPartitionReplicaInfoEntry : topicPartitionReplicaInfoMap
        .entrySet()) {

      TopicPartition topicPartition = topicPartitionReplicaInfoEntry.getKey();
      DescribeLogDirsResponse.ReplicaInfo replicaInfo = topicPartitionReplicaInfoEntry.getValue();

      if (topicPartition.topic().equals(topic)) {
        totalPartitionsInBroker++;
        LOGGER.trace("totalPartitions In The Broker = {}", totalPartitionsInBroker);
      }

      LOGGER.trace("broker information: {}", broker);
      LOGGER.trace("logDirInfo for kafka-logs: topicPartition = {}, replicaInfo = {}", topicPartition, replicaInfo);
    }

    return totalPartitionsInBroker;
  }

  /**
   * This may be called multiple times.  This method must be thread safe and must assume that start() may be called
   * concurrently.  This can happen if the monitoring application's life cycle is being managed by a container.
   * Implementations must be non-blocking and should release the resources acquired by the service during start().
   */
  @Override
  public void stop() {
    if (_running.compareAndSet(true, false)) {
      _executor.shutdown();
    }
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
  public void awaitShutdown(long timeout, TimeUnit timeUnit) {

    try {
      _executor.awaitTermination(3, TimeUnit.MINUTES);
      LOGGER.info("{} shutdown completed", _configDefinedServiceName);
    } catch (InterruptedException e) {
      LOGGER.info("Thread interrupted when waiting for {} to shutdown", _configDefinedServiceName);
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "-" + _configDefinedServiceName;
  }

  void setExpectedPartitionsCount(int count) {
    _expectedPartitionsCount = count;
  }

  int expectedPartitionsCount() {
    return _expectedPartitionsCount;
  }
}
