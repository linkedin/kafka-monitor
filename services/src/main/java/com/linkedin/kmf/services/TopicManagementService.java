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

import com.linkedin.kmf.services.configs.TopicManagementConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import kafka.admin.AdminUtils;
import kafka.admin.PreferredReplicaLeaderElectionCommand;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kmf.common.Utils.ZK_CONNECTION_TIMEOUT_MS;
import static com.linkedin.kmf.common.Utils.ZK_SESSION_TIMEOUT_MS;

import scala.collection.Seq;


/**
 * Runs this periodically to rebalance the monitored topic across brokers and to reassign elected leaders to brokers so that the
 * monitored topic is sampling all the brokers evenly.
 */
public class TopicManagementService implements Service  {

  private static final Logger LOG = LoggerFactory.getLogger(TopicManagementService.class);

  /**
   * The state of the monitored topic.
   */
  static class TopicState {

    final List<PartitionInfo> _partitionInfo;
    //Assigned _electedLeaders tracks if there is some partition for which it is the preferred leader which could be none.
    final Set<Integer> _preferredLeaders;
    final Set<Integer> _electedLeaders;
    final Collection<Broker> _allBrokers;
    final double _expectedRatio;

    /**
     * @param electedLeaders the set of broker ids that have been elected as leaders.
     * @param preferredLeaders the set of brokers that are listed as a preferred leader in at least one partition.
     * @param expectedRatio the expected partition to broker ratio
     */
    TopicState(Set<Integer> electedLeaders, Set<Integer> preferredLeaders, Collection<Broker> allBrokers,
      List<PartitionInfo> partitionInfo, double expectedRatio) {

      this._partitionInfo = partitionInfo;
      this._preferredLeaders = preferredLeaders;
      this._electedLeaders = electedLeaders;
      this._allBrokers = allBrokers;
      this._expectedRatio = expectedRatio;
    }

    /**
     *
     * @return true if the current partition to broker ratio is less than the expected ratio
     */
    boolean insufficientPartitions() {
      double actualRatio = ((double) _partitionInfo.size()) / _allBrokers.size();
      return actualRatio < _expectedRatio;
    }

    /**
     * @return true if broker is not the preferred leader for at least one partition.
     */
    boolean someBrokerMissingPartition() {

      for (Broker broker : _allBrokers) {
        if (!_preferredLeaders.contains(broker.id())) {
          return true;
        }
      }
      return false;
    }

    /**
     * @return true if at least one broker is not elected leader for at least one partition.
     */
    boolean someBrokerWithoutLeader() {
      for (Broker broker : _allBrokers) {
        if (!_electedLeaders.contains(broker.id())) {
          return true;
        }
      }
      return false;
    }

    /**
     * Infers the replication factor of the topic.
     *
     * @return -1 if partitions do not all have the same replication factor possibly indicating that the topic is
     * undergoing maintenance
     */
    int replicationFactor() {
      if (_partitionInfo.isEmpty()) {
        return -1;
      }

      Node[] replicas = _partitionInfo.get(0).replicas();
      if (replicas == null) {
        return -1;
      }

      int replicationFactor = replicas.length;
      for (PartitionInfo partitionInfo : _partitionInfo) {
        if (partitionInfo.replicas() == null) {
          return -1;
        }
        if (replicationFactor != partitionInfo.replicas().length) {
          return -1;
        }
      }

      return replicationFactor;
    }

  }

  /**
   * <pre>
   * Each time this is invoked zero or more of the following happens.  T
   *
   * 1. if number of partitions falls below threshold then create new partitions.  The produce service will need to
   *   detect this this condition and create new metrics for the new partitions.
   * 2. This is done during rebalance.
   *   if a broker does not have enough partitions then assign it partitions from brokers that have excess
   *   if a broker is not a leader of some partition then make it a leader of some partition by finding out
   *     which broker is a leader of more than one partition.  Does this involve moving partitions among brokers?
   * 3. run a preferred replica election
   * </pre>
   */
  private class TopicManagementRunnable implements Runnable {
    @Override
    public void run() {
      try {
        TopicState topicState = topicState();
        if (topicState.someBrokerWithoutLeader() || topicState.someBrokerMissingPartition() || topicState.insufficientPartitions()) {

          LOG.info(_serviceName + ": topic rebalance started.");
          LOG.debug(_serviceName + ": broker count " + topicState._allBrokers.size() + " partitionCount " + topicState._partitionInfo.size() + ".");

          if (topicState.replicationFactor() == -1) {
            LOG.info(_serviceName + " can't determine replication factor of monitored topic " + _topic + ".  Will try rebalance later.");
            return;
          }
          if (topicState.insufficientPartitions()) {
            int idealPartitionCount = _partitionsPerBroker * topicState._allBrokers.size();
            int addPartitionCount = idealPartitionCount - topicState._partitionInfo.size();
            LOG.info(_serviceName + ": adding " + addPartitionCount + " partitions.");
            AdminUtils.addPartitions(_zkUtils, _topic, idealPartitionCount, null, false);
            waitForAddedPartitionsToBecomeActive(idealPartitionCount);
            topicState = topicState();
          }
          if (topicState.someBrokerMissingPartition()) {
            LOG.info(_serviceName + ": rebalancing monitored topic.");
            waitForTopicAssignmentsToComplete();
            reassignPartitions(topicState._allBrokers, topicState._partitionInfo.size(), topicState.replicationFactor());
            waitForTopicAssignmentsToComplete();
            topicState = topicState();
          }
          if (topicState.someBrokerWithoutLeader()) {
            LOG.info(_serviceName + ": running preferred replica election.");
            triggerPreferredLeaderElection(_zkUtils, topicState._partitionInfo);
          }
          LOG.info(_serviceName + ": topic rebalance complete.");
        } else {
          LOG.info(_serviceName + ": topic is in good state, no rebalance needed.");
        }
      } catch (Exception e) {
        if (e instanceof IOException) {
          //Can't do this with catch block because nothing declares IOException although scala code can still throw it.
          LOG.error(_serviceName + ": will retry later due to IOException.", e);
        } else {
          LOG.error(_serviceName + ": monitored topic rebalance failed with exception.  Exiting rebalance service.", e);
          stop();
        }
      }
    }
  }

  private final double _partitionBrokerRatioThreshold;
  private final String _topic;
  private final int _partitionsPerBroker;
  private final int _scheduleIntervalMs;
  private final ZkUtils _zkUtils;
  private final ScheduledExecutorService _executor;
  private volatile boolean _running = false;
  private final String _serviceName;


  public TopicManagementService(Map<String, Object> props, String serviceName) {
    _serviceName = serviceName;
    TopicManagementConfig config = new TopicManagementConfig(props);
    _partitionBrokerRatioThreshold = config.getDouble(TopicManagementConfig.PARTITIONS_PER_BROKER_THRESHOLD);
    _topic = config.getString(TopicManagementConfig.TOPIC_CONFIG);
    _partitionsPerBroker = config.getInt(TopicManagementConfig.PARTITIONS_PER_BROKER_CONFIG);
    _scheduleIntervalMs = config.getInt(TopicManagementConfig.REBALANCE_INTERVAL_MS_CONFIG);
    String zkUrl = config.getString(TopicManagementConfig.ZOOKEEPER_CONNECT_CONFIG);
    _zkUtils = ZkUtils.apply(zkUrl, ZK_SESSION_TIMEOUT_MS + _scheduleIntervalMs, ZK_CONNECTION_TIMEOUT_MS + _scheduleIntervalMs,
      JaasUtils.isZkSecurityEnabled());
    _executor = Executors.newScheduledThreadPool(1);

    LOG.info("Topic management service \"" + _serviceName + "\" constructed with expected partition/broker ratio " + _partitionBrokerRatioThreshold
      + " topic " + _topic + " partitionsPerBroker " + _partitionsPerBroker + " scheduleIntervalMs " +
      _scheduleIntervalMs + ".");
  }


  /**
   *  Polling...
   */
  private void waitForAddedPartitionsToBecomeActive(int expectedNumberOfPartitions) {
    long timeout = System.currentTimeMillis() + 1000 * 60;

    //Using ZkUtils instead of the consumer because the consumer seems to cache the last answer it got which was the old
    //number of partitions.
    scala.collection.mutable.ArrayBuffer<String> scalaTopic = new scala.collection.mutable.ArrayBuffer<>();
    scalaTopic.$plus$eq(_topic);
    while (System.currentTimeMillis() < timeout) {
      if (_zkUtils.getPartitionAssignmentForTopics(scalaTopic).apply(_topic).size()  == expectedNumberOfPartitions) {
        return;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    throw new IllegalStateException("Waiting for additional partitions to appear timed out.");
  }

  /**
   * Wait for all topic assignments to complete.
   */
  private void waitForTopicAssignmentsToComplete() throws InterruptException {
    while (!_zkUtils.getPartitionsBeingReassigned().isEmpty()) {
      try {
        LOG.debug("Waiting for current partition assignment to be complete.");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private void reassignPartitions(Collection<Broker> brokers, int partitionCount, int replicationFactor) {
    scala.collection.mutable.ArrayBuffer<Object> brokersAsInt = new scala.collection.mutable.ArrayBuffer<>(brokers.size());
    for (Broker broker : brokers) {
      brokersAsInt.$plus$eq(broker.id());
    }

    scala.collection.Map<Object, Seq<Object>> partitionToReplicas =
        AdminUtils.assignReplicasToBrokers(brokersAsInt, partitionCount, replicationFactor, 0, 0);

    String jsonReassignmentData = scalaReassignmentToJson(partitionToReplicas, partitionCount);

    LOG.debug(_serviceName + ": reassignments " + jsonReassignmentData + ".");
    _zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath().toString(), jsonReassignmentData, _zkUtils.DefaultAcls());

  }

  /**
   * This should look something like
   * <pre>
   *   {"version":1,"partitions":[
   *     {"topic":"kmf-topic","partition":5,"replicas":[1]},
   *     {"topic":"kmf-topic","partition":4,"replicas":[0]},
   *     {"topic":"kmf-topic","partition":3,"replicas":[2]},
   *     {"topic":"kmf-topic","partition":1,"replicas":[0]},
   *     {"topic":"kmf-topic","partition":2,"replicas":[1]},
   *     {"topic":"kmf-topic","partition":0,"replicas":[2]}]}
   * </pre>
   * @return a json string
   */
  private String scalaReassignmentToJson(scala.collection.Map<Object, Seq<Object>> scalaPartitionToReplicas, int partitionCount) {
    //TODO: Is this the best way to construct this JSON data structure?
    StringBuilder bldr = new StringBuilder();
    bldr.append("{\"version\":1,\"partitions\":[\n");
    for (int partition = 0; partition < partitionCount; partition++) {
      bldr.append("  {\"topic\":\"").append(_topic).append("\",\"partition\":").append(partition).append(",\"replicas\":[");
      scala.collection.Seq<Object> replicas = scalaPartitionToReplicas.apply(partition);
      for (int replicaIndex = 0; replicaIndex < replicas.size(); replicaIndex++) {
        Object replica = replicas.apply(replicaIndex);
        bldr.append(replica).append(",");
      }
      bldr.setLength(bldr.length() - 1);
      bldr.append("]},\n");
    }
    bldr.setLength(bldr.length() - 2);
    bldr.append("]}");
    return bldr.toString();
  }

  /**
   * This runs the preferred replica election for all partitions.
   */
  protected void triggerPreferredLeaderElection(ZkUtils zkUtils, List<PartitionInfo> partitionInfoList) {
    scala.collection.mutable.HashSet<TopicAndPartition> scalaPartitionInfoSet = new scala.collection.mutable.HashSet<>();
    for (PartitionInfo javaPartitionInfo : partitionInfoList) {
      scalaPartitionInfoSet.add(new TopicAndPartition(_topic, javaPartitionInfo.partition()));
    }
    PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkUtils, scalaPartitionInfoSet);
  }

  /**
   * @return the state of the topic e.g. all the partitions and replicas
   */
  private TopicState topicState() {
    List<PartitionInfo> partitionInfoList = partitionInfoForMonitoredTopic();
    Collection<Broker> brokers = scala.collection.JavaConversions.asJavaCollection(_zkUtils.getAllBrokersInCluster());
    return topicState(partitionInfoList, brokers, _partitionBrokerRatioThreshold);
  }


  private List<PartitionInfo> partitionInfoForMonitoredTopic() {
    scala.collection.mutable.ArrayBuffer<String> topicList = new scala.collection.mutable.ArrayBuffer<>();
    topicList.$plus$eq(_topic);
    scala.collection.Map<Object, scala.collection.Seq<Object>> partitionAssignments =
        _zkUtils.getPartitionAssignmentForTopics(topicList).apply(_topic);
    List<PartitionInfo> partitionInfoList = new ArrayList<>();
    scala.collection.Iterator<scala.Tuple2<Object, scala.collection.Seq<Object>>> it = partitionAssignments.iterator();
    while (it.hasNext()) {
      scala.Tuple2<Object, scala.collection.Seq<Object>> scalaTuple = it.next();
      Integer partition = (Integer) scalaTuple._1();
      scala.Option<Object> leaderOption = _zkUtils.getLeaderForPartition(_topic, partition);
      Node leader = leaderOption.isEmpty() ?  null : new Node((Integer) leaderOption.get(), "", -1);
      Node[] replicas = new Node[scalaTuple._2().size()];
      for (int i = 0; i < replicas.length; i++) {
        Integer brokerId = (Integer) scalaTuple._2().apply(i);
        replicas[i] = new Node(brokerId, "", -1);
      }
      partitionInfoList.add(new PartitionInfo(_topic, partition, leader, replicas, null));
    }

    return partitionInfoList;
  }

  /**
   * Create a TopicState instance.
   */
  static TopicState topicState(List<PartitionInfo> partitionInfoList, Collection<Broker> brokers, double partitionBrokerRatioThreshold) {

    //Assigned _electedLeaders tracks if there is some partition for which it is the preferred leader which could be none.
    Set<Integer> preferredLeaders = new HashSet<>(brokers.size());
    Set<Integer> electedLeaders = new HashSet<>(brokers.size());

    // Count the number of partitions a broker is involved with and if it is a leader for some partition
    // Check that a partition has at least a certain number of replicas
    for (PartitionInfo partitionInfo : partitionInfoList) {
      if (partitionInfo.replicas().length > 0) {
        preferredLeaders.add(partitionInfo.replicas()[0].id());
      }
      if (partitionInfo.leader() != null) {
        electedLeaders.add(partitionInfo.leader().id());
      }
    }

    return new TopicState(electedLeaders, preferredLeaders, brokers, partitionInfoList, partitionBrokerRatioThreshold);
  }

  @Override
  public synchronized void start() {
    if (_running) {
      return;
    }
    Runnable r = new TopicManagementRunnable();
    _executor.scheduleWithFixedDelay(r, _scheduleIntervalMs, _scheduleIntervalMs, TimeUnit.MILLISECONDS);
    _running = true;
  }

  @Override
  public synchronized void stop() {
    if (!_running) {
      return;
    }
    _executor.shutdown();
  }

  @Override
  public boolean isRunning() {
    return _running;
  }

  @Override
  public synchronized void awaitShutdown() {
    if (!_running) {
      return;
    }
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Waiting for termination failed.", e);
    }
    _running = false;
  }
}