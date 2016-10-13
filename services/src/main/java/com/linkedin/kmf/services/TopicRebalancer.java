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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * Runs this periodically to rebalance the monitored topic across brokers and to reassign leaders to brokers so that the
 * monitored topic is sampling all the brokers evenly.
 */
class TopicRebalancer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(TopicRebalancer.class);

  /**
   * The state of the monitored topic.
   */
  enum RebalanceCondition {
    OK,
    PartitionsLow,
    BrokerUnderMonitored,
    BrokerNotLeader;
  }

  interface PartitionsAddedCallback {
    /**
     * This is called when partitions have been added to the monitored topic.
     * @param addedPartitionCount a positive integer, the number of partitions added
     */
    void partitionsAdded(int addedPartitionCount);
  }

  private final double _rebalanceThreshold;
  private final String _topic;
  private final String _zkConnect;
  private final PartitionsAddedCallback _addPartitionCallback;
  private final int _rebalancePartitionFactor;
  private final int _scheduleDurationMs;
  private ZkUtils _zkUtils;
  private final int _replicationFactor;

  /**
   *
   * @param rebalanceThreshold This assumes that you want to have at least one per broker.  When the number of partitions
   *                            falls below rebalanceThreshold * brokerCount then new partitions are created.
   * @param rebalancePartitionFactor While rebalanceThreshold establishes a low water mark this parameter establishes
   *                                   the desired state.  The number of partitions should be
   *                                   rebalancePartitionFactor * brokerCount.
   * @param topic Topic identifier
   * @param zkConnect Zoo keeper connection url
   * @param addPartitionCallback This gets called when partitions have been added to the monitored topic.
   * @param scheduleDurationMs The duration between times this is scheduled to run in milliseconds.
   * @param replicationFactor The desired replication factor when creating new partitions.
   */
  TopicRebalancer(double rebalanceThreshold, int rebalancePartitionFactor, String topic, String zkConnect,
      PartitionsAddedCallback addPartitionCallback, int scheduleDurationMs,
      int replicationFactor) {

    if (rebalanceThreshold < 1) {
      throw new IllegalArgumentException(
          "rebalanceThreshold must be greater than or equal to one, but found " + rebalanceThreshold + ".");
    }

    if (addPartitionCallback == null) {
      throw new NullPointerException("addPartitionCallback may not be null");
    }

    //TODO:  should there be more parameter checking or should I assume that ProduceService has done this.

    _rebalanceThreshold = rebalanceThreshold;
    _topic = topic;
    _zkConnect = zkConnect;
    _addPartitionCallback = addPartitionCallback;
    _rebalancePartitionFactor = rebalancePartitionFactor;
    _scheduleDurationMs = scheduleDurationMs;
    _replicationFactor = replicationFactor;
  }

  int scheduleDurationMs() {
    return _scheduleDurationMs;
  }

  @Override
  public void run() {
    try {
      RebalanceCondition rebalanceCondition = monitoredTopicNeedsRebalance();
      if (rebalanceCondition != RebalanceCondition.OK) {
        LOG.info("Monitored topic \"" + _topic + "\" rebalance started.");
        rebalanceMonitoredTopic(rebalanceCondition);
        LOG.info("Monitored topic \"" + _topic + "\" rebalance complete.");
      }

    } catch (Exception e) {
      LOG.error("Monitored topic rebalance failed with exception.", e);
    }
  }

  /**
   * <pre>
   * Each time this is invoked one of the following happens.  This is done because I don't have a way to wait for any
   * of these steps to complete.
   *
   * 1. if number of partitions falls below threshold then create new partitions
   *     create new produce runnables (callback does this)
   *     create new metrics (callback does this)
   *    increment _PartitionNum (callback does this)
   * 2. This is done during rebalance.
   *   if a broker does not have enough partitions then assign it partitions from brokers that have excess
   *   if a broker is not a leader of some partition then make it a leader of some partition by finding out
   *     which broker is a leader of more than one partition.  Does this involve moving partitions among brokers?
   * 3. run a preferred replica election
   * </pre>
   */
  void rebalanceMonitoredTopic(RebalanceCondition rebalanceCondition) {
    ZkUtils zkUtils = createZooKeeperUtils();
    try {
      Collection<Broker> brokers = scala.collection.JavaConversions.asJavaCollection(zkUtils.getAllBrokersInCluster());
      List<PartitionInfo> partitionInfoList = partitionInfoForMonitoredTopic();
      int brokerCount = brokers.size();
      int partitionCount = partitionInfoList.size();

      LOG.debug("Broker count " + brokerCount + " partitionCount " + partitionCount + ".");

      switch (rebalanceCondition) {
        case PartitionsLow:
          int idealPartitionCount = _rebalancePartitionFactor * brokerCount;
          int addPartitionCount = idealPartitionCount - partitionCount;
          LOG.info("Adding " + addPartitionCount + " partitions.");
          addPartitions(zkUtils, idealPartitionCount);
          waitForAddedPartitionsToBecomeActive(idealPartitionCount);
          _addPartitionCallback.partitionsAdded(addPartitionCount);
          //fall through
        case BrokerUnderMonitored:
          LOG.info("Rebalancing monitored topic.");
          waitForOtherAssignmentsToComplete();
          reassignPartitions(brokers, partitionCount);
          waitForPartitionReassignmentToComplete();
          break;
        case BrokerNotLeader:
          LOG.info("Running preferred replica election.");
          runPreferredElection(zkUtils, partitionInfoList);
          break;
        case OK:
          break;
        default:
          throw new IllegalStateException("Unhandled state " + rebalanceCondition + ".");
      }
    } catch (InterruptedException ie) {
      throw new IllegalStateException(ie);
    }
  }

  /**
   *  Polling...
   */
  private void waitForAddedPartitionsToBecomeActive(int expectedNumberOfPartitions) {
    long timeout = System.currentTimeMillis() + 1000 * 60;

    //Using ZkUtils instead of the consumer because the consumer seems to cache the last answer it got which was the old
    //number of partitions.
    ZkUtils zkUtils = createZooKeeperUtils();
    scala.collection.mutable.ArrayBuffer<String> scalaTopic = new scala.collection.mutable.ArrayBuffer<>();
    scalaTopic.$plus$eq(_topic);
    while (System.currentTimeMillis() < timeout) {
      if (zkUtils.getPartitionAssignmentForTopics(scalaTopic).apply(_topic).size()  == expectedNumberOfPartitions) {
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
   * If there is some other assignment going then wait for it to complete.
   */
  private void waitForOtherAssignmentsToComplete() throws InterruptException {
    ZkUtils zkUtils = createZooKeeperUtils();
    while (!zkUtils.getPartitionsBeingReassigned().isEmpty()) {
      try {
        LOG.debug("Waiting for current partition assignment to be complete.");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private void waitForPartitionReassignmentToComplete() throws InterruptedException {
    ZkUtils zkUtils = createZooKeeperUtils();
    boolean reassignmentRunning = false;
    while (reassignmentRunning) {
      LOG.debug("Wait for monitored topic " + _topic + " to complete reassignment.");
      Thread.sleep(10000);
      scala.collection.Map<TopicAndPartition, ?> currentState = zkUtils.getPartitionsBeingReassigned();
      scala.collection.Iterator<TopicAndPartition> it = currentState.keysIterator();
      reassignmentRunning = false;
      while (it.hasNext()) {
        TopicAndPartition topicAndPartition = it.next();
        if (topicAndPartition.topic().equals(_topic)) {
          reassignmentRunning = true;
          break;
        }
      }
    }
  }

  private void reassignPartitions(Collection<Broker> brokers, int partitionCount) {
    scala.collection.mutable.ArrayBuffer<Object> brokersAsInt = new scala.collection.mutable.ArrayBuffer<>(brokers.size());
    for (Broker broker : brokers) {
      brokersAsInt.$plus$eq(broker.id());
    }

    scala.collection.Map<Object, Seq<Object>> partitionToReplicas =
        AdminUtils.assignReplicasToBrokers(brokersAsInt, partitionCount, _replicationFactor, 0, 0);

    String jsonReassignmentData = scalaReassignmentToJson(partitionToReplicas, partitionCount);

    LOG.debug("Reassignments " + jsonReassignmentData + ".");
    ZkUtils zkUtils = createZooKeeperUtils();
    zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath().toString(), jsonReassignmentData, zkUtils.DefaultAcls());

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
  protected void runPreferredElection(ZkUtils zkUtils, List<PartitionInfo> partitionInfoList) {
    scala.collection.mutable.HashSet<TopicAndPartition> scalaPartitionInfoSet = new scala.collection.mutable.HashSet<>();
    for (PartitionInfo javaPartitionInfo : partitionInfoList) {
      scalaPartitionInfoSet.add(new TopicAndPartition(_topic, javaPartitionInfo.partition()));
    }
    PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkUtils, scalaPartitionInfoSet);
  }

  protected void addPartitions(ZkUtils zkUtils, int addPartitionCount) {
    AdminUtils.addPartitions(zkUtils, _topic, addPartitionCount, null, false);
  }

  protected ZkUtils createZooKeeperUtils() {
    if (_zkUtils == null) {
      _zkUtils = ZkUtils.apply(_zkConnect, ZK_SESSION_TIMEOUT_MS + _scheduleDurationMs, ZK_CONNECTION_TIMEOUT_MS + _scheduleDurationMs,
          JaasUtils.isZkSecurityEnabled());
    }
    return _zkUtils;
  }


  /**
   * @return a value other than RebalanceCondition.OK if the monitored topic is no longer distributed evenly with respect
   * to the given parameters.   Will not return null.
   */
  private RebalanceCondition monitoredTopicNeedsRebalance() {

    ZkUtils zkUtils = createZooKeeperUtils();
    List<PartitionInfo> partitionInfoList = partitionInfoForMonitoredTopic();
    Collection<Broker> brokers = scala.collection.JavaConversions.asJavaCollection(zkUtils.getAllBrokersInCluster());
    return monitoredTopicNeedsRebalance(partitionInfoList, brokers, _rebalanceThreshold);
  }


  private List<PartitionInfo> partitionInfoForMonitoredTopic() {
    ZkUtils zkUtils = createZooKeeperUtils();
    scala.collection.mutable.ArrayBuffer<String> topicList = new scala.collection.mutable.ArrayBuffer<>();
    topicList.$plus$eq(_topic);
    scala.collection.Map<Object, scala.collection.Seq<Object>> partitionAssignments =
        zkUtils.getPartitionAssignmentForTopics(topicList).apply(_topic);
    List<PartitionInfo> partitionInfoList = new ArrayList<>();
    scala.collection.Iterator<scala.Tuple2<Object, scala.collection.Seq<Object>>> it = partitionAssignments.iterator();
    while (it.hasNext()) {
      scala.Tuple2<Object, scala.collection.Seq<Object>> scalaTuple = it.next();
      Integer partition = (Integer) scalaTuple._1();
      Node leader = new Node((Integer) zkUtils.getLeaderForPartition(_topic, partition).get(), "", -1);
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
   * This returns RebalanceCondition.OK unless one of the following conditions are met in this order:
   * <ul>
   *   <li> The minimum number of total monitored partitions falls below floor(brokers * partitionThreshold).
   *        Returns PartitionsLow</li>
   *   <li> One or more brokers does not have a monitored partition or falls below the minimum number of monitored
   *        partitions. Returns BrokerUnderMonitored</li>
   *   <li> One or more brokers is not assigned as a preferred leader for any partition.  BrokerUnderMonitored</li>
   *   <li> One or more brokers is the currently elected leader of a monitored partition. BrokerNotLeader</li>
   * </ul>
   * @param partitionInfoList this should only contain partition info for the monitored topic
   * @param brokers get this from ZkUtils, this should be all the active brokers in the cluster
   * @param partitionThreshold the lower water mark for when we do not have enough monitored partitions
   * @return see above
   */
  static RebalanceCondition monitoredTopicNeedsRebalance(List<PartitionInfo> partitionInfoList, Collection<Broker> brokers,
      double partitionThreshold) {

    int partitionCount = partitionInfoList.size();
    int minPartitionCount = (int) (brokers.size() * partitionThreshold);
    int minPartitionCountPerBroker = (int) partitionThreshold;

    if (partitionCount < minPartitionCount) {
      return RebalanceCondition.PartitionsLow;
    }

    Map<Integer, Integer> brokerToPartitionCount = new HashMap<>(brokers.size());
    //Assigned leaders tracks if there is some partition for which it is the preferred leader which could be none.
    Set<Integer> assignedLeaders = new HashSet<>(brokers.size());
    Set<Integer> leaders = new HashSet<>(brokers.size());

    // Count the number of partitions a broker is involved with and if it is a leader for some partition
    // Check that a partition has at least a certain number of replicas
    for (PartitionInfo partitionInfo : partitionInfoList) {

      for (Node node : partitionInfo.replicas()) {
        int broker = node.id();
        if (!brokerToPartitionCount.containsKey(broker)) {
          brokerToPartitionCount.put(broker, 0);
        }
        int count = brokerToPartitionCount.get(broker);
        brokerToPartitionCount.put(broker, count + 1);
      }

      if (partitionInfo.replicas().length > 0) {
        assignedLeaders.add(partitionInfo.replicas()[0].id());
      }
      if (partitionInfo.leader() != null) {
        leaders.add(partitionInfo.leader().id());
      }
    }

    // Check that a broker is a leader for at least one partition
    // Check that a broker has at least minPartitionCountPerBroker
    for (Broker broker : brokers) {
      if (!brokerToPartitionCount.containsKey(broker.id())) {
        return RebalanceCondition.BrokerUnderMonitored;
      }

      if (brokerToPartitionCount.get(broker.id()) < minPartitionCountPerBroker) {
        return RebalanceCondition.BrokerUnderMonitored;
      }

      if (!assignedLeaders.contains(broker.id())) {
        return RebalanceCondition.BrokerUnderMonitored;
      }
    }

    for (Broker broker : brokers) {
      if (!leaders.contains(broker.id())) {
        return RebalanceCondition.BrokerNotLeader;
      }
    }
    return RebalanceCondition.OK;
  }
}