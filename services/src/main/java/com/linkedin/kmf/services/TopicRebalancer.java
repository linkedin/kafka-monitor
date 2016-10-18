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
 * Runs this periodically to rebalance the monitored topic across brokers and to reassign _electedLeaders to brokers so that the
 * monitored topic is sampling all the brokers evenly.
 */
class TopicRebalancer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(TopicRebalancer.class);

  /**
   * The state of the monitored topic.
   */
  static class TopicState {

    Map<Integer, Integer> _brokerToPartitionCount = new HashMap<>();
    //Assigned _electedLeaders tracks if there is some partition for which it is the preferred leader which could be none.
    Set<Integer> _preferredLeaders = new HashSet<>();
    Set<Integer> _electedLeaders = new HashSet<>();
    Collection<Broker> _allBrokers;
    List<PartitionInfo> _partitionInfo;

    public TopicState(Map<Integer, Integer> brokerToPartitionCount, Set<Integer> electedLeaders,
      Set<Integer> preferredLeaders, List<PartitionInfo> partitionInfo, Collection<Broker> allBrokers) {
      this._brokerToPartitionCount = brokerToPartitionCount;
      this._preferredLeaders = preferredLeaders;
      this._electedLeaders = electedLeaders;
      this._allBrokers = allBrokers;
      this._partitionInfo = partitionInfo;
    }

    /**
     *
     * @param expectedRatio the expected partition to broker ratio
     * @return true if the current partition to broker ratio is less than the expected ratio
     */
    boolean partitionsLow(double expectedRatio) {
      double actualRatio = ((double) _partitionInfo.size()) / _allBrokers.size();
      return actualRatio < expectedRatio;
    }

    /**
     * @return true if broker is not the preferred leader for at least one partition.
     */
    boolean brokerMissingPartition() {

      for (Broker broker : _allBrokers) {
        if (!_brokerToPartitionCount.containsKey(broker.id())) {
          return true;
        }

        if (!_preferredLeaders.contains(broker.id())) {
          return true;
        }
      }
      return false;
    }

    /**
     * @return true if at least one broker is not elected leader for at least one partition.
     */
    boolean brokerNotElectedLeader() {
      for (Broker broker : _allBrokers) {
        if (!_electedLeaders.contains(broker.id())) {
          return true;
        }
      }
      return false;
    }

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
  private final PartitionsAddedCallback _addPartitionCallback;
  private final int _rebalancePartitionFactor;
  private final int _scheduleIntervalMs;
  private final ZkUtils _zkUtils;
  private final int _replicationFactor;

  /**
   *
   * @param expectedPartitionToBrokerRatio When then number of partitions falls below this threshold then new partitions
   *                                       are created.
   * @param rebalancePartitionFactor While expectedPartitionToBrokerRatio establishes a low water mark this parameter establishes
   *                                   the desired state.  The number of partitions should be
   *                                   rebalancePartitionFactor * brokerCount.
   * @param topic Topic identifier
   * @param zkConnect Zoo keeper connection url
   * @param addPartitionCallback This gets called when partitions have been added to the monitored topic.
   * @param scheduleIntervalMs The duration between times this is scheduled to run in milliseconds.
   * @param replicationFactor The desired replication factor when creating new partitions.
   */
  TopicRebalancer(double expectedPartitionToBrokerRatio, int rebalancePartitionFactor, String topic, String zkConnect,
      PartitionsAddedCallback addPartitionCallback, int scheduleIntervalMs,
      int replicationFactor) {

    _rebalanceThreshold = expectedPartitionToBrokerRatio;
    _topic = topic;
    _addPartitionCallback = addPartitionCallback;
    _rebalancePartitionFactor = rebalancePartitionFactor;
    _scheduleIntervalMs = scheduleIntervalMs;
    _replicationFactor = replicationFactor;
    _zkUtils = ZkUtils.apply(zkConnect, ZK_SESSION_TIMEOUT_MS + _scheduleIntervalMs, ZK_CONNECTION_TIMEOUT_MS + _scheduleIntervalMs,
      JaasUtils.isZkSecurityEnabled());
    LOG.info("Rebalance threshold ratio " + _rebalanceThreshold + " topic " + _topic + " _rebalancePartitionFactor " +
      _rebalancePartitionFactor + " scheduleIntervalMs " + _scheduleIntervalMs + " replication factor " + _replicationFactor + ".");
  }

  int scheduleDurationMs() {
    return _scheduleIntervalMs;
  }

  @Override
  public void run() {
    try {
      TopicState topicState = topicState();
      if (topicState.brokerNotElectedLeader() || topicState.brokerMissingPartition() || topicState.partitionsLow(_rebalanceThreshold)) {
        LOG.info("Topic rebalance started.");
        rebalanceMonitoredTopic(topicState);
        LOG.info("Topic rebalance complete.");
      } else {
        LOG.info("Topic is in good state, no rebalance needed.");
      }
    } catch (Exception e) {
      LOG.error("Monitored topic rebalance failed with exception.", e);
    }
  }

  /**
   * <pre>
   * Each time this is invoked zero or more of the following happens.  T
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
  void rebalanceMonitoredTopic(TopicState topicState) {
    try {
      LOG.debug("Broker count " + topicState._allBrokers.size() + " partitionCount " + topicState._partitionInfo.size() + ".");

      if (topicState.partitionsLow(_rebalanceThreshold)) {
        int idealPartitionCount = _rebalancePartitionFactor * topicState._allBrokers.size();
        int addPartitionCount = idealPartitionCount - topicState._partitionInfo.size();
        LOG.info("Adding " + addPartitionCount + " partitions.");
        addPartitions(_zkUtils, idealPartitionCount);
        waitForAddedPartitionsToBecomeActive(idealPartitionCount);
        _addPartitionCallback.partitionsAdded(addPartitionCount);
        topicState = topicState();
      }
      if (topicState.brokerMissingPartition()) {
        LOG.info("Rebalancing monitored topic.");
        waitForOtherAssignmentsToComplete();
        reassignPartitions(topicState._allBrokers, topicState._partitionInfo.size());
        waitForPartitionReassignmentToComplete();
        topicState = topicState();
      }
      if (topicState.brokerNotElectedLeader()) {
        LOG.info("Running preferred replica election.");
        runPreferredElection(_zkUtils, topicState._partitionInfo);
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
   * If there is some other assignment going then wait for it to complete.
   */
  private void waitForOtherAssignmentsToComplete() throws InterruptException {
    while (!_zkUtils.getPartitionsBeingReassigned().isEmpty()) {
      try {
        LOG.debug("Waiting for current partition assignment to be complete.");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private void waitForPartitionReassignmentToComplete() throws InterruptedException {
    boolean reassignmentRunning = false;
    while (reassignmentRunning) {
      LOG.debug("Wait for monitored topic " + _topic + " to complete reassignment.");
      Thread.sleep(10000);
      scala.collection.Map<TopicAndPartition, ?> currentState = _zkUtils.getPartitionsBeingReassigned();
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

  /**
   * @return the state of the topic e.g. all the partitions and replicas
   */
  private TopicState topicState() {
    List<PartitionInfo> partitionInfoList = partitionInfoForMonitoredTopic();
    Collection<Broker> brokers = scala.collection.JavaConversions.asJavaCollection(_zkUtils.getAllBrokersInCluster());
    return topicState(partitionInfoList, brokers);
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
      Node leader = new Node((Integer) _zkUtils.getLeaderForPartition(_topic, partition).get(), "", -1);
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
  static TopicState topicState(List<PartitionInfo> partitionInfoList, Collection<Broker> brokers) {

    Map<Integer, Integer> brokerToPartitionCount = new HashMap<>(brokers.size());
    //Assigned _electedLeaders tracks if there is some partition for which it is the preferred leader which could be none.
    Set<Integer> preferredLeaders = new HashSet<>(brokers.size());
    Set<Integer> electedLeaders = new HashSet<>(brokers.size());

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
        preferredLeaders.add(partitionInfo.replicas()[0].id());
      }
      if (partitionInfo.leader() != null) {
        electedLeaders.add(partitionInfo.leader().id());
      }
    }

    return new TopicState(brokerToPartitionCount, electedLeaders, preferredLeaders, partitionInfoList, brokers);
  }
}