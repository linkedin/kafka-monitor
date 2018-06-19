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

import com.linkedin.kmf.common.Utils;
import com.linkedin.kmf.services.configs.CommonServiceConfig;
import com.linkedin.kmf.services.configs.MultiClusterTopicManagementServiceConfig;
import com.linkedin.kmf.services.configs.TopicManagementServiceConfig;
import com.linkedin.kmf.topicfactory.TopicFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import kafka.admin.AdminOperationException;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import kafka.admin.PreferredReplicaLeaderElectionCommand;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import static com.linkedin.kmf.common.Utils.ZK_CONNECTION_TIMEOUT_MS;
import static com.linkedin.kmf.common.Utils.ZK_SESSION_TIMEOUT_MS;

/**
 * This service periodically checks and rebalances the monitor topics across a pipeline of Kafka clusters so that
 * leadership of the partitions of the monitor topic in each cluster is distributed evenly across brokers in the cluster.
 *
 * More specifically, this service may do some or all of the following tasks depending on the config:
 *
 * - Create the monitor topic using the user-specified replication factor and partition number
 * - Increase partition number of the monitor topic if either partitionsToBrokersRatio or minPartitionNum is not satisfied
 * - Increase replication factor of the monitor topic if the user-specified replicationFactor is not satisfied
 * - Reassign partition across brokers to make sure each broker acts as preferred leader of at least one partition of the monitor topic
 * - Trigger preferred leader election to make sure each broker acts as leader of at least one partition of the monitor topic.
 * - Make sure the number of partitions of the monitor topic is same across all monitored custers.
 *
 */
public class MultiClusterTopicManagementService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(MultiClusterTopicManagementService.class);

  private final AtomicBoolean _isRunning = new AtomicBoolean(false);
  private final String _serviceName;
  private final Map<String, TopicManagementHelper> _topicManagementByCluster;
  private final int _scheduleIntervalMs;
  private final ScheduledExecutorService _executor;

  public MultiClusterTopicManagementService(Map<String, Object> props, String serviceName) throws Exception {
    _serviceName = serviceName;
    MultiClusterTopicManagementServiceConfig config = new MultiClusterTopicManagementServiceConfig(props);
    String topic = config.getString(CommonServiceConfig.TOPIC_CONFIG);
    Map<String, Map> propsByCluster = props.containsKey(MultiClusterTopicManagementServiceConfig.PROPS_PER_CLUSTER_CONFIG)
        ? (Map) props.get(MultiClusterTopicManagementServiceConfig.PROPS_PER_CLUSTER_CONFIG) : new HashMap<>();
    _topicManagementByCluster = initializeTopicManagementHelper(propsByCluster, topic);
    _scheduleIntervalMs = config.getInt(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG);
    _executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, _serviceName + "-multi-cluster-topic-management-service");
      }
    });
  }

  private Map<String, TopicManagementHelper> initializeTopicManagementHelper(Map<String, Map> propsByCluster, String topic) throws Exception {
    Map<String, TopicManagementHelper> topicManagementByCluster = new HashMap<>();
    for (Map.Entry<String, Map> entry: propsByCluster.entrySet()) {
      String clusterName = entry.getKey();
      Map serviceProps = entry.getValue();
      if (serviceProps.containsKey(MultiClusterTopicManagementServiceConfig.TOPIC_CONFIG))
        throw new ConfigException("The raw per-cluster config for MultiClusterTopicManagementService must not contain " +
            MultiClusterTopicManagementServiceConfig.TOPIC_CONFIG);
      serviceProps.put(MultiClusterTopicManagementServiceConfig.TOPIC_CONFIG, topic);
      topicManagementByCluster.put(clusterName, new TopicManagementHelper(serviceProps));
    }
    return topicManagementByCluster;
  }

  @Override
  public synchronized void start() {
    if (_isRunning.compareAndSet(false, true)) {
      Runnable r = new TopicManagementRunnable();
      _executor.scheduleWithFixedDelay(r, 0, _scheduleIntervalMs, TimeUnit.MILLISECONDS);
      LOG.info("{}/MultiClusterTopicManagementService started.", _serviceName);
    }
  }

  @Override
  public synchronized void stop() {
    if (_isRunning.compareAndSet(true, false)) {
      _executor.shutdown();
      LOG.info("{}/MultiClusterTopicManagementService stopped.", _serviceName);
    }
  }

  @Override
  public boolean isRunning() {
    return _isRunning.get() && !_executor.isShutdown();
  }

  @Override
  public void awaitShutdown() {
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted when waiting for {}/MultiClusterTopicManagementService to shutdown", _serviceName);
    }
    LOG.info("{}/MultiClusterTopicManagementService shutdown completed", _serviceName);
  }

  private class TopicManagementRunnable implements Runnable {
    @Override
    public void run() {
      try {
        for (TopicManagementHelper helper : _topicManagementByCluster.values()) {
          helper.maybeCreateTopic();
        }

        /*
         * The partition number of the monitor topics should be the minimum partition number that satisifies the following conditions:
         * - partition number of the monitor topics across all monitored clusters should be the same
         * - partitionNum / brokerNum >= user-configured partitionsToBrokersRatio.
         * - partitionNum >= user-configured minPartitionNum
         */

        int minPartitionNum = 0;
        for (TopicManagementHelper helper : _topicManagementByCluster.values()) {
          minPartitionNum = Math.max(minPartitionNum, helper.minPartitionNum());
        }
        for (TopicManagementHelper helper : _topicManagementByCluster.values()) {
          helper.maybeAddPartitions(minPartitionNum);
        }

        for (Map.Entry<String, TopicManagementHelper> entry : _topicManagementByCluster.entrySet()) {
          String clusterName = entry.getKey();
          TopicManagementHelper helper = entry.getValue();
          try {
            helper.maybeReassignPartitionAndElectLeader();
          } catch (IOException | ZkNodeExistsException | AdminOperationException e) {
            LOG.warn(_serviceName + "/MultiClusterTopicManagementService will retry later in cluster " + clusterName, e);
          }
        }
      } catch (Exception e) {
        LOG.error(_serviceName + "/MultiClusterTopicManagementService will stop due to error.", e);
        stop();
      }
    }
  }

  static class TopicManagementHelper {

    private final boolean _topicCreationEnabled;
    private final String _topic;
    private final String _zkConnect;
    private final int _replicationFactor;
    private final double _minPartitionsToBrokersRatio;
    private final int _minPartitionNum;
    private final TopicFactory _topicFactory;
    private final Properties _topicProperties;

    TopicManagementHelper(Map<String, Object> props) throws Exception {
      TopicManagementServiceConfig config = new TopicManagementServiceConfig(props);
      _topicCreationEnabled = config.getBoolean(TopicManagementServiceConfig.TOPIC_CREATION_ENABLED_CONFIG);
      _topic = config.getString(TopicManagementServiceConfig.TOPIC_CONFIG);
      _zkConnect = config.getString(TopicManagementServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
      _replicationFactor = config.getInt(TopicManagementServiceConfig.TOPIC_REPLICATION_FACTOR_CONFIG);
      _minPartitionsToBrokersRatio = config.getDouble(TopicManagementServiceConfig.PARTITIONS_TO_BROKERS_RATIO_CONFIG);
      _minPartitionNum = config.getInt(TopicManagementServiceConfig.MIN_PARTITION_NUM_CONFIG);
      String topicFactoryClassName = config.getString(TopicManagementServiceConfig.TOPIC_FACTORY_CLASS_CONFIG);

      _topicProperties = new Properties();
      if (props.containsKey(TopicManagementServiceConfig.TOPIC_PROPS_CONFIG)) {
        for (Map.Entry<String, Object> entry: ((Map<String, Object>) props.get(TopicManagementServiceConfig.TOPIC_PROPS_CONFIG)).entrySet())
          _topicProperties.put(entry.getKey(), entry.getValue().toString());
      }

      Map topicFactoryConfig = props.containsKey(TopicManagementServiceConfig.TOPIC_FACTORY_PROPS_CONFIG) ?
          (Map) props.get(TopicManagementServiceConfig.TOPIC_FACTORY_PROPS_CONFIG) : new HashMap();
      _topicFactory = (TopicFactory) Class.forName(topicFactoryClassName).getConstructor(Map.class).newInstance(topicFactoryConfig);
    }

    void maybeCreateTopic() throws Exception {
      if (_topicCreationEnabled) {
        _topicFactory.createTopicIfNotExist(_zkConnect, _topic, _replicationFactor, _minPartitionsToBrokersRatio, _topicProperties);
      }
    }

    int minPartitionNum() {
      int brokerCount = Utils.getBrokerCount(_zkConnect);
      return Math.max((int) Math.ceil(_minPartitionsToBrokersRatio * brokerCount), _minPartitionNum);
    }

    void maybeAddPartitions(int minPartitionNum) {
      ZkUtils zkUtils = ZkUtils.apply(_zkConnect, ZK_SESSION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, JaasUtils.isZkSecurityEnabled());
      try {
        scala.collection.Map<Object, scala.collection.Seq<Object>> existingAssignment = getPartitionAssignment(zkUtils, _topic);
        int partitionNum = existingAssignment.size();

        if (partitionNum < minPartitionNum) {
          LOG.info("MultiClusterTopicManagementService will increase partition of the topic {} "
              + "in cluster {} from {} to {}.", _topic, _zkConnect, partitionNum, minPartitionNum);

          scala.Option<scala.collection.Map<java.lang.Object, scala.collection.Seq<java.lang.Object>>> replicaAssignment = scala.Option.apply(null);
          AdminUtils.addPartitions(zkUtils, _topic, existingAssignment, getAllBrokers(zkUtils), minPartitionNum, replicaAssignment, false);
        }
      } finally {
        zkUtils.close();
      }
    }

    void maybeReassignPartitionAndElectLeader() throws Exception {
      ZkUtils zkUtils = ZkUtils.apply(_zkConnect, ZK_SESSION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, JaasUtils.isZkSecurityEnabled());

      try {
        List<PartitionInfo> partitionInfoList = getPartitionInfo(zkUtils, _topic);
        Collection<Broker> brokers = scala.collection.JavaConversions.asJavaCollection(zkUtils.getAllBrokersInCluster());

        if (partitionInfoList.size() == 0)
          throw new IllegalStateException("Topic " + _topic + " does not exist in cluster " + _zkConnect);

        int currentReplicationFactor = getReplicationFactor(partitionInfoList);
        int expectedReplicationFactor = Math.max(currentReplicationFactor, _replicationFactor);

        if (_replicationFactor < currentReplicationFactor)
          LOG.debug("Configured replication factor {} is smaller than the current replication factor {} of the topic {} in cluster {}",
              _replicationFactor, currentReplicationFactor, _topic, _zkConnect);

        if (expectedReplicationFactor > currentReplicationFactor && zkUtils.getPartitionsBeingReassigned().isEmpty()) {
          LOG.info("MultiClusterTopicManagementService will increase the replication factor of the topic {} in cluster {}"
              + "from {} to {}", _topic, _zkConnect, currentReplicationFactor, expectedReplicationFactor);
          reassignPartitions(zkUtils, brokers, _topic, partitionInfoList.size(), expectedReplicationFactor);
        }

        // Update the properties of the monitor topic if any config is different from the user-specified config
        Properties currentProperties = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), _topic);
        Properties expectedProperties = new Properties();
        for (Object key: currentProperties.keySet())
          expectedProperties.put(key, currentProperties.get(key));
        for (Object key: _topicProperties.keySet())
          expectedProperties.put(key, _topicProperties.get(key));

        if (!currentProperties.equals(expectedProperties)) {
          LOG.info("MultiClusterTopicManagementService will overwrite properties of the topic {} "
              + "in cluster {} from {} to {}.", _topic, _zkConnect, currentProperties, expectedProperties);
          AdminUtils.changeTopicConfig(zkUtils, _topic, expectedProperties);
        }

        if (partitionInfoList.size() >= brokers.size() &&
            someBrokerNotPreferredLeader(partitionInfoList, brokers) &&
            zkUtils.getPartitionsBeingReassigned().isEmpty()) {
          LOG.info("MultiClusterTopicManagementService will reassign partitions of the topic {} in cluster {}", _topic, _zkConnect);
          reassignPartitions(zkUtils, brokers, _topic, partitionInfoList.size(), expectedReplicationFactor);
        }

        if (partitionInfoList.size() >= brokers.size() &&
            someBrokerNotElectedLeader(partitionInfoList, brokers)) {
          LOG.info("MultiClusterTopicManagementService will trigger preferred leader election for the topic {} in cluster {}", _topic, _zkConnect);
          triggerPreferredLeaderElection(zkUtils, partitionInfoList);
        }
      } finally {
        zkUtils.close();
      }
    }

    private static void triggerPreferredLeaderElection(ZkUtils zkUtils, List<PartitionInfo> partitionInfoList) {
      scala.collection.mutable.HashSet<TopicAndPartition> scalaPartitionInfoSet = new scala.collection.mutable.HashSet<>();
      for (PartitionInfo javaPartitionInfo : partitionInfoList) {
        scalaPartitionInfoSet.add(new TopicAndPartition(javaPartitionInfo.topic(), javaPartitionInfo.partition()));
      }
      PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkUtils, scalaPartitionInfoSet);
    }

    private static void reassignPartitions(ZkUtils zkUtils, Collection<Broker> brokers, String topic, int partitionCount, int replicationFactor) {
      scala.collection.mutable.ArrayBuffer<BrokerMetadata> brokersMetadata = new scala.collection.mutable.ArrayBuffer<>(brokers.size());
      for (Broker broker : brokers) {
        brokersMetadata.$plus$eq(new BrokerMetadata(broker.id(), broker.rack()));
      }
      scala.collection.Map<Object, Seq<Object>> newAssignment =
          AdminUtils.assignReplicasToBrokers(brokersMetadata, partitionCount, replicationFactor, 0, 0);

      scala.collection.mutable.ArrayBuffer<String> topicList = new scala.collection.mutable.ArrayBuffer<>();
      topicList.$plus$eq(topic);
      scala.collection.Map<Object, scala.collection.Seq<Object>> currentAssignment = zkUtils.getPartitionAssignmentForTopics(topicList).apply(topic);
      String currentAssignmentJson = formatAsReassignmentJson(topic, currentAssignment);
      String newAssignmentJson = formatAsReassignmentJson(topic, newAssignment);

      LOG.info("Reassign partitions for topic " + topic);
      LOG.info("Current partition replica assignment " + currentAssignmentJson);
      LOG.info("New partition replica assignment " + newAssignmentJson);
      zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath(), newAssignmentJson, zkUtils.DefaultAcls());
    }

    private static scala.collection.mutable.ArrayBuffer<BrokerMetadata> getAllBrokers(ZkUtils zkUtils) {
      Collection<Broker> brokers = scala.collection.JavaConversions.asJavaCollection(zkUtils.getAllBrokersInCluster());
      scala.collection.mutable.ArrayBuffer<BrokerMetadata> brokersMetadata = new scala.collection.mutable.ArrayBuffer<>(brokers.size());
      for (Broker broker : brokers) {
        brokersMetadata.$plus$eq(new BrokerMetadata(broker.id(), broker.rack()));
      }
      return brokersMetadata;
    }

    private static scala.collection.Map<Object, scala.collection.Seq<Object>> getPartitionAssignment(ZkUtils zkUtils, String topic) {
      scala.collection.mutable.ArrayBuffer<String> topicList = new scala.collection.mutable.ArrayBuffer<>();
      topicList.$plus$eq(topic);
      scala.collection.Map<Object, scala.collection.Seq<Object>> partitionAssignment =
          zkUtils.getPartitionAssignmentForTopics(topicList).apply(topic);
      return partitionAssignment;
    }

    private static List<PartitionInfo> getPartitionInfo(ZkUtils zkUtils, String topic) {
      scala.collection.mutable.ArrayBuffer<String> topicList = new scala.collection.mutable.ArrayBuffer<>();
      topicList.$plus$eq(topic);
      scala.collection.Map<Object, scala.collection.Seq<Object>> partitionAssignments =
          zkUtils.getPartitionAssignmentForTopics(topicList).apply(topic);
      List<PartitionInfo> partitionInfoList = new ArrayList<>();
      scala.collection.Iterator<scala.Tuple2<Object, scala.collection.Seq<Object>>> it = partitionAssignments.iterator();
      while (it.hasNext()) {
        scala.Tuple2<Object, scala.collection.Seq<Object>> scalaTuple = it.next();
        Integer partition = (Integer) scalaTuple._1();
        scala.Option<Object> leaderOption = zkUtils.getLeaderForPartition(topic, partition);
        Node leader = leaderOption.isEmpty() ?  null : new Node((Integer) leaderOption.get(), "", -1);
        Node[] replicas = new Node[scalaTuple._2().size()];
        for (int i = 0; i < replicas.length; i++) {
          Integer brokerId = (Integer) scalaTuple._2().apply(i);
          replicas[i] = new Node(brokerId, "", -1);
        }
        partitionInfoList.add(new PartitionInfo(topic, partition, leader, replicas, null));
      }

      return partitionInfoList;
    }

    static int getReplicationFactor(List<PartitionInfo> partitionInfoList) {
      if (partitionInfoList.isEmpty())
        throw new RuntimeException("Partition list is empty");

      int replicationFactor = partitionInfoList.get(0).replicas().length;
      for (PartitionInfo partitionInfo : partitionInfoList) {
        if (replicationFactor != partitionInfo.replicas().length) {
          String topic = partitionInfoList.get(0).topic();
          LOG.warn("Partitions of the topic " + topic + " have different replication factor");
          return -1;
        }
      }
      return replicationFactor;
    }

    static boolean someBrokerNotPreferredLeader(List<PartitionInfo> partitionInfoList, Collection<Broker> brokers) {
      Set<Integer> brokersNotPreferredLeader = new HashSet<>(brokers.size());
      for (Broker broker: brokers)
        brokersNotPreferredLeader.add(broker.id());
      for (PartitionInfo partitionInfo : partitionInfoList)
        brokersNotPreferredLeader.remove(partitionInfo.replicas()[0].id());

      return !brokersNotPreferredLeader.isEmpty();
    }

    static boolean someBrokerNotElectedLeader(List<PartitionInfo> partitionInfoList, Collection<Broker> brokers) {
      Set<Integer> brokersNotElectedLeader = new HashSet<>(brokers.size());
      for (Broker broker: brokers)
        brokersNotElectedLeader.add(broker.id());
      for (PartitionInfo partitionInfo : partitionInfoList) {
        if (partitionInfo.leader() != null)
          brokersNotElectedLeader.remove(partitionInfo.leader().id());
      }
      return !brokersNotElectedLeader.isEmpty();
    }

    /**
     * @param topic topic
     * @param partitionsToBeReassigned a map from partition (int) to replica list (int seq)
     *
     * @return a json string with the same format as output of kafka.utils.ZkUtils.formatAsReassignmentJson
     *
     * Example:
     * <pre>
     *   {"version":1,"partitions":[
     *     {"topic":"kmf-topic","partition":1,"replicas":[0,1]},
     *     {"topic":"kmf-topic","partition":2,"replicas":[1,2]},
     *     {"topic":"kmf-topic","partition":0,"replicas":[2,0]}]}
     * </pre>
     */
    private static String formatAsReassignmentJson(String topic, scala.collection.Map<Object, Seq<Object>> partitionsToBeReassigned) {
      StringBuilder bldr = new StringBuilder();
      bldr.append("{\"version\":1,\"partitions\":[\n");
      for (int partition = 0; partition < partitionsToBeReassigned.size(); partition++) {
        bldr.append("  {\"topic\":\"").append(topic).append("\",\"partition\":").append(partition).append(",\"replicas\":[");
        scala.collection.Seq<Object> replicas = partitionsToBeReassigned.apply(partition);
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

  }
}

