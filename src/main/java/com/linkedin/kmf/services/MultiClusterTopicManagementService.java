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

import com.linkedin.kmf.services.configs.CommonServiceConfig;
import com.linkedin.kmf.services.configs.MultiClusterTopicManagementServiceConfig;
import com.linkedin.kmf.services.configs.TopicManagementServiceConfig;
import com.linkedin.kmf.topicfactory.TopicFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import kafka.server.ConfigType;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option$;
import scala.collection.Seq;


/**
 * This service periodically checks and re-balances the monitor topics across a pipeline of Kafka clusters so that
 * leadership of the partitions of the monitor topic in each cluster is distributed evenly across brokers in the cluster.
 *
 * More specifically, this service may do some or all of the following tasks depending on the config:
 *
 * - Create the monitor topic using the user-specified replication factor and partition number
 * - Increase partition number of the monitor topic if either partitionsToBrokersRatio or minPartitionNum is not satisfied
 * - Increase replication factor of the monitor topic if the user-specified replicationFactor is not satisfied
 * - Reassign partition across brokers to make sure each broker acts as preferred leader of at least one partition of the monitor topic
 * - Trigger preferred leader election to make sure each broker acts as the leader of at least one partition of the monitor topic.
 * - Make sure the number of partitions of the monitor topic is same across all monitored clusters.
 *
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MultiClusterTopicManagementService implements Service {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiClusterTopicManagementService.class);
  private static final String METRIC_GROUP_NAME = "topic-management-service";
  private final CompletableFuture<Void> _topicPartitionResult = new CompletableFuture<>();
  private final AtomicBoolean _isRunning = new AtomicBoolean(false);
  private final String _serviceName;
  private final Map<String, TopicManagementHelper> _topicManagementByCluster;
  private final int _scheduleIntervalMs;
  private final long _preferredLeaderElectionIntervalMs;
  private final ScheduledExecutorService _executor;


  @SuppressWarnings("unchecked")
  public MultiClusterTopicManagementService(Map<String, Object> props, String serviceName) throws Exception {
    _serviceName = serviceName;
    MultiClusterTopicManagementServiceConfig config = new MultiClusterTopicManagementServiceConfig(props);
    String topic = config.getString(CommonServiceConfig.TOPIC_CONFIG);
    Map<String, Map> propsByCluster = props.containsKey(MultiClusterTopicManagementServiceConfig.PROPS_PER_CLUSTER_CONFIG)
        ? (Map) props.get(MultiClusterTopicManagementServiceConfig.PROPS_PER_CLUSTER_CONFIG) : new HashMap<>();
    _topicManagementByCluster = initializeTopicManagementHelper(propsByCluster, topic);
    _scheduleIntervalMs = config.getInt(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG);
    _preferredLeaderElectionIntervalMs = config.getLong(MultiClusterTopicManagementServiceConfig.PREFERRED_LEADER_ELECTION_CHECK_INTERVAL_MS_CONFIG);
    _executor = Executors.newSingleThreadScheduledExecutor(
      r -> new Thread(r, _serviceName + "-multi-cluster-topic-management-service"));
    _topicPartitionResult.complete(null);
  }

  public CompletableFuture<Void> topicPartitionResult() {
    return _topicPartitionResult;
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
      Runnable tmRunnable = new TopicManagementRunnable();
      _executor.scheduleWithFixedDelay(tmRunnable, 0, _scheduleIntervalMs, TimeUnit.MILLISECONDS);

      Runnable pleRunnable = new PreferredLeaderElectionRunnable();
      _executor.scheduleWithFixedDelay(pleRunnable, _preferredLeaderElectionIntervalMs, _preferredLeaderElectionIntervalMs,
          TimeUnit.MILLISECONDS);
      LOGGER.info("{}/MultiClusterTopicManagementService started.", _serviceName);
    }
  }

  @Override
  public synchronized void stop() {
    if (_isRunning.compareAndSet(true, false)) {
      _executor.shutdown();
      LOGGER.info("{}/MultiClusterTopicManagementService stopped.", _serviceName);
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
      LOGGER.info("Thread interrupted when waiting for {}/MultiClusterTopicManagementService to shutdown", _serviceName);
    }
    LOGGER.info("{}/MultiClusterTopicManagementService shutdown completed", _serviceName);
  }



  private class TopicManagementRunnable implements Runnable {

    @Override
    public void run() {
      try {
        for (TopicManagementHelper helper : _topicManagementByCluster.values()) {
          helper.maybeCreateTopic();
        }

        /*
         * The partition number of the monitor topics should be the minimum partition number that satisfies the following conditions:
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
          } catch (IOException | KafkaException e) {
            LOGGER.warn(_serviceName + "/MultiClusterTopicManagementService will retry later in cluster " + clusterName, e);
          }
        }
      } catch (Throwable t) {
        // Need to catch throwable because there is scala API that can throw NoSuchMethodError in runtime
        // and such error is not caught by compilation
        LOGGER.error(_serviceName + "/MultiClusterTopicManagementService will stop due to error.", t);
        stop();
      }
    }
  }

  /**
   * Check if Preferred leader election is requested during Topic Management (TopicManagementRunnable),
   * trigger Preferred leader election when there is no partition reassignment in progress.
   */
  private class PreferredLeaderElectionRunnable implements Runnable {
    @Override
    public void run() {
      try {
        for (Map.Entry<String, TopicManagementHelper> entry : _topicManagementByCluster.entrySet()) {
          String clusterName = entry.getKey();
          TopicManagementHelper helper = entry.getValue();
          try {
            helper.maybeElectLeader();
          } catch (IOException | KafkaException e) {
            LOGGER.warn(_serviceName + "/MultiClusterTopicManagementService will retry later in cluster " + clusterName, e);
          }
        }
      } catch (Throwable t) {
        /* Need to catch throwable because there is scala API that can throw NoSuchMethodError in runtime
         and such error is not caught by compilation. */
        LOGGER.error(_serviceName
            + "/MultiClusterTopicManagementService/PreferredLeaderElectionRunnable will stop due to an error.", t);
        stop();
      }
    }
  }

  @SuppressWarnings("FieldCanBeLocal")
  static class TopicManagementHelper {
    private final String _zkConnect;
    private final int _replicationFactor;
    private final double _minPartitionsToBrokersRatio;
    private final int _minPartitionNum;
    private final Properties _topicProperties;
    private boolean _preferredLeaderElectionRequested;
    private final int _requestTimeoutMs;
    private final List _bootstrapServers;

    // package private for unit testing
    boolean _topicCreationEnabled;
    AdminClient _adminClient;
    String _topic;
    TopicFactory _topicFactory;


    @SuppressWarnings("unchecked")
    TopicManagementHelper(Map<String, Object> props) throws Exception {
      TopicManagementServiceConfig config = new TopicManagementServiceConfig(props);
      AdminClientConfig adminClientConfig = new AdminClientConfig(props);
      String topicFactoryClassName = config.getString(TopicManagementServiceConfig.TOPIC_FACTORY_CLASS_CONFIG);
      _topicCreationEnabled = config.getBoolean(TopicManagementServiceConfig.TOPIC_CREATION_ENABLED_CONFIG);
      _topic = config.getString(TopicManagementServiceConfig.TOPIC_CONFIG);
      _zkConnect = config.getString(TopicManagementServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
      _replicationFactor = config.getInt(TopicManagementServiceConfig.TOPIC_REPLICATION_FACTOR_CONFIG);
      _minPartitionsToBrokersRatio = config.getDouble(TopicManagementServiceConfig.PARTITIONS_TO_BROKERS_RATIO_CONFIG);
      _minPartitionNum = config.getInt(TopicManagementServiceConfig.MIN_PARTITION_NUM_CONFIG);
      _preferredLeaderElectionRequested = false;
      _requestTimeoutMs = adminClientConfig.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
      _bootstrapServers = adminClientConfig.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
      _topicProperties = new Properties();
      if (props.containsKey(TopicManagementServiceConfig.TOPIC_PROPS_CONFIG)) {
        for (Map.Entry<String, Object> entry: ((Map<String, Object>) props.get(TopicManagementServiceConfig.TOPIC_PROPS_CONFIG)).entrySet())
          _topicProperties.put(entry.getKey(), entry.getValue().toString());
      }

      Map topicFactoryConfig = props.containsKey(TopicManagementServiceConfig.TOPIC_FACTORY_PROPS_CONFIG) ?
          (Map) props.get(TopicManagementServiceConfig.TOPIC_FACTORY_PROPS_CONFIG) : new HashMap();
      _topicFactory = (TopicFactory) Class.forName(topicFactoryClassName).getConstructor(Map.class).newInstance(topicFactoryConfig);

      _adminClient = constructAdminClient(props);
      LOGGER.info("{} configs: {}", _adminClient.getClass().getSimpleName(), props);
    }

    @SuppressWarnings("unchecked")
    void maybeCreateTopic() throws Exception {
      if (_topicCreationEnabled) {
        int brokerCount = _adminClient.describeCluster().nodes().get().size();
        int numPartitions = Math.max((int) Math.ceil(brokerCount * _minPartitionsToBrokersRatio), minPartitionNum());
        NewTopic newTopic = new NewTopic(_topic, numPartitions, (short) _replicationFactor);
        newTopic.configs((Map) _topicProperties);
        _topicFactory.createTopicIfNotExist(_topic, (short) _replicationFactor, _minPartitionsToBrokersRatio,
            _topicProperties, _adminClient);
      }
    }

    AdminClient constructAdminClient(Map<String, Object> props) {
      return AdminClient.create(props);
    }

    int minPartitionNum() throws InterruptedException, ExecutionException {
      int brokerCount = _adminClient.describeCluster().nodes().get().size();
      return Math.max((int) Math.ceil(_minPartitionsToBrokersRatio * brokerCount), _minPartitionNum);
    }

    void maybeAddPartitions(int minPartitionNum) throws ExecutionException, InterruptedException {
      Map<String, KafkaFuture<TopicDescription>> kafkaFutureMap =
          _adminClient.describeTopics(Collections.singleton(_topic)).values();
      KafkaFuture<TopicDescription> topicDescriptions = kafkaFutureMap.get(_topic);
      List<TopicPartitionInfo> partitions = topicDescriptions.get().partitions();

      int partitionNum = partitions.size();
      if (partitionNum < minPartitionNum) {
        LOGGER.info("{} will increase partition of the topic {} in the cluster from {}"
            + " to {}.", this.getClass().toString(), _topic, partitionNum, minPartitionNum);
        Set<Integer> blackListedBrokers = _topicFactory.getBlackListedBrokers(_zkConnect);
        Set<BrokerMetadata> brokers = new HashSet<>();
        for (Node broker : _adminClient.describeCluster().nodes().get()) {
          BrokerMetadata brokerMetadata = new BrokerMetadata(
              broker.id(), null
          );
          brokers.add(brokerMetadata);
        }

        if (!blackListedBrokers.isEmpty()) {
          brokers.removeIf(broker -> blackListedBrokers.contains(broker.id()));
        }

        // The replica assignments for the new partitions, and not the old partitions.
        // NewPartitions.increaseTo(6, asList(asList(1, 2),
        //                                    asList(2, 3),
        //                                    asList(3, 1)))
        // partition 3's PL will be broker 1, partition 4's PL will be broker 2 and partition 5's PL will be broker 3.
        List<List<Integer>> newPartitionAssignments = new ArrayList<>(new ArrayList<>());
        int partitionDifference = minPartitionNum - partitionNum;

        // leader assignments
        for (BrokerMetadata brokerMetadata : brokers) {
          List replicas = new ArrayList<>();
          // leader replica/broker
          replicas.add(brokerMetadata.id());
          newPartitionAssignments.add(replicas);
          if (newPartitionAssignments.size() > partitionDifference) {
            break;
          }
        }

        // Regardless of the partition/replica assignments here, `maybeReassignPartitionAndElectLeader()`
        // will reassign the partition as needed periodically.
        // follower assignments
        newPartitionAssignmentLoops:
        for (List<Integer> replicas : newPartitionAssignments) {
          brokersLoop:
          for (BrokerMetadata broker : brokers) {
            if (!replicas.contains(broker.id())) {
              replicas.add(broker.id());
            }
            if (replicas.size() == _replicationFactor) {
              break brokersLoop;
            }
          }
        }

        NewPartitions newPartitions = NewPartitions.increaseTo(minPartitionNum, newPartitionAssignments);

        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(_topic, newPartitions);
        CreatePartitionsResult createPartitionsResult = _adminClient.createPartitions(newPartitionsMap);

        // TODO: should we block thread on the createPartitionsResult future?
      }
    }

    /**
     * Exposed package-private access for testing. Get the total number of partitions for a Kafka topic.
     * @return total number of topic partitions
     * @throws InterruptedException when a thread is waiting, sleeping and the thread is interrupted, either before / during the activity.
     * @throws ExecutionException when attempting to retrieve the result of a task that aborted by throwing an exception.
     */
    int numPartitions() throws InterruptedException, ExecutionException {

      // TODO (andrewchoi5): connect this to unit testing method for testing maybeAddPartitions!

      return _adminClient.describeTopics(Collections.singleton(_topic)).values().get(_topic).get().partitions().size();
    }


    private Set<Node> getAvailableBrokers() throws ExecutionException, InterruptedException {
      Set<Node> brokers = new HashSet<>(_adminClient.describeCluster().nodes().get());
      Set<Integer> blackListedBrokers = _topicFactory.getBlackListedBrokers(_zkConnect);
      brokers.removeIf(broker -> blackListedBrokers.contains(broker.id()));
      return brokers;
    }

    void maybeReassignPartitionAndElectLeader() throws Exception {
      try (KafkaZkClient zkClient = KafkaZkClient.apply(_zkConnect, JaasUtils.isZkSecurityEnabled(), com.linkedin.kmf.common.Utils.ZK_SESSION_TIMEOUT_MS,
          com.linkedin.kmf.common.Utils.ZK_CONNECTION_TIMEOUT_MS, Integer.MAX_VALUE, Time.SYSTEM, METRIC_GROUP_NAME, "SessionExpireListener", null)) {

        List<TopicPartitionInfo> partitionInfoList = _adminClient
            .describeTopics(Collections.singleton(_topic)).all().get().get(_topic).partitions();
        Collection<Node> brokers = this.getAvailableBrokers();
        boolean partitionReassigned = false;
        if (partitionInfoList.size() == 0) {
          throw new IllegalStateException("Topic " + _topic + " does not exist in cluster.");
        }

        int currentReplicationFactor = getReplicationFactor(partitionInfoList);
        int expectedReplicationFactor = Math.max(currentReplicationFactor, _replicationFactor);

        if (_replicationFactor < currentReplicationFactor)
          LOGGER.debug(
              "Configured replication factor {} is smaller than the current replication factor {} of the topic {} in cluster.",
              _replicationFactor, currentReplicationFactor, _topic);

        if (expectedReplicationFactor > currentReplicationFactor && !zkClient
            .reassignPartitionsInProgress()) {
          LOGGER.info(
              "MultiClusterTopicManagementService will increase the replication factor of the topic {} in cluster"
                  + "from {} to {}", _topic, currentReplicationFactor, expectedReplicationFactor);
          reassignPartitions(zkClient, brokers, _topic, partitionInfoList.size(),
              expectedReplicationFactor);
          partitionReassigned = true;
        }

        // Update the properties of the monitor topic if any config is different from the user-specified config
        Properties currentProperties = zkClient.getEntityConfigs(ConfigType.Topic(), _topic);
        Properties expectedProperties = new Properties();
        for (Object key : currentProperties.keySet())
          expectedProperties.put(key, currentProperties.get(key));
        for (Object key : _topicProperties.keySet())
          expectedProperties.put(key, _topicProperties.get(key));

        if (!currentProperties.equals(expectedProperties)) {
          LOGGER.info("MultiClusterTopicManagementService will overwrite properties of the topic {} "
              + "in cluster from {} to {}.", _topic, currentProperties, expectedProperties);
          zkClient.setOrCreateEntityConfigs(ConfigType.Topic(), _topic, expectedProperties);
        }

        if (partitionInfoList.size() >= brokers.size() &&
            someBrokerNotPreferredLeader(partitionInfoList, brokers) && !zkClient
            .reassignPartitionsInProgress()) {
          LOGGER.info("{} will reassign partitions of the topic {} in cluster.",
              this.getClass().toString(), _topic);
          reassignPartitions(zkClient, brokers, _topic, partitionInfoList.size(),
              expectedReplicationFactor);
          partitionReassigned = true;
        }

        if (partitionInfoList.size() >= brokers.size() &&
            someBrokerNotElectedLeader(partitionInfoList, brokers)) {
          if (!partitionReassigned || !zkClient.reassignPartitionsInProgress()) {
            LOGGER.info(
                "MultiClusterTopicManagementService will trigger preferred leader election for the topic {} in "
                    + "cluster.", _topic
            );
            triggerPreferredLeaderElection(partitionInfoList, _topic);
            _preferredLeaderElectionRequested = false;
          } else {
            _preferredLeaderElectionRequested = true;
          }
        }
      }
    }

    void maybeElectLeader() throws Exception {
      if (!_preferredLeaderElectionRequested) {
        return;
      }

      try (KafkaZkClient zkClient = KafkaZkClient.apply(_zkConnect, JaasUtils.isZkSecurityEnabled(), com.linkedin.kmf.common.Utils.ZK_SESSION_TIMEOUT_MS,
          com.linkedin.kmf.common.Utils.ZK_CONNECTION_TIMEOUT_MS, Integer.MAX_VALUE, Time.SYSTEM, METRIC_GROUP_NAME, "SessionExpireListener", null)) {
        if (!zkClient.reassignPartitionsInProgress()) {
          List<TopicPartitionInfo> partitionInfoList = _adminClient
              .describeTopics(Collections.singleton(_topic)).all().get().get(_topic).partitions();
          LOGGER.info(
              "MultiClusterTopicManagementService will trigger requested preferred leader election for the"
                  + " topic {} in cluster.", _topic);
          triggerPreferredLeaderElection(partitionInfoList, _topic);
          _preferredLeaderElectionRequested = false;
        }
      }
    }

    private void triggerPreferredLeaderElection(List<TopicPartitionInfo> partitionInfoList, String partitionTopic)
        throws ExecutionException, InterruptedException {
      Collection<TopicPartition> partitions = new HashSet<>();
      for (TopicPartitionInfo javaPartitionInfo : partitionInfoList) {
        partitions.add(new TopicPartition(partitionTopic, javaPartitionInfo.partition()));
      }

      ElectLeadersOptions newOptions = new ElectLeadersOptions();
      ElectionType electionType = ElectionType.PREFERRED;
      Set<TopicPartition> topicPartitions = new HashSet<>(partitions);
      ElectLeadersResult electLeadersResult = _adminClient.electLeaders(electionType, topicPartitions, newOptions);

      LOGGER.info("{}: triggerPreferredLeaderElection - {}", this.getClass().toString(), electLeadersResult.all().get());
    }

    private static void reassignPartitions(KafkaZkClient zkClient, Collection<Node> brokers, String topic, int partitionCount, int replicationFactor) {
      scala.collection.mutable.ArrayBuffer<BrokerMetadata> brokersMetadata = new scala.collection.mutable.ArrayBuffer<>(brokers.size());
      for (Node broker : brokers) {
        brokersMetadata.$plus$eq(new BrokerMetadata(broker.id(), Option$.MODULE$.apply(broker.rack())));
      }
      scala.collection.Map<Object, Seq<Object>> assignedReplicas =
          AdminUtils.assignReplicasToBrokers(brokersMetadata, partitionCount, replicationFactor, 0, 0);

      scala.collection.immutable.Map<TopicPartition, Seq<Object>> newAssignment = new scala.collection.immutable.HashMap<>();
      scala.collection.Iterator<scala.Tuple2<Object, scala.collection.Seq<Object>>> it = assignedReplicas.iterator();
      while (it.hasNext()) {
        scala.Tuple2<Object, scala.collection.Seq<Object>> scalaTuple = it.next();
        TopicPartition tp = new TopicPartition(topic, (Integer) scalaTuple._1);
        newAssignment = newAssignment.$plus(new scala.Tuple2<>(tp, scalaTuple._2));
      }

      scala.collection.immutable.Set<String> topicList = new scala.collection.immutable.Set.Set1<>(topic);
      scala.collection.Map<Object, scala.collection.Seq<Object>> currentAssignment = zkClient.getPartitionAssignmentForTopics(topicList).apply(topic);
      String currentAssignmentJson = formatAsReassignmentJson(topic, currentAssignment);
      String newAssignmentJson = formatAsReassignmentJson(topic, assignedReplicas);

      LOGGER.info("Reassign partitions for topic " + topic);
      LOGGER.info("Current partition replica assignment " + currentAssignmentJson);
      LOGGER.info("New partition replica assignment " + newAssignmentJson);
      zkClient.createPartitionReassignment(newAssignment);
    }

    static int getReplicationFactor(List<TopicPartitionInfo> partitionInfoList) {
      if (partitionInfoList.isEmpty())
        throw new RuntimeException("Partition list is empty.");

      int replicationFactor = partitionInfoList.get(0).replicas().size();
      for (TopicPartitionInfo partitionInfo : partitionInfoList) {
        if (replicationFactor != partitionInfo.replicas().size()) {
          LOGGER.warn("Partitions of the topic have different replication factor.");
          return -1;
        }
      }
      return replicationFactor;
    }

    static boolean someBrokerNotPreferredLeader(List<TopicPartitionInfo> partitionInfoList, Collection<Node> brokers) {
      Set<Integer> brokersNotPreferredLeader = new HashSet<>(brokers.size());
      for (Node broker: brokers)
        brokersNotPreferredLeader.add(broker.id());
      for (TopicPartitionInfo partitionInfo : partitionInfoList)
        brokersNotPreferredLeader.remove(partitionInfo.replicas().get(0).id());

      return !brokersNotPreferredLeader.isEmpty();
    }

    static boolean someBrokerNotElectedLeader(List<TopicPartitionInfo> partitionInfoList, Collection<Node> brokers) {
      Set<Integer> brokersNotElectedLeader = new HashSet<>(brokers.size());
      for (Node broker: brokers)
        brokersNotElectedLeader.add(broker.id());
      for (TopicPartitionInfo partitionInfo : partitionInfoList) {
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
