/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services;

import com.linkedin.xinfra.monitor.common.Utils;
import com.linkedin.xinfra.monitor.services.configs.CommonServiceConfig;
import com.linkedin.xinfra.monitor.services.configs.MultiClusterTopicManagementServiceConfig;
import com.linkedin.xinfra.monitor.services.configs.TopicManagementServiceConfig;
import com.linkedin.xinfra.monitor.topicfactory.TopicFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
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
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option$;
import scala.collection.JavaConverters;
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
  private final int _rebalanceIntervalMs;
  private final long _preferredLeaderElectionIntervalMs;
  private final ScheduledExecutorService _executor;

  @SuppressWarnings("unchecked")
  public MultiClusterTopicManagementService(Map<String, Object> props, String serviceName) throws Exception {
    _serviceName = serviceName;
    MultiClusterTopicManagementServiceConfig config = new MultiClusterTopicManagementServiceConfig(props);
    String topic = config.getString(CommonServiceConfig.TOPIC_CONFIG);
    Map<String, Map> propsByCluster =
        props.containsKey(MultiClusterTopicManagementServiceConfig.PROPS_PER_CLUSTER_CONFIG) ? (Map) props.get(
            MultiClusterTopicManagementServiceConfig.PROPS_PER_CLUSTER_CONFIG) : new HashMap<>();
    _topicManagementByCluster = initializeTopicManagementHelper(propsByCluster, topic);
    _rebalanceIntervalMs = config.getInt(MultiClusterTopicManagementServiceConfig.REBALANCE_INTERVAL_MS_CONFIG);
    _preferredLeaderElectionIntervalMs =
        config.getLong(MultiClusterTopicManagementServiceConfig.PREFERRED_LEADER_ELECTION_CHECK_INTERVAL_MS_CONFIG);
    _executor = Executors.newSingleThreadScheduledExecutor(
      r -> new Thread(r, _serviceName + "-multi-cluster-topic-management-service"));
    _topicPartitionResult.complete(null);
  }

  public CompletableFuture<Void> topicPartitionResult() {
    return _topicPartitionResult;
  }

  private Map<String, TopicManagementHelper> initializeTopicManagementHelper(Map<String, Map> propsByCluster,
      String topic) throws Exception {
    Map<String, TopicManagementHelper> topicManagementByCluster = new HashMap<>();
    for (Map.Entry<String, Map> entry : propsByCluster.entrySet()) {
      String clusterName = entry.getKey();
      Map serviceProps = entry.getValue();
      if (serviceProps.containsKey(MultiClusterTopicManagementServiceConfig.TOPIC_CONFIG)) {
        throw new ConfigException("The raw per-cluster config for MultiClusterTopicManagementService must not contain "
            + MultiClusterTopicManagementServiceConfig.TOPIC_CONFIG);
      }
      serviceProps.put(MultiClusterTopicManagementServiceConfig.TOPIC_CONFIG, topic);
      topicManagementByCluster.put(clusterName, new TopicManagementHelper(serviceProps));
    }
    return topicManagementByCluster;
  }

  @Override
  public synchronized void start() {
    if (_isRunning.compareAndSet(false, true)) {
      final long topicManagementProcedureInitialDelay = 0;
      _executor.scheduleWithFixedDelay(
              new TopicManagementRunnable(),
              topicManagementProcedureInitialDelay,
              _rebalanceIntervalMs,
              TimeUnit.MILLISECONDS);

      LOGGER.info("Topic management periodical procedure started with initial delay {} ms and interval {} ms",
              topicManagementProcedureInitialDelay, _rebalanceIntervalMs);

      _executor.scheduleWithFixedDelay(new PreferredLeaderElectionRunnable(), _preferredLeaderElectionIntervalMs,
          _preferredLeaderElectionIntervalMs, TimeUnit.MILLISECONDS);
      LOGGER.info("Preferred leader election periodical procedure started with initial delay {} ms and interval {} ms",
              _preferredLeaderElectionIntervalMs, _preferredLeaderElectionIntervalMs);
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
  public void awaitShutdown(long timeout, TimeUnit unit) {
    try {
      _executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.info("Thread interrupted when waiting for {}/MultiClusterTopicManagementService to shutdown",
          _serviceName);
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
          } catch (KafkaException e) {
            LOGGER.warn(_serviceName + "/MultiClusterTopicManagementService will retry later in cluster " + clusterName,
                e);
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
          } catch (KafkaException e) {
            LOGGER.warn(_serviceName + "/MultiClusterTopicManagementService will retry later in cluster " + clusterName,
                e);
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
    private final int _replicationFactor;
    private final double _minPartitionsToBrokersRatio;
    private final int _minPartitionNum;
    private final Properties _topicProperties;
    private boolean _preferredLeaderElectionRequested;
    private final Duration _requestTimeout;
    private final List<String> _bootstrapServers;

    // package private for unit testing
    boolean _topicCreationEnabled;
    boolean _topicAddPartitionEnabled;
    boolean _topicReassignPartitionAndElectLeaderEnabled;
    AdminClient _adminClient;
    String _topic;
    TopicFactory _topicFactory;

    @SuppressWarnings("unchecked")
    TopicManagementHelper(Map<String, Object> props) throws Exception {

      TopicManagementServiceConfig config = new TopicManagementServiceConfig(props);
      AdminClientConfig adminClientConfig = new AdminClientConfig(props);
      String topicFactoryClassName = config.getString(TopicManagementServiceConfig.TOPIC_FACTORY_CLASS_CONFIG);
      _topicCreationEnabled = config.getBoolean(TopicManagementServiceConfig.TOPIC_CREATION_ENABLED_CONFIG);
      _topicAddPartitionEnabled = config.getBoolean(TopicManagementServiceConfig.TOPIC_ADD_PARTITION_ENABLED_CONFIG);
      _topicReassignPartitionAndElectLeaderEnabled = config.getBoolean(TopicManagementServiceConfig.TOPIC_REASSIGN_PARTITION_AND_ELECT_LEADER_ENABLED_CONFIG);
      _topic = config.getString(TopicManagementServiceConfig.TOPIC_CONFIG);
      _replicationFactor = config.getInt(TopicManagementServiceConfig.TOPIC_REPLICATION_FACTOR_CONFIG);
      _minPartitionsToBrokersRatio = config.getDouble(TopicManagementServiceConfig.PARTITIONS_TO_BROKERS_RATIO_CONFIG);
      _minPartitionNum = config.getInt(TopicManagementServiceConfig.MIN_PARTITION_NUM_CONFIG);
      _preferredLeaderElectionRequested = false;
      _requestTimeout = Duration.ofMillis(adminClientConfig.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
      _bootstrapServers = adminClientConfig.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
      _topicProperties = new Properties();
      if (props.containsKey(TopicManagementServiceConfig.TOPIC_PROPS_CONFIG)) {
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) props.get(
            TopicManagementServiceConfig.TOPIC_PROPS_CONFIG)).entrySet()) {
          _topicProperties.put(entry.getKey(), entry.getValue().toString());
        }
      }

      Map topicFactoryConfig =
          props.containsKey(TopicManagementServiceConfig.TOPIC_FACTORY_PROPS_CONFIG) ? (Map) props.get(
              TopicManagementServiceConfig.TOPIC_FACTORY_PROPS_CONFIG) : new HashMap();
      _topicFactory =
          (TopicFactory) Class.forName(topicFactoryClassName).getConstructor(Map.class).newInstance(topicFactoryConfig);
      _adminClient = constructAdminClient(props);
      LOGGER.info("{} configs: {}", _adminClient.getClass().getSimpleName(), props);
      logConfigurationValues();
    }

    private void logConfigurationValues() {
      LOGGER.info("TopicManagementHelper for cluster with bootstrap servers {} is configured with " +
              "[topic={}, topicCreationEnabled={}, topicAddPartitionEnabled={}, " +
              "topicReassignPartitionAndElectLeaderEnabled={}, minPartitionsToBrokersRatio={}, " +
              "minPartitionNum={}]", _bootstrapServers, _topic, _topicCreationEnabled, _topicAddPartitionEnabled,
              _topicReassignPartitionAndElectLeaderEnabled, _minPartitionsToBrokersRatio, _minPartitionNum);
    }

    @SuppressWarnings("unchecked")
    void maybeCreateTopic() throws Exception {
      if (!_topicCreationEnabled) {
        LOGGER.info("Topic creation is not enabled for {} in a cluster with bootstrap servers {}. " +
                "Refer to config: {}", _topic, _bootstrapServers, TopicManagementServiceConfig.TOPIC_CREATION_ENABLED_CONFIG);
        return;
      }
      NewTopic newTopic = new NewTopic(_topic, minPartitionNum(), (short) _replicationFactor);
      newTopic.configs((Map) _topicProperties);
      _topicFactory.createTopicIfNotExist(_topic, (short) _replicationFactor, _minPartitionsToBrokersRatio,
              _topicProperties, _adminClient);
    }

    AdminClient constructAdminClient(Map<String, Object> props) {
      return AdminClient.create(props);
    }

    int minPartitionNum() throws InterruptedException, ExecutionException {
      int brokerCount = _adminClient.describeCluster().nodes().get().size();
      return Math.max((int) Math.ceil(_minPartitionsToBrokersRatio * brokerCount), _minPartitionNum);
    }

    void maybeAddPartitions(final int requiredMinPartitionNum)
            throws ExecutionException, InterruptedException, CancellationException, TimeoutException {
      if (!_topicAddPartitionEnabled) {
        LOGGER.info("Adding partition to {} topic is not enabled in a cluster with bootstrap servers {}. " +
                "Refer to config: {}", _topic, _bootstrapServers, TopicManagementServiceConfig.TOPIC_ADD_PARTITION_ENABLED_CONFIG);
        return;
      }
      Map<String, KafkaFuture<TopicDescription>> kafkaFutureMap =
          _adminClient.describeTopics(Collections.singleton(_topic)).values();
      KafkaFuture<TopicDescription> topicDescriptions = kafkaFutureMap.get(_topic);
      List<TopicPartitionInfo> partitions = topicDescriptions.get(_requestTimeout.toMillis(), TimeUnit.MILLISECONDS).partitions();

      final int currPartitionNum = partitions.size();
      if (currPartitionNum >= requiredMinPartitionNum) {
        LOGGER.debug("{} will not increase partition of the topic {} in the cluster. Current partition count {} and '" +
                "minimum required partition count is {}.", this.getClass().toString(), _topic, currPartitionNum, requiredMinPartitionNum);
        return;
      }
      LOGGER.info("{} will increase partition of the topic {} in the cluster from {}" + " to {}.",
              this.getClass().toString(), _topic, currPartitionNum,  requiredMinPartitionNum);
      Set<BrokerMetadata> brokers = new HashSet<>();
      for (Node broker : _adminClient.describeCluster().nodes().get(_requestTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        BrokerMetadata brokerMetadata = new BrokerMetadata(broker.id(), null);
        brokers.add(brokerMetadata);
      }
      Set<Integer> excludedBrokers = _topicFactory.getExcludedBrokers(_adminClient);
      if (!excludedBrokers.isEmpty()) {
        brokers.removeIf(broker ->  excludedBrokers.contains(broker.id()));
      }

      List<List<Integer>> newPartitionAssignments =
              newPartitionAssignments(requiredMinPartitionNum, currPartitionNum, brokers, _replicationFactor);

      NewPartitions newPartitions = NewPartitions.increaseTo(requiredMinPartitionNum, newPartitionAssignments);

      Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
      newPartitionsMap.put(_topic, newPartitions);
      _adminClient.createPartitions(newPartitionsMap).all().get(_requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
      LOGGER.info("{} finished increasing partition of the topic {} in the cluster from {} to {}",
              this.getClass().toString(), _topic, currPartitionNum,  requiredMinPartitionNum);
    }

    static List<List<Integer>> newPartitionAssignments(int minPartitionNum, int partitionNum,
        Set<BrokerMetadata> brokers, int rf) {

      // The replica assignments for the new partitions, and not the old partitions.
      // .increaseTo(6, asList(asList(1, 2),
      //                       asList(2, 3),
      //                       asList(3, 1)))
      // partition 3's preferred leader will be broker 1,
      // partition 4's preferred leader will be broker 2 and
      // partition 5's preferred leader will be broker 3.
      List<List<Integer>> newPartitionAssignments = new ArrayList<>();
      int partitionDifference = minPartitionNum - partitionNum;

      // leader assignments -
      while (newPartitionAssignments.size() != partitionDifference) {
        List replicas = new ArrayList<>();
        // leader replica/broker -
        int brokerMetadata = randomBroker(brokers).id();
        replicas.add(brokerMetadata);

        newPartitionAssignments.add(replicas);
      }

      // follower assignments -
      // Regardless of the partition/replica assignments here, maybeReassignPartitionAndElectLeader()
      // will reassign the partition as needed periodically.
      for (List<Integer> replicas : newPartitionAssignments) {
        for (BrokerMetadata broker : brokers) {
          if (!replicas.contains(broker.id())) {
            replicas.add(broker.id());
          }
          if (replicas.size() == rf) {
            break;
          }
        }
      }
      return newPartitionAssignments;
    }

    private static BrokerMetadata randomBroker(Set<BrokerMetadata> brokers) {

      if (brokers == null || brokers.size() == 0) {
        throw new IllegalArgumentException("brokers object is either null or empty.");
      }

      // Using Set enforces the usage of loop which is O(n).
      // As the list of brokers does not change in newPartitionAssignments,
      // the acceptance of a List argument instead of a Set will be faster which is (O(1))
      List<BrokerMetadata> brokerMetadataList = new ArrayList<>(brokers);
      // convert to a list so there's no need to create a index and iterate through this set
      //addAll() is replaced with parameterized constructor call for better performance..

      int brokerSetSize = brokers.size();

      // In practicality, the Random object should be rather more shared than this.
      int random = new Random().nextInt(brokerSetSize);

      return brokerMetadataList.get(random);
    }

    /**
     * Exposed package-private access for testing. Get the total number of partitions for a Kafka topic.
     * @return total number of topic partitions
     * @throws InterruptedException when a thread is waiting, sleeping and the thread is interrupted, either before / during the activity.
     * @throws ExecutionException when attempting to retrieve the result of a task that aborted by throwing an exception.
     */
    int numPartitions() throws InterruptedException, ExecutionException {

      return _adminClient.describeTopics(Collections.singleton(_topic)).values().get(_topic).get().partitions().size();
    }

    private Set<Node> getAvailableBrokers() throws ExecutionException, InterruptedException {
      Set<Node> brokers = new HashSet<>(_adminClient.describeCluster().nodes().get());
      Set<Integer> excludedBrokers = _topicFactory.getExcludedBrokers(_adminClient);
      brokers.removeIf(broker -> excludedBrokers.contains(broker.id()));
      return brokers;
    }

    void maybeReassignPartitionAndElectLeader() throws ExecutionException, InterruptedException, TimeoutException {
      if (!_topicReassignPartitionAndElectLeaderEnabled) {
        LOGGER.info("Reassign partition and elect leader to {} topic is not enabled in a cluster with bootstrap servers {}. " +
                "Refer to config: {}", _topic, _bootstrapServers, TopicManagementServiceConfig.TOPIC_REASSIGN_PARTITION_AND_ELECT_LEADER_ENABLED_CONFIG);
        return;
      }
      List<TopicPartitionInfo> partitionInfoList =
          _adminClient.describeTopics(Collections.singleton(_topic)).all().get().get(_topic).partitions();
      Collection<Node> brokers = this.getAvailableBrokers();
      boolean partitionReassigned = false;
      if (partitionInfoList.size() == 0) {
        throw new IllegalStateException("Topic " + _topic + " does not exist in cluster.");
      }

      int currentReplicationFactor = getReplicationFactor(partitionInfoList);
      int expectedReplicationFactor = Math.max(currentReplicationFactor, _replicationFactor);

      if (_replicationFactor < currentReplicationFactor) {
        LOGGER.debug(
            "Configured replication factor {} is smaller than the current replication factor {} of the topic {} in cluster.",
            _replicationFactor, currentReplicationFactor, _topic);
      }

      if (expectedReplicationFactor > currentReplicationFactor && Utils.ongoingPartitionReassignments(_adminClient)
          .isEmpty()) {

        LOGGER.info(
            "MultiClusterTopicManagementService will increase the replication factor of the topic {} in cluster"
                + "from {} to {}", _topic, currentReplicationFactor, expectedReplicationFactor);
        reassignPartitions(_adminClient, brokers, _topic, partitionInfoList.size(), expectedReplicationFactor);

        partitionReassigned = true;
      }

      // Update the properties of the monitor topic if any config is different from the user-specified config
      ConfigResource topicConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, _topic);
      Config currentConfig = _adminClient.describeConfigs(Collections.singleton(topicConfigResource)).all().get().get(topicConfigResource);
      Collection<AlterConfigOp> alterConfigOps = new ArrayList<>();
      for (Map.Entry<Object, Object> entry : _topicProperties.entrySet()) {
        String name = String.valueOf(entry.getKey());
        ConfigEntry configEntry = new ConfigEntry(name, String.valueOf(entry.getValue()));
        if (!configEntry.equals(currentConfig.get(name))) {
          alterConfigOps.add(new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET));
        }
      }

      if (!alterConfigOps.isEmpty()) {
        LOGGER.info("MultiClusterTopicManagementService will overwrite properties of the topic {} "
                + "in cluster with {}.", _topic, alterConfigOps);
        Map<ConfigResource, Collection<AlterConfigOp>> configs = Collections.singletonMap(topicConfigResource, alterConfigOps);
        _adminClient.incrementalAlterConfigs(configs);
      }

      if (partitionInfoList.size() >= brokers.size() && someBrokerNotPreferredLeader(partitionInfoList, brokers)
          && Utils.ongoingPartitionReassignments(_adminClient).isEmpty()) {
        LOGGER.info("{} will reassign partitions of the topic {} in cluster.", this.getClass().toString(), _topic);
        reassignPartitions(_adminClient, brokers, _topic, partitionInfoList.size(), expectedReplicationFactor);

        partitionReassigned = true;
      }

      if (partitionInfoList.size() >= brokers.size() && someBrokerNotElectedLeader(partitionInfoList, brokers)) {
        if (!partitionReassigned || Utils.ongoingPartitionReassignments(_adminClient).isEmpty()) {
          LOGGER.info("MultiClusterTopicManagementService will trigger preferred leader election for the topic {} in "
              + "cluster.", _topic);
          triggerPreferredLeaderElection(partitionInfoList, _topic);
          _preferredLeaderElectionRequested = false;
        } else {
          _preferredLeaderElectionRequested = true;
        }
      }
    }

    void maybeElectLeader() throws InterruptedException, ExecutionException, TimeoutException {
      if (!_preferredLeaderElectionRequested) {
        return;
      }

      if (Utils.ongoingPartitionReassignments(_adminClient).isEmpty()) {
        List<TopicPartitionInfo> partitionInfoList =
            _adminClient.describeTopics(Collections.singleton(_topic)).all().get().get(_topic).partitions();
        LOGGER.info("MultiClusterTopicManagementService will trigger requested preferred leader election for the"
            + " topic {} in cluster.", _topic);
        triggerPreferredLeaderElection(partitionInfoList, _topic);
        _preferredLeaderElectionRequested = false;
      }
    }

    private void triggerPreferredLeaderElection(List<TopicPartitionInfo> partitionInfoList, String partitionTopic) {
      Set<TopicPartition> partitions = new HashSet<>();
      for (TopicPartitionInfo javaPartitionInfo : partitionInfoList) {
        partitions.add(new TopicPartition(partitionTopic, javaPartitionInfo.partition()));
      }
      ElectLeadersResult electLeadersResult = _adminClient.electLeaders(ElectionType.PREFERRED, partitions);

      LOGGER.info("{}: triggerPreferredLeaderElection - {}", this.getClass().toString(),
          electLeadersResult.all());
    }

    private static void reassignPartitions(AdminClient adminClient, Collection<Node> brokers, String topic,
        int partitionCount, int replicationFactor) {

      scala.collection.mutable.ArrayBuffer<BrokerMetadata> brokersMetadata =
          new scala.collection.mutable.ArrayBuffer<>(brokers.size());
      for (Node broker : brokers) {
        brokersMetadata.$plus$eq(new BrokerMetadata(broker.id(), Option$.MODULE$.apply(broker.rack())));
      }
      scala.collection.Map<Object, Seq<Object>> assignedReplicas =
          AdminUtils.assignReplicasToBrokers(brokersMetadata, partitionCount, replicationFactor, 0, 0);
      scala.collection.immutable.Map<TopicPartition, Seq<Object>> newAssignment =
          new scala.collection.immutable.HashMap<>();
      scala.collection.Iterator<scala.Tuple2<Object, scala.collection.Seq<Object>>> it = assignedReplicas.iterator();
      while (it.hasNext()) {
        scala.Tuple2<Object, scala.collection.Seq<Object>> scalaTuple = it.next();
        TopicPartition tp = new TopicPartition(topic, (Integer) scalaTuple._1);
        newAssignment = newAssignment.$plus(new scala.Tuple2<>(tp, scalaTuple._2));
      }

      String newAssignmentJson = formatAsNewReassignmentJson(topic, assignedReplicas);
      LOGGER.info("Reassign partitions for topic " + topic);
      LOGGER.info("New topic partition replica assignments: {}", newAssignmentJson);

      Set<Map.Entry<TopicPartition, Seq<Object>>> newAssignmentMap =
          scala.collection.JavaConverters.mapAsJavaMap(newAssignment).entrySet();
      Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
      for (Map.Entry<TopicPartition, Seq<Object>> topicPartitionSeqEntry : newAssignmentMap) {
        List<Integer> targetReplicas = new ArrayList<>();
        List<Object> replicas = JavaConverters.seqAsJavaList(topicPartitionSeqEntry.getValue());
        for (Object replica : replicas) {
          targetReplicas.add((int) replica);
        }
        NewPartitionReassignment newPartitionReassignment = new NewPartitionReassignment(targetReplicas);
        reassignments.put(topicPartitionSeqEntry.getKey(), Optional.of(newPartitionReassignment));
      }

      AlterPartitionReassignmentsResult alterPartitionReassignmentsResult =
          adminClient.alterPartitionReassignments(reassignments);
      try {
        alterPartitionReassignmentsResult.all().get();
      } catch (InterruptedException | ExecutionException e) {

        LOGGER.error("An exception occurred while altering the partition reassignments for {}", topic, e);
      }
    }

    static int getReplicationFactor(List<TopicPartitionInfo> partitionInfoList) {
      if (partitionInfoList.isEmpty()) {
        throw new RuntimeException("Partition list is empty.");
      }

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
      for (Node broker : brokers) {
        brokersNotPreferredLeader.add(broker.id());
      }
      for (TopicPartitionInfo partitionInfo : partitionInfoList) {
        brokersNotPreferredLeader.remove(partitionInfo.replicas().get(0).id());
      }

      return !brokersNotPreferredLeader.isEmpty();
    }

    static boolean someBrokerNotElectedLeader(List<TopicPartitionInfo> partitionInfoList, Collection<Node> brokers) {
      Set<Integer> brokersNotElectedLeader = new HashSet<>(brokers.size());
      for (Node broker : brokers) {
        brokersNotElectedLeader.add(broker.id());
      }
      for (TopicPartitionInfo partitionInfo : partitionInfoList) {
        if (partitionInfo.leader() != null) {
          brokersNotElectedLeader.remove(partitionInfo.leader().id());
        }
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

    // TODO (andrewchoi5): uncomment this method when Xinfra Monitor is upgraded to 'org.apache.kafka' 'kafka_2.12' version '2.4.1'
//    private static String formatAsOldAssignmentJson(String topic, scala.collection.Map<Object, ReplicaAssignment> partitionsToBeReassigned) {
//      StringBuilder bldr = new StringBuilder();
//      bldr.append("{\"version\":1,\"partitions\":[\n");
//      for (int partition = 0; partition < partitionsToBeReassigned.size(); partition++) {
//        bldr.append("  {\"topic\":\"").append(topic).append("\",\"partition\":").append(partition).append(",\"replicas\":[");
//        ReplicaAssignment replicas = partitionsToBeReassigned.apply(partition);
//        for (int replicaIndex = 0; replicaIndex < replicas.replicas().size(); replicaIndex++) {
//          Object replica = replicas.replicas().apply(replicaIndex);
//          bldr.append(replica).append(",");
//        }
//        bldr.setLength(bldr.length() - 1);
//        bldr.append("]},\n");
//      }
//      bldr.setLength(bldr.length() - 2);
//      bldr.append("]}");
//      return bldr.toString();
//    }

    /**
     * @param topic Kafka topic
     * @param partitionsToReassign a map from partition (int) to new replica list (int seq)
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
    private static String formatAsNewReassignmentJson(String topic,
        scala.collection.Map<Object, Seq<Object>> partitionsToReassign) {
      StringBuilder builder = new StringBuilder();
      builder.append("{\"version\":1,\"partitions\":[\n");
      for (int partition = 0; partition < partitionsToReassign.size(); partition++) {
        builder.append("  {\"topic\":\"")
            .append(topic)
            .append("\",\"partition\":")
            .append(partition)
            .append(",\"replicas\":[");
        Seq<Object> replicas = partitionsToReassign.apply(partition);
        for (int replicaIndex = 0; replicaIndex < replicas.size(); replicaIndex++) {
          Object replica = replicas.apply(replicaIndex);
          builder.append(replica).append(",");
        }
        builder.setLength(builder.length() - 1);
        builder.append("]},\n");
      }
      builder.setLength(builder.length() - 2);
      builder.append("]}");
      return builder.toString();
    }
  }
}
