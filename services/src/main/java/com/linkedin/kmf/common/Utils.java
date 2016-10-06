/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.kmf.common;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import kafka.cluster.Broker;
import kafka.server.KafkaConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.utils.ZkUtils;
import kafka.admin.AdminUtils;
import scala.collection.Seq;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;


/**
 * Kafka monitoring utilities.
 */
public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  private static final int ZK_CONNECTION_TIMEOUT_MS = 30_000;
  private static final int ZK_SESSION_TIMEOUT_MS = 30_000;

  /**
   * Read number of partitions for the given topic on the specified zookeeper
   * @param zkUrl zookeeper connection url
   * @param topic topic name
   *
   * @return the number of partitions of the given topic
   */
  public static int getPartitionNumForTopic(String zkUrl, String topic) {
    ZkUtils zkUtils = ZkUtils.apply(zkUrl, ZK_SESSION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, JaasUtils.isZkSecurityEnabled());
    try {
      Seq<String> topics = scala.collection.JavaConversions.asScalaBuffer(Arrays.asList(topic));
      return zkUtils.getPartitionsForTopics(topics).apply(topic).size();
    } finally {
      zkUtils.close();
    }
  }

  /**
   * Create the topic that the monitor uses to monitor the cluster.  This method attempts to create a topic so that all
   * the brokers in the cluster will have partitionFactor partitions.  If the topic exists, but has different parameters
   * then this does nothing to update the parameters.
   *
   * TODO: Do we care about rack aware mode?  I would think no because we want to spread the topic over all brokers.
   * @param zkUrl zookeeper connection url
   * @param topic topic name
   * @param replicationFactor the replication factor for the topic
   * @param partitionFactor This is multiplied by the number brokers to compute the number of partitions in the topic.
   * @return the number of partitions created
   */
  public static int createMonitoringTopicIfNotExists(String zkUrl, String topic, int replicationFactor,
      int partitionFactor) {
    ZkUtils zkUtils = ZkUtils.apply(zkUrl, ZK_SESSION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, JaasUtils.isZkSecurityEnabled());
    try {
      if (AdminUtils.topicExists(zkUtils, topic)) {
        LOG.info("Monitoring topic \"" + topic + "\" already exists.");
        return getPartitionNumForTopic(zkUrl, topic);
      }

      int brokerCount = zkUtils.getAllBrokersInCluster().size();

      if (partitionFactor <= 0) {
        throw new IllegalArgumentException("Partition factor must be greater than zero, but was configured for " +
            partitionFactor + ".");
      }
      int partitionCount = brokerCount * partitionFactor;

      int minIsr = Math.max(replicationFactor - 1, 1);
      Properties topicConfig = new Properties();
      topicConfig.setProperty(KafkaConfig.MinInSyncReplicasProp(), Integer.toString(minIsr));
      AdminUtils.createTopic(zkUtils, topic, partitionCount, replicationFactor, topicConfig);

      LOG.info("Created monitoring topic \"" + topic + "\" with " + partitionCount + " partitions, min ISR of " + minIsr
          + " and replication factor of " + replicationFactor + ".");

      return partitionCount;
    } finally {
      zkUtils.close();
    }
  }

  /**
   * This is so you can call KafkaConsumer.partitionsFor don't consume messages from this that is the job of the
   * ConsumeService.
   *
   * @param brokerList probably get this from the configuration file.
   * @param zkUrl zoo keeper url
   * @param topic the monitoring topic
   * @return an initialized KafkaConsumer
   */

  private static KafkaConsumer<?, ?> constructClientForMetadata(String brokerList, String zkUrl, String topic) {

    Properties consumerProps = new Properties();

    // Assign default config. This has the lowest priority.
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "kmf-metadata-consumer");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kmf-metadata-consumer-group");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    // Assign config specified for ConsumeService.
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    consumerProps.put("zookeeper.connect", zkUrl);

    KafkaConsumer<String, String> metadataConsumer = new KafkaConsumer<>(consumerProps);
    metadataConsumer.subscribe(Collections.singletonList(topic));
    return metadataConsumer;
  }

  /**
   *
   * @param zkUrl the zookeeper url
   * @param topic the monitored topic
   * @param partitionThreshold  This assumes that you want to have at least one per broker.  When the number of partitions
   * @param brokersList this is the list of brokers we would need to create a consumer connection.
   * @return true if the monitored topic is no longer distributed evenly with respect to the given parameters
   */
  public static boolean monitoredTopicNeedsRebalance(String zkUrl, String topic, String brokersList,
      double partitionThreshold) {

    if (partitionThreshold < 1) {
      throw new IllegalArgumentException(
          "partitionThreshold must be greater than or equal to one, but found " + partitionThreshold + ".");
    }

    ZkUtils zkUtils = ZkUtils.apply(zkUrl, ZK_SESSION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, JaasUtils.isZkSecurityEnabled());

    try (KafkaConsumer<?, ?> consumerForMetadata = constructClientForMetadata(brokersList, zkUrl, topic)) {
      List<PartitionInfo> partitionInfoList = consumerForMetadata.partitionsFor(topic);
      Collection<Broker> brokers = scala.collection.JavaConversions.asJavaCollection(zkUtils.getAllBrokersInCluster());
      return monitoredTopicNeedsRebalance(partitionInfoList, brokers, partitionThreshold);
    } finally {
      zkUtils.close();
    }
  }

  /**
   * If any of the following conditions are met this returns true:
   * <ul>
   *   <li> The minimum number of total monitored partitions falls below floor(brokers * partitionThreshold). </li>
   *   <li> One or more brokers does not have a monitored partition or falls below the minimum number of monitored partitions. </li>
   *   <li> One or more brokers is not a leader of a monitored partition. </li>
   * </ul>
   * @param partitionInfoList get this from the KafkaConsumer, this should only contain partition info for the monitored topic
   * @param brokers get this from ZkUtils, this should be all the brokers in the cluster
   * @param partitionThreshold the lower water mark for when we do not have enough monitored partitions
   * @return see above
   */
  static boolean monitoredTopicNeedsRebalance(List<PartitionInfo> partitionInfoList, Collection<Broker> brokers,
      double partitionThreshold) {

    int partitionCount = partitionInfoList.size();
    int minPartitionCount = (int) (partitionCount * partitionThreshold);
    int minPartitionCountPerBroker = minPartitionCount / brokers.size();

    if (partitionCount > minPartitionCount) {
      return true;
    }

    Map<Integer, Integer> brokerToPartitionCount = new HashMap<>(brokers.size());
    Set<Integer> leaders = new HashSet<>(brokers.size());

    // Count the number of partitions a broker is involved with and if it is a leader for some partition
    // Check that a partition has at least a certain number of replicas
    for (PartitionInfo partitionInfo : partitionInfoList) {
      if (partitionInfo.replicas().length < minPartitionCountPerBroker) {
        return true;
      }

      for (Node node : partitionInfo.replicas()) {
        int broker = node.id();
        if (!brokerToPartitionCount.containsKey(broker)) {
          brokerToPartitionCount.put(broker, 0);
        }
        int count = brokerToPartitionCount.get(node.id());
        brokerToPartitionCount.put(broker, count + 1);
      }

      leaders.add(partitionInfo.replicas()[0].id());
    }

    // Check that a broker is a leader for at least one partition
    // Check that a broker has at least minPartitionCountPerBroker
    for (Broker broker : brokers) {
      if (!leaders.contains(broker.id())) {
        return true;
      }
      if (!brokerToPartitionCount.containsKey(broker.id())) {
        return true;
      }
      if (brokerToPartitionCount.get(broker.id()) < minPartitionCountPerBroker) {
        return true;
      }
    }
    return false;
  }


  /**
   * @param timestamp time in Ms when this message is generated
   * @param topic     topic this message is sent to
   * @param idx       index is consecutive numbers used by KafkaMonitor to determine duplicate or lost messages
   * @param msgSize   size of the message
   * @return          string that encodes the above fields
   */
  public static String jsonFromFields(String topic, long idx, long timestamp, String producerId, int msgSize) {
    GenericRecord record = new GenericData.Record(DefaultTopicSchema.MESSAGE_V0);
    record.put(DefaultTopicSchema.TOPIC_FIELD.name(), topic);
    record.put(DefaultTopicSchema.INDEX_FIELD.name(), idx);
    record.put(DefaultTopicSchema.TIME_FIELD.name(), timestamp);
    record.put(DefaultTopicSchema.PRODUCER_ID_FIELD.name(), producerId);
    // CONTENT_FIELD is composed of #msgSize number of character 'x', e.g. xxxxxxxxxx
    record.put(DefaultTopicSchema.CONTENT_FIELD.name(), String.format("%1$-" + msgSize + "s", "").replace(' ', 'x'));
    return jsonFromGenericRecord(record);
  }

  /**
   * @param message kafka message in the string format
   * @return        GenericRecord that is deserialized from kafka message w.r.t. expected schema
   */
  public static GenericRecord genericRecordFromJson(String message) {
    GenericRecord record = new GenericData.Record(DefaultTopicSchema.MESSAGE_V0);
    JSONObject jsonObject = new JSONObject(message);
    record.put(DefaultTopicSchema.TOPIC_FIELD.name(), jsonObject.getString(DefaultTopicSchema.TOPIC_FIELD.name()));
    record.put(DefaultTopicSchema.INDEX_FIELD.name(), jsonObject.getLong(DefaultTopicSchema.INDEX_FIELD.name()));
    record.put(DefaultTopicSchema.TIME_FIELD.name(), jsonObject.getLong(DefaultTopicSchema.TIME_FIELD.name()));
    record.put(DefaultTopicSchema.PRODUCER_ID_FIELD.name(), jsonObject.getString(DefaultTopicSchema.PRODUCER_ID_FIELD.name()));
    record.put(DefaultTopicSchema.CONTENT_FIELD.name(), jsonObject.getString(DefaultTopicSchema.CONTENT_FIELD.name()));
    return record;
  }

  public static String jsonFromGenericRecord(GenericRecord record) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(DefaultTopicSchema.MESSAGE_V0);

    try {
      Encoder encoder = new JsonEncoder(DefaultTopicSchema.MESSAGE_V0, out);
      writer.write(record, encoder);
      encoder.flush();
    } catch (IOException e) {
      LOG.error("Unable to serialize avro record due to error " + e);
    }
    return out.toString();
  }

}
