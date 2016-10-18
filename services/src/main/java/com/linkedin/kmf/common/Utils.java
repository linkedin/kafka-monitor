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

import java.util.Properties;
import kafka.server.KafkaConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.common.security.JaasUtils;
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

  public static final int ZK_CONNECTION_TIMEOUT_MS = 30_000;
  public static final int ZK_SESSION_TIMEOUT_MS = 30_000;

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
