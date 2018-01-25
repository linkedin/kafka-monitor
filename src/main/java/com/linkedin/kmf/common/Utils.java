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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.utils.ZkUtils;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.security.JaasUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;


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
    } catch (NoSuchElementException e) {
      return 0;
    } finally {
      zkUtils.close();
    }
  }

  /**
   * Create the topic.  This method attempts to create a topic so that all
   * the brokers in the cluster will have partitionToBrokerRatio partitions.  If the topic exists, but has different parameters
   * then this does nothing to update the parameters.
   *
   * TODO: Do we care about rack aware mode?  I would think no because we want to spread the topic over all brokers.
   * @param zkUrl zookeeper connection url
   * @param topic topic name
   * @param replicationFactor the replication factor for the topic
   * @param partitionToBrokerRatio This is multiplied by the number brokers to compute the number of partitions in the topic.
   * @param minPartitionNum partition number to be created at least
   * @param topicConfig additional parameters for the topic for example min.insync.replicas
   * @return the number of partitions created
   */
  public static int createTopicIfNotExists(String zkUrl, String topic, int replicationFactor,
                                           double partitionToBrokerRatio, int minPartitionNum, Properties topicConfig) {
    ZkUtils zkUtils = ZkUtils.apply(zkUrl, ZK_SESSION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, JaasUtils.isZkSecurityEnabled());
    try {
      if (AdminUtils.topicExists(zkUtils, topic)) {
        return getPartitionNumForTopic(zkUrl, topic);
      }
      int brokerCount = zkUtils.getAllBrokersInCluster().size();
      int partitionCount = Math.max((int) Math.ceil(brokerCount * partitionToBrokerRatio), minPartitionNum);

      try {
        AdminUtils.createTopic(zkUtils, topic, partitionCount, replicationFactor, topicConfig, RackAwareMode.Enforced$.MODULE$);
      } catch (TopicExistsException e) {
        //There is a race condition with the consumer.
        LOG.debug("Monitoring topic " + topic + " already exists in cluster " + zkUrl, e);
        return getPartitionNumForTopic(zkUrl, topic);
      }
      LOG.info("Created monitoring topic " + topic + " in cluster " + zkUrl + " with " + partitionCount + " partitions, min ISR of "
        + topicConfig.get(KafkaConfig.MinInSyncReplicasProp()) + " and replication factor of " + replicationFactor + ".");

      return partitionCount;
    } finally {
      zkUtils.close();
    }
  }

  /**
   * @param zkUrl zookeeper connection url
   * @return      number of brokers in this cluster
   */
  public static int getBrokerCount(String zkUrl) {
    ZkUtils zkUtils = ZkUtils.apply(zkUrl, ZK_SESSION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, JaasUtils.isZkSecurityEnabled());
    try {
      return zkUtils.getAllBrokersInCluster().size();
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

  public static List<MbeanAttributeValue> getMBeanAttributeValues(String mbeanExpr, String attributeExpr) {
    List<MbeanAttributeValue> values = new ArrayList<>();
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      Set<ObjectName> mbeanNames = server.queryNames(new ObjectName(mbeanExpr), null);
      for (ObjectName mbeanName: mbeanNames) {
        MBeanInfo mBeanInfo = server.getMBeanInfo(mbeanName);
        MBeanAttributeInfo[] attributeInfos = mBeanInfo.getAttributes();
        for (MBeanAttributeInfo attributeInfo: attributeInfos) {
          if (attributeInfo.getName().equals(attributeExpr) || attributeExpr.length() == 0 || attributeExpr.equals("*")) {
            double value = (Double) server.getAttribute(mbeanName, attributeInfo.getName());
            values.add(new MbeanAttributeValue(mbeanName.getCanonicalName(), attributeInfo.getName(), value));
          }
        }
      }
    } catch (Exception e) {
      LOG.error("fail to retrieve value for " + mbeanExpr + ":" + attributeExpr, e);
    }
    return values;
  }

}
