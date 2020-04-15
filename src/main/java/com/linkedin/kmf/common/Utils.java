/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Kafka Monitoring utilities.
 */
public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  public static final int ZK_CONNECTION_TIMEOUT_MS = 30_000;
  public static final int ZK_SESSION_TIMEOUT_MS = 30_000;

  /**
   * Read number of partitions for the given topic on the specified ZooKeeper
   * @param adminClient AdminClient object initialized.
   * @param topic topic name.
   * @return the number of partitions of the given topic
   * @throws ExecutionException thrown when describeTopics(topics) get(topic) execution fails.
   * @throws InterruptedException thrown when adminClient's describeTopics getTopic is interrupted.
   */
  private static int getPartitionNumForTopic(AdminClient adminClient, String topic)
      throws ExecutionException, InterruptedException {
    try {
      return adminClient.describeTopics(Collections.singleton(topic)).values().get(topic).get().partitions().size();
    } catch (NoSuchElementException e) {
      return 0;
    } finally {
      LOG.info("Finished getPartitionNumForTopic.");
    }
  }

  /**
   * Create the topic. This method attempts to create a topic so that all
   * the brokers in the cluster will have partitionToBrokerRatio partitions.  If the topic exists, but has different parameters
   * then this does nothing to update the parameters.
   *
   * TODO: Do we care about rack aware mode?  I would think no because we want to spread the topic over all brokers.
   * @param topic topic name
   * @param replicationFactor the replication factor for the topic
   * @param partitionToBrokerRatio This is multiplied by the number brokers to compute the number of partitions in the topic.
   * @param minPartitionNum partition number to be created at least
   * @param topicConfig additional parameters for the topic for example min.insync.replicas
   * @param adminClient AdminClient object initialized.
   * @return the number of partitions created
   * @throws ExecutionException exception thrown then executing the topic creation fails.
   * @throws InterruptedException exception that's thrown when interrupt occurs.
   */
  @SuppressWarnings("unchecked")
  public static int createTopicIfNotExists(String topic, short replicationFactor, double partitionToBrokerRatio,
      int minPartitionNum, Properties topicConfig, AdminClient adminClient)
      throws ExecutionException, InterruptedException {
    try {
      if (adminClient.listTopics().names().get().contains(topic)) {
        return getPartitionNumForTopic(adminClient, topic);
      }
      int brokerCount = Utils.getBrokerCount(adminClient);
      int partitionCount = Math.max((int) Math.ceil(brokerCount * partitionToBrokerRatio), minPartitionNum);
      try {
        NewTopic newTopic = new NewTopic(topic, partitionCount, replicationFactor);
        //noinspection rawtypes
        newTopic.configs((Map) topicConfig);

        List<NewTopic> topics = new ArrayList<>();
        topics.add(newTopic);
        adminClient.createTopics(topics);
      } catch (TopicExistsException e) {
        /* There is a race condition with the consumer. */
        LOG.debug("Monitoring topic " + topic + " already exists in the cluster.", e);
        return getPartitionNumForTopic(adminClient, topic);
      }
      LOG.info("Created monitoring topic {} in cluster with {} partitions and replication factor of {}.", topic,
          partitionCount, replicationFactor);

      return partitionCount;
    } finally {
      LOG.info("Completed the topic creation if it doesn't exist for {}", topic);
    }
  }

  /**
   * @return the number of brokers in this cluster
   */
  private static int getBrokerCount(AdminClient adminClient) throws ExecutionException, InterruptedException {
    return adminClient.describeCluster().nodes().get().size();
  }

  /**
   * @param timestamp time in Ms when this message is generated
   * @param topic     topic this message is sent to
   * @param idx       index is consecutive numbers used by KafkaMonitor to determine duplicate or lost messages
   * @param msgSize   size of the message
   * @return string that encodes the above fields
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
   * @return GenericRecord that is de-serialized from kafka message w.r.t. expected schema.
   */
  public static GenericRecord genericRecordFromJson(String message) {
    GenericRecord record = new GenericData.Record(DefaultTopicSchema.MESSAGE_V0);
    JSONObject jsonObject = new JSONObject(message);
    record.put(DefaultTopicSchema.TOPIC_FIELD.name(), jsonObject.getString(DefaultTopicSchema.TOPIC_FIELD.name()));
    record.put(DefaultTopicSchema.INDEX_FIELD.name(), jsonObject.getLong(DefaultTopicSchema.INDEX_FIELD.name()));
    record.put(DefaultTopicSchema.TIME_FIELD.name(), jsonObject.getLong(DefaultTopicSchema.TIME_FIELD.name()));
    record.put(DefaultTopicSchema.PRODUCER_ID_FIELD.name(),
        jsonObject.getString(DefaultTopicSchema.PRODUCER_ID_FIELD.name()));
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
      for (ObjectName mbeanName : mbeanNames) {
        MBeanInfo mBeanInfo = server.getMBeanInfo(mbeanName);
        MBeanAttributeInfo[] attributeInfos = mBeanInfo.getAttributes();
        for (MBeanAttributeInfo attributeInfo : attributeInfos) {
          if (attributeInfo.getName().equals(attributeExpr) || attributeExpr.length() == 0 || attributeExpr.equals(
              "*")) {
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
