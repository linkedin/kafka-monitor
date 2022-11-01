/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.linkedin.avroutil1.compatibility.AvroCodecUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import kafka.admin.BrokerMetadata;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Xinfra Monitor utilities.
 */
public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  private static final long LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS = 60000L;
  private static final int LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS = 3;
  private static final String LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_CONFIG = "list.partition.reassignment.timeout.ms";
  private static final int DEFAULT_RETRY_BACKOFF_BASE = 2;

  public static String prettyPrint(Object value) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
    String written = objectWriter.writeValueAsString(value);
    LOG.trace("pretty printed: {}", written);

    return written;
  }

  /**
   * Retrieve the map of {@link PartitionReassignment reassignment} by {@link TopicPartition partitions}.
   *
   * If the response times out, the method retries up to {@link #LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS} times.
   * The max time to wait for the {@link AdminClient adminClient} response is computed.
   *
   * @param adminClient The {@link AdminClient adminClient} to ask for ongoing partition reassignments
   * @return The map of {@link PartitionReassignment reassignment} by {@link TopicPartition partitions}
   */
  public static Map<TopicPartition, PartitionReassignment> ongoingPartitionReassignments(AdminClient adminClient)
      throws InterruptedException, ExecutionException, TimeoutException {
    Map<TopicPartition, PartitionReassignment> partitionReassignments = null;
    int attempts = 0;
    long timeoutMs = LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS;
    do {
      ListPartitionReassignmentsResult responseResult = adminClient.listPartitionReassignments();
      try {
        // A successful response is expected to be non-null.
        partitionReassignments = responseResult.reassignments().get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (TimeoutException timeoutException) {
        LOG.info(
            "Xinfra Monitor has failed to list partition reassignments in {}ms (attempt={}). "
                + "Please consider increasing the value of {} config.",
            timeoutMs, 1 + attempts, LIST_PARTITION_REASSIGNMENTS_TIMEOUT_MS_CONFIG);
        attempts++;
        if (attempts == LIST_PARTITION_REASSIGNMENTS_MAX_ATTEMPTS) {
          throw timeoutException;
        }
        timeoutMs *= DEFAULT_RETRY_BACKOFF_BASE;
      }
    } while (partitionReassignments == null);

    return partitionReassignments;
  }

  public static List<Integer> replicaIdentifiers(Set<BrokerMetadata> brokers) {
    if (brokers == null || brokers.size() == 0) {
      throw new IllegalArgumentException("brokers are either null or empty.");
    }

    List<BrokerMetadata> brokerMetadataList = new ArrayList<>(brokers);

    // Shuffle to get a random order in the replica list
    Collections.shuffle(brokerMetadataList);

    // Get broker ids for replica list
    List<Integer> replicaList = brokerMetadataList.stream().map(m -> m.id()).collect(Collectors.toList());

    return replicaList;
  }

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
        LOG.info("AdminClient indicates that topic {} already exists in the cluster. Topic config: {}", topic, topicConfig);
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
        CreateTopicsResult result = adminClient.createTopics(topics);

        // waits for this topic creation future to complete, and then returns its result.
        result.values().get(topic).get();
        LOG.info("CreateTopicsResult: {}.", result.values());
      } catch (TopicExistsException e) {
        /* There is a race condition with the consumer. */
        LOG.info("Monitoring topic " + topic + " already exists in the cluster.", e);
        return getPartitionNumForTopic(adminClient, topic);
      }
      LOG.info("Created monitoring topic {} in cluster with {} partitions and replication factor of {}.", topic,
          partitionCount, replicationFactor);

      return partitionCount;
    } finally {
      LOG.info("Completed the topic creation if it doesn't exist for {}.", topic);
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
   * @param idx       index is consecutive numbers used by XinfraMonitor to determine duplicate or lost messages
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
    try {
      Decoder jsonDecoder = AvroCompatibilityHelper.newCompatibleJsonDecoder(DefaultTopicSchema.MESSAGE_V0, message);
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(DefaultTopicSchema.MESSAGE_V0, DefaultTopicSchema.MESSAGE_V0);
      return reader.read(null, jsonDecoder);
    } catch (Exception e) {
      throw new IllegalStateException("unable to deserialize " + message, e);
    }
  }

  public static String jsonFromGenericRecord(GenericRecord record) {
    try {
      return AvroCodecUtil.serializeJson(record, AvroVersion.AVRO_1_4);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to serialize avro record due to error: " + record, e);
    }
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

  public static void delay(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      LOG.warn("While trying to sleep for {} millis. Got:", duration.toMillis(), e);
    }
  }
}
