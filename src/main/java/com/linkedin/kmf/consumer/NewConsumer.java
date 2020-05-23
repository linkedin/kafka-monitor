/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kmf.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wraps around the new consumer from Apache Kafka and implement the #KMBaseConsumer interface
 */
public class NewConsumer implements KMBaseConsumer {

  private final KafkaConsumer<String, String> _consumer;
  private Iterator<ConsumerRecord<String, String>> _recordIter;
  private static final Logger LOGGER = LoggerFactory.getLogger(NewConsumer.class);
  private static long lastCommitted;

  public NewConsumer(String topic, Properties consumerProperties, AdminClient adminClient)
      throws ExecutionException, InterruptedException {
    LOGGER.info("{} is being instantiated in the constructor..", this.getClass());
    this.configureGroupId(consumerProperties, adminClient);
    _consumer = new KafkaConsumer<>(consumerProperties);
    _consumer.subscribe(Collections.singletonList(topic));
  }

  /**
   * https://docs.confluent.io/current/clients/producer.html
   * https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java#L69
   * The partitioner will hash the key with murmur2 algorithm
   * and divide it by the number of partitions.
   * The result is that the same key is always assigned to the same partition.
   * @param groupId kafka consumer group ID
   * @param consumerOffsetsTopicPartitions number of partitions in the __consumer_offsets topic.
   * @return hashed integer
   */
  protected int groupIdHelper(String groupId, int consumerOffsetsTopicPartitions) {
    // TODO: use long hash = UUID.nameUUIDFromBytes(s.getBytes()).getMostSignificantBits() ?
    // TODO: MessageDigest digest = MessageDigest.getInstance("SHA-256");
    // TODO: byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
    return Utils.murmur2(groupId.getBytes()) % consumerOffsetsTopicPartitions;
  }

  /**
   * hash(group.id) % (number of __consumer_offsets topic partitions).
   * The partition's leader is the group coordinator
   * Choose B s.t hash(A) % (number of __consumer_offsets topic partitions) == hash(B) % (number of __consumer_offsets topic partitions)
   * @param consumerProperties persistent set of kafka consumer properties
   */
  protected void configureGroupId(Properties consumerProperties, AdminClient adminClient)
      throws ExecutionException, InterruptedException {
    String consumerOffsetsTopic = Topic.GROUP_METADATA_TOPIC_NAME;
    int numConsumerOffsetsTopicPartitions = adminClient.describeTopics(Collections.singleton(consumerOffsetsTopic))
        .values()
        .get(consumerOffsetsTopic)
        .get()
        .partitions()
        .size();

    String consumerGroupPrefix = "__shadow_group";
    int suffix = 0;

    // TODO: populate desired consumer group identifer.
    String groupId = "WANTED_CONSUMER_GROUP_ID";
    while (groupIdHelper(consumerGroupPrefix + suffix, numConsumerOffsetsTopicPartitions) != groupIdHelper(groupId,
        numConsumerOffsetsTopicPartitions)) {
      suffix++;
    }
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupPrefix + suffix);
  }

  @Override
  public BaseConsumerRecord receive() {
    if (_recordIter == null || !_recordIter.hasNext()) {
      _recordIter = _consumer.poll(Duration.ofMillis(Long.MAX_VALUE)).iterator();
    }

    ConsumerRecord<String, String> record = _recordIter.next();
    return new BaseConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), record.value());
  }

  @Override
  public void commitAsync() {
    _consumer.commitAsync();
  }

  @Override
  public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    _consumer.commitAsync(offsets, callback);
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    _consumer.commitAsync(callback);
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition tp) {
    return _consumer.committed(tp);
  }

  @Override
  public void close() {
    _consumer.close();
  }

  @Override
  public long lastCommitted() {
    return lastCommitted;
  }

  @Override
  public void updateLastCommit() {
    lastCommitted = System.currentTimeMillis();
  }
}
