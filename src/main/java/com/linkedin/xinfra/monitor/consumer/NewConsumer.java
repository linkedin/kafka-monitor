/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.consumer;

import com.linkedin.xinfra.monitor.common.ConsumerGroupCoordinatorUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wraps around the new consumer from Apache Kafka and implements the #KMBaseConsumer interface
 */
public class NewConsumer implements KMBaseConsumer {

  private final KafkaConsumer<String, String> _consumer;
  private Iterator<ConsumerRecord<String, String>> _recordIter;
  private static final Logger LOGGER = LoggerFactory.getLogger(NewConsumer.class);
  private static long lastCommitted;

  public NewConsumer(String topic, Properties consumerProperties, AdminClient adminClient)
      throws ExecutionException, InterruptedException {
    LOGGER.info("{} is being instantiated in the constructor..", this.getClass().getSimpleName());

    NewConsumerConfig newConsumerConfig = new NewConsumerConfig(consumerProperties);
    String targetConsumerGroupId = newConsumerConfig.getString(NewConsumerConfig.TARGET_CONSUMER_GROUP_ID_CONFIG);

    if (targetConsumerGroupId != null) {
      consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, configureGroupId(targetConsumerGroupId, adminClient));
    }
    _consumer = new KafkaConsumer<>(consumerProperties);
    _consumer.subscribe(Collections.singletonList(topic));
  }

  static String configureGroupId(String targetConsumerGroupId, AdminClient adminClient)
      throws ExecutionException, InterruptedException {

    return ConsumerGroupCoordinatorUtils.findCollision(targetConsumerGroupId, adminClient);
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
