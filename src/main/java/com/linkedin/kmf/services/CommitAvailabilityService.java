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

import com.linkedin.kmf.common.DefaultTopicSchema;
import com.linkedin.kmf.common.Utils;
import com.linkedin.kmf.consumer.BaseConsumerRecord;
import com.linkedin.kmf.consumer.KMBaseConsumer;
import com.linkedin.kmf.consumer.NewConsumer;
import com.linkedin.kmf.services.configs.CommonServiceConfig;
import com.linkedin.kmf.services.configs.ConsumeServiceConfig;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommitAvailabilityService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(CommitAvailabilityService.class);
  private static final int LATENCY_SLA_MS = 1000;
  private static final String TAGS_NAME = "name";
  private static final long TIME_WINDOW_MS = 10000;
  private static final int NUM_SAMPLES = 60;
  private static final long THREAD_SLEEP_MS = 1000;
  private final String _name;
  private final Map<TopicPartition, OffsetAndMetadata> _offsetsToCommit;
  private final KMBaseConsumer _consumer;
  private final Map<Integer, Long> _nextIndexes;
  private final AtomicBoolean _running;
  private final Thread _commitThread;
  private final CommitAvailabilityMetrics _commitAvailabilityMetrics;
  private final String _topic;
  private static final String KMF_CONSUMER = "kmf-consumer";
  private static final String KMF_CONSUMER_GROUP_PREFIX = "kmf-consumer-group-";

  /**
   * CommitAvailabilityService measures the availability of consume offset commits to the Kafka broker.
   * @param props properties file for configuration.
   */
  public CommitAvailabilityService(Map<String, Object> props, String name)
      throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
             InstantiationException {
    _name = name;
    _nextIndexes = new HashMap<>();
    _offsetsToCommit = new HashMap<>();
    Properties consumerProps = new Properties();
    ConsumeServiceConfig config = new ConsumeServiceConfig(props);
    _topic = config.getString(ConsumeServiceConfig.TOPIC_CONFIG);
    String consumerClassName = config.getString(ConsumeServiceConfig.CONSUMER_CLASS_CONFIG);
    _running = new AtomicBoolean(false);
    // Assign default config. This has the lowest priority.
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, java.lang.Boolean.FALSE);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, KMF_CONSUMER);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, KMF_CONSUMER_GROUP_PREFIX + new Random().nextInt());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    String brokerList = config.getString(ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    String zkConnect = config.getString(ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    if (consumerClassName.equals(NewConsumer.class.getCanonicalName()) || consumerClassName.equals(NewConsumer.class.getSimpleName()))
      consumerClassName = NewConsumer.class.getCanonicalName();
    // Assign config specified for ConsumeService.
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    consumerProps.put(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect);
    Map consumerPropsOverride = props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)
        ? (Map) props.get(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG) : new HashMap<>();
    // Assign config specified for consumer. This has the highest priority.
    consumerProps.putAll(consumerPropsOverride);
    if (props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)) props.forEach(consumerProps::putIfAbsent);
    _consumer = (KMBaseConsumer) Class.forName(consumerClassName).getConstructor(String.class, Properties.class).newInstance(_topic, consumerProps);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    MetricConfig metricConfig = new MetricConfig().samples(NUM_SAMPLES).timeWindow(TIME_WINDOW_MS, TimeUnit.MILLISECONDS);
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put(TAGS_NAME, _name);
    _commitAvailabilityMetrics = new CommitAvailabilityMetrics(metrics, tags);
    _commitThread = new Thread(() -> {
      try {
        this.commit();
      } catch (Exception e) {
        LOG.error(_name + "/CommitAvailabilityService commit() failed", e);
      }
    }, _name + " commit-service");
    _commitThread.setDaemon(true);
  }

  private void commit() {
    LOG.info("Commit starting in {}.", this.getClass().getSimpleName());
    try {
      Thread.sleep(THREAD_SLEEP_MS);
    } catch (InterruptedException e) {
      LOG.error("Error occurred while sleeping the thread ", e);
    }

    while (_running.get()) {
      BaseConsumerRecord record;
      try {
        record = _consumer.receive();
      } catch (Exception e) {
        LOG.warn(_name + "/ConsumeService failed to receive record", e);
        // Avoid busy while loop
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          LOG.error("Interrupted Exception occured while trying to sleep the thread.", ex);
        }
        continue;
      }
      if (record == null) continue;
      GenericRecord avroRecord = Utils.genericRecordFromJson(record.value());
      if (avroRecord == null) {
        continue;
      }
      int partition = record.partition();
      long index = (Long) avroRecord.get(DefaultTopicSchema.INDEX_FIELD.name());
      long currMs = System.currentTimeMillis();
      long prevMs = (Long) avroRecord.get(DefaultTopicSchema.TIME_FIELD.name());
      if (currMs - prevMs > LATENCY_SLA_MS)
        if (index == -1L || !_nextIndexes.containsKey(partition)) {
          _nextIndexes.put(partition, -1L);
          continue;
        }
      try {
        _consumer.commitAsync();
        _commitAvailabilityMetrics._offsetsCommitted.record();
      } catch (KafkaException ke) {
        LOG.error("Exception while trying to to async commit.", ke);
        _commitAvailabilityMetrics._failedCommitOffsets.record();
      }
    }
  }

  @Override
  public synchronized void start() {
    if (_running.compareAndSet(false, true)) {
      _commitThread.start();
      LOG.info("{}/ConsumeService started.", _name);
    }
  }

  @Override
  public synchronized void stop() {
    LOG.info("{} stopped.", this.getClass().getSimpleName());
  }

  @Override
  public void awaitShutdown() {
  }

  @Override
  public boolean isRunning() {
    return true;
  }
}

