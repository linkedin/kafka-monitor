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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
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
import org.testng.Assert;

import static org.testng.Assert.assertNotNull;

/**
 * CommitAvailabilityService measures the availability of consume offset commits to the Kafka broker.
 */
public class CommitAvailabilityService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(CommitAvailabilityService.class);
  private static final String TAGS_NAME = "name";
  private static final long TIME_WINDOW_MS = 10000;
  private static final int NUM_SAMPLES = 60;
  private static final long THREAD_SLEEP_MS = 1000;
  private static final String KMF_CONSUMER = "kmf-consumer";
  private static final String KMF_CONSUMER_GROUP_PREFIX = "kmf-consumer-group-";
  private final String _name;
  private final Map<TopicPartition, OffsetAndMetadata> _offsetsToCommit;
  private final Map<Integer, Long> _nextIndexes;
  private final AtomicBoolean _running;
  private final Thread _commitThread;
  private final CommitAvailabilityMetrics _commitAvailabilityMetrics;
  private final String _topic;
  private KMBaseConsumer _kmBaseConsumer;

  /*
   * Commit offsets in a loop and set max.poll to 1 (or very low) and call commitAsync() after every record, measuring how long it takes.
   * in a way that we have a consumer for every broker (that leads a partition of __ConsumerOffsets) in the cluster.
   * (Kafka consumer code which finds the group-coordinator broker for a consumer group)
   * Then, figure out how to name the consumer groups so they land on specific brokers.
   */
  public CommitAvailabilityService(Map<String, Object> props, String name) throws InvocationTargetException, InstantiationException {
    _name = name;
    _nextIndexes = new HashMap<>();
    _offsetsToCommit = new HashMap<>();
    ConsumeServiceConfig config = new ConsumeServiceConfig(props);
    _topic = config.getString(ConsumeServiceConfig.TOPIC_CONFIG);
    _running = new AtomicBoolean(false);
    Properties consumerProps = new Properties();
    String consumerClassName = config.getString(ConsumeServiceConfig.CONSUMER_CLASS_CONFIG);
    String brokerList = config.getString(ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    String zkConnect = config.getString(ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    setupConsumerProps(consumerProps, brokerList, zkConnect, consumerClassName, props);
    try {
      _kmBaseConsumer = (KMBaseConsumer) Class.forName(consumerClassName).getConstructor(String.class, Properties.class).newInstance(_topic, consumerProps);
    } catch (IllegalAccessException | ClassNotFoundException | NoSuchMethodException exception) {
      LOG.error("Exception occurred while constructing CommitAvailabilityService.", exception);
    }
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    MetricConfig metricConfig = new MetricConfig().samples(NUM_SAMPLES).timeWindow(TIME_WINDOW_MS, TimeUnit.MILLISECONDS);
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put(TAGS_NAME, _name);
    _commitAvailabilityMetrics = new CommitAvailabilityMetrics(metrics, tags);
    _commitThread = new Thread(() -> {
      try {
        this.initiateCommit();
      } catch (Exception e) {
        LOG.error(_name + "/CommitAvailabilityService commit() failed", e);
      }
    }, _name + " commit-service");
    _commitThread.setDaemon(true);
  }

  private void setupConsumerProps(Properties consumerProps, String brokerList, String zkConnect, String consumerClassName, Map<String, Object> props) {
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, java.lang.Boolean.FALSE);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, KMF_CONSUMER);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, KMF_CONSUMER_GROUP_PREFIX + new Random().nextInt());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    consumerProps.put(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect);
    if (consumerClassName.equals(NewConsumer.class.getSimpleName())) {
      consumerClassName = NewConsumer.class.getCanonicalName();
    }
    Map consumerPropsOverride;
    if (props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)) {
      consumerPropsOverride = (Map) props.get(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG);
    } else {
      consumerPropsOverride = new HashMap<>();
    }
    consumerProps.putAll(consumerPropsOverride);
    if (props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)) {
      props.forEach(consumerProps::putIfAbsent);
    }
  }

  private void initiateCommit() throws Exception {
    LOG.info("Commit starting in {}.", getClass().getSimpleName());
    try {
      Thread.sleep(THREAD_SLEEP_MS);
    } catch (InterruptedException exception) {
      LOG.error("Error occurred while sleeping the thread ", exception);
    }

    while (_running.get()) {
      BaseConsumerRecord baseConsumerRecord;
      try {
        baseConsumerRecord = _kmBaseConsumer.receive();
      } catch (Exception exception) {
        LOG.warn(_name + "/CommitAvailabilityService failed to receive record.", exception);
        /* Avoid busy while loop */
        try {
          long threadSleepMs = 100;
          Thread.sleep(threadSleepMs);
        } catch (InterruptedException ex) {
          LOG.error("Interrupted Exception occurred while trying to sleep the thread.", ex);
        }
        continue;
      }
      if (baseConsumerRecord == null) continue;
      GenericRecord genericAvroRecord = Utils.genericRecordFromJson(baseConsumerRecord.value());
      int partition = baseConsumerRecord.partition();
      long index = (Long) genericAvroRecord.get(DefaultTopicSchema.INDEX_FIELD.name());
      if (index == -1L || !_nextIndexes.containsKey(partition)) {
        _nextIndexes.put(partition, -1L);
        continue;
      }

      try {
        TopicPartition topicPartition = new TopicPartition(_topic, partition);
        commitAndRetrieveOffsets(topicPartition, _offsetsToCommit);

      } catch (KafkaException kafkaException) {
        LOG.error("Exception while trying to to async commit.", kafkaException);
        _commitAvailabilityMetrics._failedCommitOffsets.record();
      }
    }
  }

  private OffsetAndMetadata commitAndRetrieveOffsets(TopicPartition topicPartition, Map<TopicPartition, OffsetAndMetadata> offsetMap) throws Exception {
    final AtomicBoolean callbackFired = new AtomicBoolean(false);
    final AtomicReference<Exception> offsetCommitIssue = new AtomicReference<>(null);
    OffsetAndMetadata committed;
    while (true) {
      /* Call commitAsync, wait for a NON-NULL return value (see https://issues.apache.org/jira/browse/KAFKA-6183) */
      OffsetCommitCallback commitCallback = (topicPartitionOffsetAndMetadataMap, exception) -> {
        if (exception != null) {
          offsetCommitIssue.set(exception);
        }
        callbackFired.set(true);
      };
      if (offsetMap != null) {
        _kmBaseConsumer.commitAsync(offsetMap, commitCallback);
      } else {
        _kmBaseConsumer.commitAsync(commitCallback);
      }
      _commitAvailabilityMetrics._offsetsCommitted.record();
      while (!callbackFired.get()) {
        long timeoutSeconds = 20;
        final Duration timeout = Duration.ofSeconds(timeoutSeconds);
        _kmBaseConsumer.poll(timeout);
      }
      Assert.assertNull(offsetCommitIssue.get(), "Offset commit failed");
      committed = _kmBaseConsumer.committed(topicPartition);
      if (committed != null) {
        break;
      }
      long threadSleepMillis = 100;
      Thread.sleep(threadSleepMillis);
    }
    assertNotNull(committed, "Unable to retrieve committed offsets within timeout");
    return committed;
  }

  @Override
  public synchronized void start() {
    if (_running.compareAndSet(false, true)) {
      _commitThread.start();
      LOG.info("{}/CommitAvailabilityService started.", _name);
    }
  }

  @Override
  public synchronized void stop() {
    LOG.info("{} stopped.", this.getClass().getSimpleName());
  }

  @Override
  public void awaitShutdown() {
    LOG.info("{} awaiting shut down.", this.getClass().getSimpleName());
  }

  @Override
  public String getServiceName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public boolean isRunning() {
    return _running.get() && _commitThread.isAlive();
  }
}

