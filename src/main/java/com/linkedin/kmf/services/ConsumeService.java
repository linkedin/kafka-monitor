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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumeService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeService.class);
  private static final long THREAD_SLEEP_MS = 1000;
  private static final String METRIC_GROUP_NAME = "consume-service";
  private static final String TAGS_NAME = "name";
  private static final String FALSE = "false";
  private static final String[] NON_OVERRIDABLE_PROPERTIES =
    new String[] {ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG};
  private final String _name;
  private ConsumeMetrics _sensors;
  private final KMBaseConsumer _consumer;
  private Thread _consumeThread;
  private final int _latencyPercentileMaxMs;
  private final int _latencyPercentileGranularityMs;
  private final AtomicBoolean _running;
  private final String _topic;
  private final int _latencySlaMs;
  private AdminClient _adminClient;
  private CommitAvailabilityMetrics _commitAvailabilityMetrics;
  private static final int NUM_SAMPLES = 60;
  private static final long TIME_WINDOW_MS = 10000;
  private final Map<Integer, Long> _nextIndexes;
  private final Map<TopicPartition, OffsetAndMetadata> _offsetsToCommit;
  private static final long CONSUME_THREAD_SLEEP_MS = 100;

  public ConsumeService(Map<String, Object> props, String name, CompletableFuture<Void> topicPartitionResult) throws Exception {
    _name = name;
    Map consumerPropsOverride = props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)
      ? (Map) props.get(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG) : new HashMap<>();
    ConsumeServiceConfig config = new ConsumeServiceConfig(props);
    _topic = config.getString(ConsumeServiceConfig.TOPIC_CONFIG);
    String zkConnect = config.getString(ConsumeServiceConfig.ZOOKEEPER_CONNECT_CONFIG);
    String brokerList = config.getString(ConsumeServiceConfig.BOOTSTRAP_SERVERS_CONFIG);
    String consumerClassName = config.getString(ConsumeServiceConfig.CONSUMER_CLASS_CONFIG);
    _latencySlaMs = config.getInt(ConsumeServiceConfig.LATENCY_SLA_MS_CONFIG);
    _latencyPercentileMaxMs = config.getInt(ConsumeServiceConfig.LATENCY_PERCENTILE_MAX_MS_CONFIG);
    _latencyPercentileGranularityMs = config.getInt(ConsumeServiceConfig.LATENCY_PERCENTILE_GRANULARITY_MS_CONFIG);
    _running = new AtomicBoolean(false);
    for (String property: NON_OVERRIDABLE_PROPERTIES) {
      if (consumerPropsOverride.containsKey(property)) {
        throw new ConfigException("Override must not contain " + property + " config.");
      }
    }
    Properties consumerProps = new Properties();

    // Assign default config. This has the lowest priority.
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "kmf-consumer");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kmf-consumer-group-" + new Random().nextInt());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    if (consumerClassName.equals(NewConsumer.class.getCanonicalName()) || consumerClassName.equals(NewConsumer.class.getSimpleName())) {
      consumerClassName = NewConsumer.class.getCanonicalName();
    }

    // Assign config specified for ConsumeService.
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    consumerProps.put(CommonServiceConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect);

    // Assign config specified for consumer. This has the highest priority.
    consumerProps.putAll(consumerPropsOverride);

    if (props.containsKey(ConsumeServiceConfig.CONSUMER_PROPS_CONFIG)) {
      props.forEach(consumerProps::putIfAbsent);
    }
    _consumer = (KMBaseConsumer) Class.forName(consumerClassName).getConstructor(String.class, Properties.class).newInstance(_topic, consumerProps);
    _nextIndexes = new HashMap<>();
    _offsetsToCommit = new HashMap<>();
    topicPartitionResult.thenRun(() -> {
      MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
      List<MetricsReporter> reporters = new ArrayList<>();
      reporters.add(new JmxReporter(JMX_PREFIX));
      Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
      Map<String, String> tags = new HashMap<>();
      tags.put(TAGS_NAME, _name);
      _adminClient = AdminClient.create(props);
      _sensors = new ConsumeMetrics(metrics, tags, _topic, topicPartitionResult, _adminClient, _latencyPercentileMaxMs, _latencyPercentileGranularityMs);
      _commitAvailabilityMetrics = new CommitAvailabilityMetrics(metrics, tags);
      _consumeThread = new Thread(() -> {
        try {
          consume();
        } catch (Exception e) {
          LOG.error(_name + "/ConsumeService failed", e);
        }
      }, _name + " consume-service");
      _consumeThread.setDaemon(true);
    });
  }

  private void consume() throws Exception {
    // Delay 1 second to reduce the chance that consumer creates topic before TopicManagementService
    Thread.sleep(1000);

    Map<Integer, Long> nextIndexes = new HashMap<>();

    while (_running.get()) {
      BaseConsumerRecord record;
      try {
        record = _consumer.receive();
      } catch (Exception e) {
        _sensors._consumeError.record();
        LOG.warn(_name + "/ConsumeService failed to receive record", e);
        // Avoid busy while loop
        Thread.sleep(CONSUME_THREAD_SLEEP_MS);
        continue;
      }

      if (record == null) continue;

      GenericRecord avroRecord = Utils.genericRecordFromJson(record.value());
      if (avroRecord == null) {
        _sensors._consumeError.record();
        continue;
      }
      int partition = record.partition();
      long index = (Long) avroRecord.get(DefaultTopicSchema.INDEX_FIELD.name());
      long currMs = System.currentTimeMillis();
      long prevMs = (Long) avroRecord.get(DefaultTopicSchema.TIME_FIELD.name());
      _sensors._recordsConsumed.record();
      _sensors._bytesConsumed.record(record.value().length());
      _sensors._recordsDelay.record(currMs - prevMs);

      if (currMs - prevMs > _latencySlaMs)
        _sensors._recordsDelayed.record();

      if (index == -1L || !nextIndexes.containsKey(partition)) {
        nextIndexes.put(partition, -1L);
        continue;
      }

      long nextIndex = nextIndexes.get(partition);

      if (nextIndex == -1 || index == nextIndex) {
        nextIndexes.put(partition, index + 1);

        /* Commit availability and commit latency service */
        try {
          TopicPartition topicPartition = new TopicPartition(_topic, partition);
          final AtomicBoolean callbackFired = new AtomicBoolean(false);
          final AtomicReference<Exception> offsetCommitIssues = new AtomicReference<>(null);
          OffsetAndMetadata offsetAndMetadata = null;
          /* Call commitAsync, wait for a NON-NULL return value (see https://issues.apache.org/jira/browse/KAFKA-6183) */
          OffsetCommitCallback commitCallback = (topicPartitionOffsetAndMetadataMap, exception) -> {
            if (exception != null) {
              offsetCommitIssues.set(exception);
            }
            callbackFired.set(true);
          };

          if (_offsetsToCommit != null) {
            _consumer.commitAsync(_offsetsToCommit, commitCallback);
          } else {
            _consumer.commitAsync(commitCallback);
          }
          _commitAvailabilityMetrics._offsetsCommitted.record();
          while (!callbackFired.get()) {
            long timeoutSeconds = 20;
            final Duration timeout = Duration.ofSeconds(timeoutSeconds);
            _consumer.poll(timeout);
          }
          org.testng.Assert.assertNull(offsetCommitIssues.get(), "Offset commit failed.");
          offsetAndMetadata = _consumer.committed(topicPartition);
          if (offsetAndMetadata != null) {
            break;
          }
          long threadSleepMillis = 100;
          Thread.sleep(threadSleepMillis);

        } catch (KafkaException kafkaException) {
          LOG.error("Exception while trying to perform a asynchronous commit.", kafkaException);
          _commitAvailabilityMetrics._failedCommitOffsets.record();
        }

      } else if (index < nextIndex) {
        _sensors._recordsDuplicated.record();
      } else if (index > nextIndex) {
        nextIndexes.put(partition, index + 1);
        long numLostRecords = index - nextIndex;
        _sensors._recordsLost.record(numLostRecords);
        LOG.info("_recordsLost recorded: Avro record current index: {} at {}. Next index: {}. Lost {} records.", index, currMs, nextIndex, numLostRecords);
      }
    }
    // end of consume() while loop
  }

  @Override
  public synchronized void start() {
    if (_running.compareAndSet(false, true)) {
      _consumeThread.start();
      LOG.info("{}/ConsumeService started.", _name);
    }
  }

  @Override
  public synchronized void stop() {
    if (_running.compareAndSet(true, false)) {
      try {
        _consumer.close();
      } catch (Exception e) {
        LOG.warn(_name + "/ConsumeService while trying to close consumer.", e);
      }
      LOG.info("{}/ConsumeService stopped.", _name);
    }
  }

  @Override
  public void awaitShutdown() {
    LOG.info("{}/ConsumeService shutdown completed.", _name);
  }

  @Override
  public String getServiceName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public boolean isRunning() {
    return _running.get() && _consumeThread.isAlive();
  }

}
