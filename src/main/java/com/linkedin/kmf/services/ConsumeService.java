/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeService.class);
  private static final String TAGS_NAME = "name";
  private static final long COMMIT_TIME_INTERVAL = 4;
  private static final long CONSUME_THREAD_SLEEP_MS = 100;
  private static Metrics metrics;
  private final AtomicBoolean _running;
  private final KMBaseConsumer _baseConsumer;
  private final int _latencySlaMs;
  private ConsumeMetrics _sensors;
  private Thread _consumeThread;
  private final AdminClient _adminClient;
  private CommitAvailabilityMetrics _commitAvailabilityMetrics;
  private CommitLatencyMetrics _commitLatencyMetrics;
  private String _topic;
  private String _name;
  private static final String METRIC_GROUP_NAME = "consume-service";
  private static Map<String, String> tags;

  /**
   * Mainly contains services for three metrics:
   * 1 - ConsumeAvailability metrics
   * 2 - CommitOffsetAvailability metrics
   *   2.1 - commitAvailabilityMetrics records offsets committed upon success. that is, no exception upon callback
   *   2.2 - commitAvailabilityMetrics records offsets commit fail upon failure. that is, exception upon callback
   * 3 - CommitOffsetLatency metrics
   *   3.1 - commitLatencyMetrics records the latency between last successful callback and start of last recorded commit.
   *
   * @param name Name of the Monitor instance
   * @param topicPartitionResult The completable future for topic partition
   * @param consumerFactory Consumer Factory object.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public ConsumeService(String name,
                        CompletableFuture<Void> topicPartitionResult,
                        ConsumerFactory consumerFactory)
      throws ExecutionException, InterruptedException {
    _baseConsumer = consumerFactory.baseConsumer();
    _latencySlaMs = consumerFactory.latencySlaMs();
    _name = name;
    _adminClient = consumerFactory.adminClient();
    _running = new AtomicBoolean(false);

    CompletableFuture<Void> topicPartitionFuture = topicPartitionResult.thenRun(() -> {
      MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
      List<MetricsReporter> reporters = new ArrayList<>();
      reporters.add(new JmxReporter(JMX_PREFIX));
      metrics = new Metrics(metricConfig, reporters, new SystemTime());
      tags = new HashMap<>();
      tags.put(TAGS_NAME, name);
      _topic = consumerFactory.topic();
      _sensors = new ConsumeMetrics(metrics, tags, consumerFactory.latencyPercentileMaxMs(), consumerFactory.latencyPercentileGranularityMs());
      _commitLatencyMetrics = new CommitLatencyMetrics(metrics, tags, consumerFactory.latencyPercentileMaxMs(), consumerFactory.latencyPercentileGranularityMs());
      _commitAvailabilityMetrics = new CommitAvailabilityMetrics(metrics, tags);
      _consumeThread = new Thread(() -> {
        try {
          consume();
        } catch (Exception e) {
          LOG.error(name + "/ConsumeService failed", e);
        }
      }, name + " consume-service");
      _consumeThread.setDaemon(true);
    });

    topicPartitionFuture.get();
  }

  private void consume() throws Exception {
    /* Delay 1 second to reduce the chance that consumer creates topic before TopicManagementService */
    Thread.sleep(1000);

    Map<Integer, Long> nextIndexes = new HashMap<>();

    while (_running.get()) {
      BaseConsumerRecord record;
      try {
        record = _baseConsumer.receive();
      } catch (Exception e) {
        _sensors._consumeError.record();
        LOG.warn(_name + "/ConsumeService failed to receive record", e);
        /* Avoid busy while loop */
        Thread.sleep(CONSUME_THREAD_SLEEP_MS);
        continue;
      }

      if (record == null) continue;

      GenericRecord avroRecord = null;
      try {
        avroRecord = Utils.genericRecordFromJson(record.value());
      } catch (Exception exception) {
        LOG.error("exception occurred while getting avro record.", exception);
      }

      if (avroRecord == null) {
        _sensors._consumeError.record();
        continue;
      }
      int partition = record.partition();
      /* Commit availability and commit latency service */
      /* Call commitAsync, wait for a NON-NULL return value (see https://issues.apache.org/jira/browse/KAFKA-6183) */
      OffsetCommitCallback commitCallback = new OffsetCommitCallback() {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap, Exception kafkaException) {
          if (kafkaException != null) {
            LOG.error("Exception while trying to perform an asynchronous commit.", kafkaException);
            _commitAvailabilityMetrics._failedCommitOffsets.record();
          } else {
            _commitAvailabilityMetrics._offsetsCommitted.record();
            _commitLatencyMetrics.recordCommitComplete();
          }
        }
      };

      /* Current timestamp to perform subtraction*/
      long currTimeMillis = System.currentTimeMillis();

      /* 4 seconds consumer offset commit interval. */
      long timeDiffMillis = TimeUnit.SECONDS.toMillis(COMMIT_TIME_INTERVAL);

      if (currTimeMillis - _baseConsumer.lastCommitted() >= timeDiffMillis) {
        /* commit the consumer offset asynchronously with a callback. */
        _baseConsumer.commitAsync(commitCallback);
        _commitLatencyMetrics.recordCommitStart();
        /* Record the current time for the committed consumer offset */
        _baseConsumer.updateLastCommit();
      }
      /* Finished consumer offset commit service. */

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

      } else if (index < nextIndex) {
        _sensors._recordsDuplicated.record();
      } else if (index > nextIndex) {
        nextIndexes.put(partition, index + 1);
        long numLostRecords = index - nextIndex;
        _sensors._recordsLost.record(numLostRecords);
        LOG.info("_recordsLost recorded: Avro record current index: {} at timestamp {}. Next index: {}. Lost {} records.", index, currMs, nextIndex, numLostRecords);
      }
    }
    /* end of consume() while loop */
  }

  Metrics metrics() {
    return metrics;
  }

  void startConsumeThreadForTesting() {
    if (_running.compareAndSet(false, true)) {
      _consumeThread.start();
      LOG.info("{}/ConsumeService started.", _name);
    }
  }

  @Override
  public synchronized void start() {
    if (_running.compareAndSet(false, true)) {
      _consumeThread.start();
      LOG.info("{}/ConsumeService started.", _name);

      Sensor topicPartitionCount = metrics.sensor("topic-partitions");
      DescribeTopicsResult describeTopicsResult = _adminClient.describeTopics(Collections.singleton(_topic));
      Map<String, KafkaFuture<TopicDescription>> topicResultValues = describeTopicsResult.values();
      KafkaFuture<TopicDescription> topicDescriptionKafkaFuture = topicResultValues.get(_topic);
      TopicDescription topicDescription = null;
      try {
        topicDescription = topicDescriptionKafkaFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Exception occurred while getting the topicDescriptionKafkaFuture", e);
      }
      double partitionCount = topicDescription.partitions().size();
      topicPartitionCount.add(
          new MetricName("topic-partitions-count", METRIC_GROUP_NAME, "The total number of partitions for the topic.", tags),
          new CumulativeSum(partitionCount));
    }
  }

  @Override
  public synchronized void stop() {
    if (_running.compareAndSet(true, false)) {
      try {
        _baseConsumer.close();
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
  public boolean isRunning() {
    return _running.get() && _consumeThread.isAlive();
  }

}
